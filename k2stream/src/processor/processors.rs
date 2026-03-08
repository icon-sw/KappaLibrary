use std::any::Any;
use std::{collections::HashMap, sync::MutexGuard};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::processor::connections::{Input, Output};
use crate::errors::{K2Error, K2ErrorCode};
use crate::k2err;
use crate::processor::memory::{DataHeader, K2Data, Memory, MemoryTrait};
use crate::processor::parameters::{Parameter, ParameterType};
use crate::processor::states::State;
use crate::streamer::stream_controller::{Callback, StreamController};

pub type ProcessorNewReturn = Result<Box<dyn ProcessorTrait>, K2Error>;

#[derive(Debug,PartialEq, Clone)]
pub enum StreamType {
    NONE,
    RECEIVER,
    SENDER,
    BOTH,
}

pub struct StreamBlock {
    stream_id: isize,
    task_id: Vec<isize>,
    memory: Memory,
    initialized: bool,
    inputs: HashMap<String, Box<dyn MemoryTrait>>,
    outputs: HashMap<String, Box<dyn MemoryTrait>>,
}

impl Default for StreamBlock {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamBlock
{
    pub fn new() -> Self {
        Self {
            stream_id: -1_isize,
            task_id: Vec::new(),
            inputs: HashMap::new(),
            outputs: HashMap::new(),
            memory: Memory::new(),
            initialized: false,
        }
    }
    pub fn get_stream_id(&self) -> isize {
        self.stream_id
    }
    pub fn get_task_id(&self) -> &Vec<isize> {
        &self.task_id
    }
    pub fn set_stream_id(&mut self, stream_id: isize) {
        self.stream_id = stream_id;
    }
    pub fn set_task_id(&mut self, task_id: isize) {
        self.task_id.push(task_id);
    }
    pub fn add_input<T>(&mut self, name: String) -> Result<(), K2Error>
    where Input<T> : MemoryTrait,
          T: 'static + Send + Sync {
        if !self.inputs.contains_key(&name) {
            self.inputs.insert(name.clone(), Box::new(Input::<T>::new(name)));
            Ok(())
        } else {
            Err(k2err!(K2ErrorCode::AlreadyExists, "Input already exists"))
        }
    }
    pub fn add_output<T>(&mut self, name: String) -> Result<(), K2Error>
    where Output<T> : MemoryTrait,
          T: 'static + Send + Sync + Clone {
        if !self.outputs.contains_key(&name) {
            self.outputs.insert(name.clone(), Box::new(Output::<T>::new(name)));
            Ok(())
        } else {
            Err(k2err!(K2ErrorCode::AlreadyExists, "Output already exists"))
        }
    }
    pub fn add_parameter<T>(&mut self, name: String, parameter: ParameterType) -> Result<(), K2Error>
    where   T: 'static + Clone + Sync + Send + Default + PartialOrd,
    {
        self.memory.insert(
            name.clone(),
            Box::new(
                Parameter::<T>::new(
                    name, T::default(),
                    parameter,
        )?))
    }
    pub fn add_state<T>(&mut self, name: String) -> Result<(), K2Error>
    where T: 'static + Clone + Sync + Send + Default,
    {
        self.memory.insert(
            name.clone(), 
            Box::new(
                State::<T>::new(
                    name,
        )?))
    }
    pub fn add_command(&mut self, command: String, callback: Callback) -> Result<(), K2Error> {
        let name = command.clone().split(".").next().ok_or(k2err!(K2ErrorCode::InvalidValue, "Invalid command format"))?.to_string();
        let stream_cntr = StreamController::get_stream_by_id(self.stream_id)?;
        let mut stream_cntr = stream_cntr.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "".to_string()))?;
        let stream_cntr = stream_cntr.as_any_mut().downcast_mut::<StreamController>().ok_or(k2err!(K2ErrorCode::LockError, ""))?;
        StreamController::add_command(stream_cntr, command, name, callback)
     }
    pub fn is_initialized(&mut self) -> bool {
        if !self.initialized {
            self.initialized = self.memory.is_initialized();
        }
        self.initialized
    }
    pub fn initialize(&mut self) -> Result<(), K2Error> {
        if self.stream_id == -1 {
            return Err(k2err!( K2ErrorCode::Uninitialized, "Stream ID is not set"));
        }
        if self.task_id.is_empty() {
            return Err(k2err!( K2ErrorCode::Uninitialized, "Task ID is not set"));
        }
        for data in self.memory.values_mut() {
            if !data.is_setted() {
                data.initialize();
            }
        }
        self.initialized = true;
        Ok(())
    }
    pub fn get_parameter<T>(&self, name: &String) -> Result<&Parameter<T>, K2Error>
    where T: 'static + Send + Sync
    {
        self.memory.get(name).ok_or(k2err!( K2ErrorCode::NotFound, "Parameter not found"))?.as_any().downcast_ref::<Parameter<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast parameter"))
    }
    pub fn get_state<T>(&self, name: &String) -> Result<&State<T>, K2Error>
    where T: 'static + Send + Sync{
        self.memory.get(name).ok_or(k2err!( K2ErrorCode::NotFound, "State not found"))?.as_any().downcast_ref::<State<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast state"))
    }
    pub fn get_input<T>(&self, name: &String) -> Result<&Input<T>, K2Error>
    where T: 'static + Send + Sync 
    {
        self.inputs.get(name).ok_or(k2err!( K2ErrorCode::NotFound, "Input not found"))?.as_any().downcast_ref::<Input<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast input"))
    }
    pub fn get_output<T: 'static + Send + Sync + Clone>(&self, name: &String) -> Result<&Output<T>, K2Error> 
    where T: 'static + Send + Sync + Clone
    {
        self.outputs.get(name).ok_or(k2err!( K2ErrorCode::NotFound, "Output not found"))?.as_any().downcast_ref::<Output<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast output"))
    }
    pub fn get_parameter_mut<T>(&mut self, name: &String) -> Result<&mut Parameter<T>, K2Error> 
    where T: 'static + Send + Sync
    {
        self.memory.get_mut(name).ok_or(k2err!( K2ErrorCode::NotFound, "Parameter not found"))?.as_any_mut().downcast_mut::<Parameter<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast parameter"))
    }
    pub fn get_state_mut<T>(&mut self, name: &String) -> Result<&mut State<T>, K2Error> 
    where T: 'static + Send + Sync
    {
        self.memory.get_mut(name).ok_or(k2err!( K2ErrorCode::NotFound, "State not found"))?.as_any_mut().downcast_mut::<State<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast state"))
    }
    pub fn get_input_mut<T>(&mut self, name: &String) -> Result<&mut Input<T>, K2Error>
    where T: 'static + Send + Sync
    {
        self.inputs.get_mut(name).ok_or(k2err!( K2ErrorCode::NotFound, "Input not found"))?.as_any_mut().downcast_mut::<Input<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast input"))
    }
    pub fn get_output_mut<T>(&mut self, name: &String) -> Result<&mut Output<T>, K2Error> 
    where T: 'static + Send + Sync + Clone
    {
        self.outputs.get_mut(name).ok_or(k2err!( K2ErrorCode::NotFound, "Output not found"))?.as_any_mut().downcast_mut::<Output<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast output"))
    }
    pub fn set_param_value<T>(&mut self, name: &String, value: T) -> Result<(), K2Error> 
    where T: 'static + Clone + Sync + Send + PartialOrd + Default {
        self.get_parameter_mut(name)?.set(value)
    }
    pub fn set_state_value<T>(&mut self, name: &String, value: T) -> Result<(), K2Error> 
    where T: 'static + Clone + Sync + Send + Default {
        self.get_state_mut(name)?.set(value)
    }
    pub fn get_param_value<T>(&self, name: &String) -> Result<&T, K2Error> 
    where T: 'static + Clone + Sync + Send + PartialOrd + Default {
        Ok(self.get_parameter(name)?.get())
    }
    pub fn get_state_value<T>(&self, name: &String) -> Result<&T, K2Error> 
    where T: 'static + Clone + Sync + Send + PartialOrd + Default {
        Ok(self.get_state(name)?.get())
    }
    pub fn connect<T>(&mut self, output_name: &String, input_name: &String, other_block: &StreamBlock) -> Result<(), K2Error> 
    where T: 'static + Send + Sync + Clone
    {
        let output = self.get_output_mut::<T>(output_name)?;
        let output = output.as_any_mut().downcast_mut::<Output<T>>().ok_or(k2err!(K2ErrorCode::BadFormat, "Output type mismatch"))?;
        let input = other_block.get_input::<T>(input_name)?;
        let input = input.as_any().downcast_ref::<Input<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Input type mismatch"))?;
        output.connect(input.get_sender());
        Ok(())
    }
    pub fn receive_input<T>(&self, name: &String) -> Result<T, K2Error> 
    where T: 'static + Send + Sync
    {
        match self.get_input(name)?.receive() {
            Ok(data) => {
                Ok(data.data)
            }
            Err(err) => Err(err)
        }
    }
    pub fn send_output<T>(&self, name: &String, data: T) -> Result<(), K2Error> 
        where T: 'static + Send + Sync + Clone 
    {
        let data = K2Data {
            id: rand::rng().next_u64(),
            data, 
        };
        self.get_output(name)?.send(data)
    }
    pub fn get_processor_type(&self) -> StreamType {
        match (self.inputs.len(), self.outputs.len()) {
            (0, 0) => StreamType::NONE,
            (0, _) => StreamType::RECEIVER,
            (_, 0) => StreamType::SENDER,
            _ => StreamType::BOTH,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamState {
    Uninitialized,
    Initialized,
    Running,
    Waiting,
}
pub trait ProcessorBlockTrait: MemoryTrait + Send + Sync {
    fn name(&self) -> &DataHeader;
    fn proc_name(&self) -> &String;
    fn header(&self) -> &ProcessorHeader;
    fn lock() -> Result<MutexGuard<'static, Self>, K2Error> where Self: Sized;
    fn get_proc_state(&self) -> Result<StreamState, K2Error>;
    fn get_stream_block(&self) -> &StreamBlock;
    fn get_stream_block_mut(&mut self) -> &mut StreamBlock;
}
pub trait ProcessorTrait: ProcessorBlockTrait + Send + Sync + Any {
    fn new(name: String) -> ProcessorNewReturn where Self: Sized;
    fn initialize(&mut self ) -> Result<(),K2Error>;
    fn process(&mut self) -> Result<(), K2Error>;
    fn finalize(&mut self) -> Result<(), K2Error>;
    fn get_processor_type(&self) -> StreamType {
        self.get_stream_block().get_processor_type()
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub struct ProcessorHeader {
    pub proc_name: String,
    pub description: String,
    pub license: String,
    pub version: String,
    pub author: String,
    pub email: String,
    pub repository: String,
}

#[cfg(test)]
mod test {
    use crate::processor::memory::DataTrait;

    use super::*;

    #[test]
    fn stream_block_test() {
        let mut stream_block = StreamBlock::new();
        stream_block.set_stream_id(1);
        assert_eq!(stream_block.get_stream_id(), 1);
        stream_block.set_task_id(1);
        stream_block.set_task_id(2);
        let task_id = stream_block.get_task_id();
        assert_eq!(task_id[0], 1);
        assert_eq!(task_id[1], 2);
    }
    #[test]
    fn stream_block_input_test() {
        let mut stream_block = StreamBlock::new();
        assert!(stream_block.add_input::<i32>("test".to_string()).is_ok());
        assert!(stream_block.add_input::<f64>("test".to_string()).is_err());
        assert!(stream_block.get_input::<i32>(&"test".to_string()).is_ok());
    }
    #[test]
    fn stream_block_output_test() {
        let mut stream_block = StreamBlock::new();
        assert!(stream_block.add_output::<i32>("test".to_string()).is_ok());
        assert!(stream_block.add_output::<f64>("test".to_string()).is_err());
        assert!(stream_block.get_output::<i32>(&"test".to_string()).is_ok());
    }
    #[test]
    fn stream_block_parameter_test() {
        let mut stream_block = StreamBlock::new();
        assert!(stream_block.add_parameter::<i64>(
            "test".to_string(),
            ParameterType::DYNAMIC).is_ok());
        assert!(stream_block.add_parameter::<f64>(
            "test".to_string(),
            ParameterType::DYNAMIC).is_err());
        assert!(!stream_block.is_initialized());
        assert!(stream_block.initialize().is_err());
        stream_block.set_stream_id(1);
        assert!(stream_block.initialize().is_err());
        stream_block.set_task_id(1);
        assert!(stream_block.initialize().is_ok());
        assert!(stream_block.is_initialized());
        let param = stream_block.get_parameter_mut::<i64>(&"test".to_string());
        assert!(param.is_ok());
        
        assert!(param.unwrap().set(10).is_ok());
        let param = stream_block.get_parameter::<i64>(&"test".to_string());
        assert_eq!(param.unwrap().get(), &10);
        assert!(stream_block.set_param_value::<i64>(&"test".to_string(), 13).is_ok());
        assert_eq!(stream_block.get_param_value::<i64>(&"test".to_string()).unwrap(), &13);
    }
    #[test]
    fn stream_block_state_test() {
        let mut stream_block = StreamBlock::new();
        assert!(stream_block.add_state::<i64>(
            "test".to_string()).is_ok());
        assert!(stream_block.add_state::<f64>(
            "test".to_string()).is_err());
        let stat = stream_block.get_state_mut::<i64>(&"test".to_string());
        assert!(stat.is_ok());
        assert!(stat.unwrap().set_init(10).is_ok());
        let stat = stream_block.get_state_mut::<i64>(&"test".to_string());
        stat.unwrap().initialize();
        let stat = stream_block.get_state::<i64>(&"test".to_string());
        assert_eq!(stat.unwrap().get(), &10);
        assert!(stream_block.set_state_value::<i64>(&"test".to_string(), 13).is_ok());
        assert_eq!(stream_block.get_state_value::<i64>(&"test".to_string()).unwrap(), &13);
    }
    #[test]
    fn stream_block_interoperability() {
        let mut stream_block_1 = StreamBlock::new();
        let mut stream_block_2 = StreamBlock::new();
        assert_eq!(stream_block_1.get_processor_type(), StreamType::NONE);
        assert!(stream_block_1.add_output::<String>("test_output".to_string()).is_ok());
        assert_eq!(stream_block_1.get_processor_type(), StreamType::RECEIVER);
        assert!(stream_block_2.add_input::<String>("test_input".to_string()).is_ok());
        assert_eq!(stream_block_2.get_processor_type(), StreamType::SENDER);
        assert!(stream_block_2.add_output::<String>("test_output_2".to_string()).is_ok());
        assert_eq!(stream_block_2.get_processor_type(), StreamType::BOTH);
        assert!(stream_block_1.connect::<String>(
            &"test_output".to_string(), 
            &"test_input".to_string(), 
            &stream_block_2).is_ok());
        assert!(stream_block_1.send_output(&"test_output".to_string(), "Hello".to_string()).is_ok());
        let res = stream_block_2.receive_input::<String>(&"test_input".to_string());
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), "Hello".to_string());
    }
}