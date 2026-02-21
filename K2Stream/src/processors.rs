use std::any::Any;
use std::{collections::HashMap, sync::MutexGuard};
use num_traits::{Float, PrimInt};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::connections::{Input, Output};
use crate::errors::{K2Error, K2ErrorCode};
use crate::k2err;
use crate::memory::{DataHeader, DataTrait, Memory, MemoryTrait};
use crate::parameters::{Parameter, ParameterType, ParameterValueType};
use crate::states::State;
use crate::stream_controller::{Callback, StreamController};

pub type ProcessorNewReturn = Result<Box<dyn ProcessorTrait>, K2Error>;

#[derive(PartialEq, Clone)]
pub enum StreamType {
    NONE,
    RECEIVER,
    SENDER,
    BOTH,
}

pub struct StreamBlock {
    id: usize,
    stream_id: isize,
    task_id: Vec<isize>,
    memory: Memory,
    initialized: bool,
    inputs: HashMap<String, Box<dyn MemoryTrait>>,
    outputs: HashMap<String, Box<dyn MemoryTrait>>,
}

impl StreamBlock
{
    pub fn new() -> Self {
        let id = rand::rng().random::<u64>() as usize;
        Self {
            id,
            stream_id: -1 as isize,
            task_id: Vec::new(),
            inputs: HashMap::new(),
            outputs: HashMap::new(),
            memory: Memory::new(),
            initialized: false,
        }
    }
    pub fn get_id(&self) -> usize {
        self.id
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
    pub fn add_input<T: 'static>(&mut self, name: String) -> Result<(), K2Error>
    where Input<T> : MemoryTrait,
          T: 'static + Send + Sync {
        if !self.inputs.contains_key(&name) {
            self.inputs.insert(name.clone(), Box::new(Input::<T>::new(name)));
            Ok(())
        } else {
            Err(k2err!(K2ErrorCode::AlreadyExists, "Input already exists"))
        }
    }
    pub fn add_output<T: 'static + Clone>(&mut self, name: String) -> Result<(), K2Error>
    where Output<T> : MemoryTrait,
          T: 'static + Send + Sync + Clone {
        if !self.outputs.contains_key(&name) {
            self.outputs.insert(name.clone(), Box::new(Output::<T>::new(name)));
            Ok(())
        } else {
            Err(k2err!(K2ErrorCode::AlreadyExists, "Output already exists"))
        }
    }
    pub fn add_parameter<T: 'static>(&mut self, kind: ParameterValueType, name: String, parameter: ParameterType) -> Result<(), K2Error>
    where   T: 'static + Clone + Sync + Send + Float + PrimInt + Default,
            Result<Parameter<T>, K2Error>: DataTrait 
    {
        match kind {
            ParameterValueType::INTEGER => {
                self.memory.insert(
                    name.clone(), 
                    Box::new(
                        Parameter::<T>::int(
                            name, 
                            T::default(), 
                            parameter, 
                )))
            }
            ParameterValueType::FLOAT => {
                self.memory.insert(
                    name.clone(), 
                    Box::new(
                        Parameter::<T>::float(
                            name, T::default(), 
                            parameter, 
                )))
            }
        }
    }
    pub fn add_state<T>(&mut self, name: String) -> Result<(), K2Error>
    where T: 'static + Clone + Sync + Send + Default,
    Result<State<T>, K2Error>: DataTrait 
    {
        self.memory.insert(
            name.clone(), 
            Box::new(
                State::<T>::new(
                    name,
        )))
    }
    pub fn add_command(&mut self, command: String, callback: Callback) -> Result<(), K2Error> {
        let name = command.clone().split(".").next().ok_or(k2err!(K2ErrorCode::InvalidValue, "Invalid command format"))?.to_string();
        StreamController::add_command(self.stream_id, command, name, callback)
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
    pub fn get_parameter<T: 'static + Send + Sync>(&self, name: &String) -> Result<&Parameter<T>, K2Error> {
        self.memory.get(name).ok_or(k2err!( K2ErrorCode::NotFound, "Parameter not found"))?.as_any().downcast_ref::<Parameter<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast parameter"))
    }
    pub fn get_state<T: 'static + Send + Sync>(&self, name: &String) -> Result<&State<T>, K2Error> {
        self.memory.get(name).ok_or(k2err!( K2ErrorCode::NotFound, "State not found"))?.as_any().downcast_ref::<State<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast state"))
    }
    pub fn get_input<T: 'static + Send + Sync>(&self, name: &String) -> Result<&Input<T>, K2Error> {
        self.inputs.get(name).ok_or(k2err!( K2ErrorCode::NotFound, "Input not found"))?.as_any().downcast_ref::<Input<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast input"))
    }
    pub fn get_output<T: 'static + Send + Sync + Clone>(&self, name: &String) -> Result<&Output<T>, K2Error> {
        self.outputs.get(name).ok_or(k2err!( K2ErrorCode::NotFound, "Output not found"))?.as_any().downcast_ref::<Output<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast output"))
    }
    pub fn get_parameter_mut<T: 'static + Send + Sync>(&mut self, name: &String) -> Result<&mut Parameter<T>, K2Error> {
        self.memory.get_mut(name).ok_or(k2err!( K2ErrorCode::NotFound, "Parameter not found"))?.as_any_mut().downcast_mut::<Parameter<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast parameter"))
    }
    pub fn get_state_mut<T: 'static + Send + Sync>(&mut self, name: &String) -> Result<&mut State<T>, K2Error> {
        self.memory.get_mut(name).ok_or(k2err!( K2ErrorCode::NotFound, "State not found"))?.as_any_mut().downcast_mut::<State<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast state"))
    }
    pub fn get_input_mut<T: 'static + Send + Sync>(&mut self, name: &String) -> Result<&mut Input<T>, K2Error> {
        self.inputs.get_mut(name).ok_or(k2err!( K2ErrorCode::NotFound, "Input not found"))?.as_any_mut().downcast_mut::<Input<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast input"))
    }
    pub fn get_output_mut<T: 'static + Send + Sync + Clone>(&mut self, name: &String) -> Result<&mut Output<T>, K2Error> {
        self.outputs.get_mut(name).ok_or(k2err!( K2ErrorCode::NotFound, "Output not found"))?.as_any_mut().downcast_mut::<Output<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast output"))
    }
    pub fn set_param<T: 'static>(&mut self, name: &String, value: T) -> Result<(), K2Error> 
    where T: 'static + Clone + Sync + Send + Float + PrimInt + Default {
        let param = self.memory.get_mut(name).ok_or(k2err!( K2ErrorCode::NotFound, "Parameter not found"))?.as_any_mut().downcast_mut::<Parameter<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast parameter"))?;
        param.set(value)
    }
    pub fn set_state<T: 'static>(&mut self, name: &String, value: T) -> Result<(), K2Error> 
    where T: 'static + Clone + Sync + Send + Default {
        let state = self.memory.get_mut(name).ok_or(k2err!( K2ErrorCode::NotFound, "State not found"))?.as_any_mut().downcast_mut::<State<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Failed to downcast state"))?;
        state.set(value)
    }
    pub fn connect<T: 'static + Send + Sync + Clone>(&mut self, output_name: &String, input_name: &String, other_block: &StreamBlock) -> Result<(), K2Error> {
        let output = self.outputs.get_mut(output_name).ok_or(k2err!( K2ErrorCode::NotFound, "Output not found"))?;
        let output = output.as_any_mut().downcast_mut::<Output<T>>().ok_or(k2err!(K2ErrorCode::BadFormat, "Output type mismatch"))?;
        let input = other_block.inputs.get(input_name).ok_or(k2err!( K2ErrorCode::NotFound, "Input not found"))?;
        let input = input.as_any().downcast_ref::<Input<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Input type mismatch"))?;
        output.connect(input.get_sender());
        Ok(())
    }
    pub fn receive_input<T: 'static + Send + Sync>(&self, name: &String) -> Result<T, K2Error> {
        let input = self.inputs.get(name).ok_or(k2err!( K2ErrorCode::NotFound, "Input not found"))?.as_any().downcast_ref::<Input<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Input type mismatch"))?;
        input.receive()
    }
    pub fn send_output<T: 'static + Send + Sync + Clone>(&self, name: &String, data: T) -> Result<(), K2Error> {
        let output = self.outputs.get(name).ok_or(k2err!( K2ErrorCode::NotFound, "Output not found"))?.as_any().downcast_ref::<Output<T>>().ok_or(k2err!( K2ErrorCode::BadFormat, "Output type mismatch"))?;
        output.send(data)
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