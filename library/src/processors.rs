use std::{collections::HashMap, sync::MutexGuard};
use num_traits::{Float, PrimInt};
use rand::Rng;

use crate::connections::{Input, Output};
use crate::memory::{DataHeader, DataTrait, Memory, MemoryTrait};
use crate::parameter::{Parameter, ParameterType, ParameterValueType};
use crate::states::State;
use crate::stream_controller::{Callback, StreamController};
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
    pub fn add_input<T: 'static>(&mut self, name: String) -> Result<(), ()>
    where Input<T> : MemoryTrait{
        if !self.inputs.contains_key(&name) {
            self.inputs.insert(name.clone(), Box::new(Input::<T>::new(name)));
            Ok(())
        } else {
            Err(())
        }
    }
    pub fn add_output<T: 'static + Clone>(&mut self, name: String) -> Result<(), ()>
    where Output<T> : MemoryTrait {
        if !self.outputs.contains_key(&name) {
            self.outputs.insert(name.clone(), Box::new(Output::<T>::new(name)));
            Ok(())
        } else {
            Err(())
        }
    }
    pub fn add_parameter<T: 'static>(&mut self, kind: ParameterValueType, name: String, parameter: ParameterType) -> Result<(), ()>
    where   T: 'static + Clone + Sync + Send + Float + PrimInt + Default,
            Result<Parameter<T>, ()>: DataTrait 
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
    pub fn add_state<T>(&mut self, name: String) -> Result<(), ()>
    where T: 'static + Clone + Sync + Send + Default,
    Result<State<T>, ()>: DataTrait 
    {
        self.memory.insert(
            name.clone(), 
            Box::new(
                State::<T>::new(
                    name,
        )))
    }
    pub fn add_command(&mut self, command: String, callback: Callback) -> Result<(), ()> {
        let name = command.clone().split(".").next().ok_or(())?.to_string();
        StreamController::add_command(self.stream_id, command, name, callback)
     }
    pub fn is_initialized(&mut self) -> bool {
        if !self.initialized {
            self.initialized = self.memory.is_initialized();
        }
        self.initialized
    }
    pub fn initialize(&mut self) -> Result<(), ()> {
        if self.stream_id == -1 {
            return Err(());
        }
        if self.task_id.is_empty() {
            return Err(());
        }
        for data in self.memory.values_mut() {
            if !data.is_setted() {
                data.initialize();
            }
        }
        self.initialized = true;
        Ok(())
    }
    pub fn get_parameter<T: 'static>(&self, name: &String) -> Result<&Parameter<T>, ()> {
        self.memory.get(name).ok_or(())?.as_any().downcast_ref::<Parameter<T>>().ok_or(())
    }
    pub fn get_state<T: 'static>(&self, name: &String) -> Result<&State<T>, ()> {
        self.memory.get(name).ok_or(())?.as_any().downcast_ref::<State<T>>().ok_or(())
    }
    pub fn get_input<T: 'static>(&self, name: &String) -> Result<&Input<T>, ()> {
        self.inputs.get(name).ok_or(())?.as_any().downcast_ref::<Input<T>>().ok_or(())
    }
    pub fn get_output<T: 'static + Clone>(&self, name: &String) -> Result<&Output<T>, ()> {
        self.outputs.get(name).ok_or(())?.as_any().downcast_ref::<Output<T>>().ok_or(())
    }
    pub fn set_param<T: 'static>(&mut self, name: &String, value: T) -> Result<(), ()> 
    where T: 'static + Clone + Sync + Send + Float + PrimInt + Default {
        let param = self.memory.get_mut(name).ok_or(())?.as_any_mut().downcast_mut::<Parameter<T>>().ok_or(())?;
        param.set(value)
    }
    pub fn set_state<T: 'static>(&mut self, name: &String, value: T) -> Result<(), ()> 
    where T: 'static + Clone + Sync + Send + Default {
        let state = self.memory.get_mut(name).ok_or(())?.as_any_mut().downcast_mut::<State<T>>().ok_or(())?;
        state.set(value)
    }
    pub fn connect<T: 'static + Clone>(&mut self, output_name: &String, input_name: &String, other_block: &StreamBlock) -> Result<(), ()> {
        let output = self.outputs.get_mut(output_name).ok_or(())?;
        let output = output.as_any_mut().downcast_mut::<Output<T>>().ok_or(())?;
        let input = other_block.inputs.get(input_name).ok_or(())?;
        let input = input.as_any().downcast_ref::<Input<T>>().ok_or(())?;
        output.connect(input.get_sender());
        Ok(())
    }
    pub fn receive_input<T: 'static>(&self, name: &String) -> Result<T, ()> {
        let input = self.inputs.get(name).ok_or(())?.as_any().downcast_ref::<Input<T>>().ok_or(())?;
        input.receive()
    }
    pub fn send_output<T: 'static + Clone>(&self, name: &String, data: T) -> Result<(), ()> {
        let output = self.outputs.get(name).ok_or(())?.as_any().downcast_ref::<Output<T>>().ok_or(())?;
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
pub trait ProcessorBlockTrait: Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    fn name(&self) -> &DataHeader;
    fn proc_name(&self) -> &String;
    fn header(&self) -> &ProcessorHeader;
    fn lock() -> Result<MutexGuard<'static, Self>, ()> where Self: Sized;
    fn get_proc_state(&self) -> Result<StreamState, ()>;
    fn get_stream_block(&self) -> &StreamBlock;
    fn get_stream_block_mut(&mut self) -> &mut StreamBlock;
}
pub trait ProcessorTrait: ProcessorBlockTrait + Send + Sync {
    fn initialize(&mut self ) -> Result<(), ()>;
    fn process(&mut self) -> Result<(), ()>;
    fn finalize(&mut self) -> Result<(), ()>;
    fn get_processor_type(&self) -> StreamType {
        self.get_stream_block().get_processor_type()
    }
}
#[derive(Clone)]
pub struct ProcessorHeader {
    pub proc_name: String,
    pub description: String,
    pub license: String,
    pub version: String,
    pub author: String,
    pub email: String,
    pub repository: String,
}