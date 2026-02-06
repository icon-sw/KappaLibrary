use std::{collections::HashMap, sync::MutexGuard};
use num_traits::{Float, PrimInt};
use rand::Rng;

use crate::connections::{Input, Output};
use crate::memory::{DataHeader, DataTrait, Memory, MemoryTrait};
use crate::parameter::{Parameter, ParameterType, ParameterValueType};
use crate::states::State;
#[derive(PartialEq, Clone)]
pub enum StreamType {
    NONE,
    RECEIVER,
    SENDER,
    BOTH,
}

pub struct StreamBlock {
    id: usize,
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
            inputs: HashMap::new(),
            outputs: HashMap::new(),
            memory: Memory::new(),
            initialized: false,
        }
    }
    pub fn get_id(&self) -> usize {
        self.id
    }
    pub fn add_input<T: 'static>(&mut self, name: String) 
    where Input<T> : MemoryTrait{
        self.inputs.insert(name.clone(), Box::new(Input::<T>::new(name)));
    }
    pub fn add_output<T: 'static + Clone>(&mut self, name: String) 
    where Output<T> : MemoryTrait {
        self.outputs.insert(name.clone(), Box::new(Output::<T>::new(name)));
    }
    pub fn add_parameter<T: 'static>(&mut self, kind: ParameterValueType, name: String, parameter: ParameterType) 
    where   T: 'static + Clone + Sync + Send + Float + PrimInt + Default,
            Result<Parameter<T>, ()>: DataTrait 
    {
        match kind {
            ParameterValueType::INTEGER => {
                let _ = self.memory.insert(
                    name.clone(), 
                    Box::new(
                        Parameter::<T>::int(
                            name, 
                            T::default(), 
                            parameter, 
                )));
            }
            ParameterValueType::FLOAT => {
                let _ = self.memory.insert(
                    name.clone(), 
                    Box::new(
                        Parameter::<T>::float(
                            name, T::default(), 
                            parameter, 
                )));
            }
        }
    }
    pub fn add_state<T>(&mut self, name: String) 
    where T: 'static + Clone + Sync + Send + Default,
    Result<State<T>, ()>: DataTrait 
    {
        let _ = self.memory.insert(
            name.clone(), 
            Box::new(
                State::<T>::new(
                    name,
        )));
    }
    pub fn is_initialized(&mut self) -> bool {
        if !self.initialized {
            self.initialized = self.memory.is_initialized();
        }
        self.initialized
    }
    pub fn initialize(&mut self) -> Result<(), ()> {
        for data in self.memory.values_mut() {
            if !data.is_setted() {
                data.initialize();
            }
        }
        self.initialized = true;
        Ok(())
    }
    pub fn get_parameter<T: 'static>(&self, name: &String) -> Option<&Parameter<T>> {
        self.memory.get(name)?.as_any().downcast_ref::<Parameter<T>>()
    }
    pub fn get_state<T: 'static>(&self, name: &String) -> Option<&State<T>> {
        self.memory.get(name)?.as_any().downcast_ref::<State<T>>()
    }
    pub fn get_input<T: 'static>(&self, name: &String) -> Option<&Input<T>> {
        self.inputs.get(name)?.as_any().downcast_ref::<Input<T>>()
    }
    pub fn get_output<T: 'static + Clone>(&self, name: &String) -> Option<&Output<T>> {
        self.outputs.get(name)?.as_any().downcast_ref::<Output<T>>()
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

pub trait ProcessorTrait :Send + Sync{
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    fn name(&self) -> &DataHeader;
    fn proc_name(&self) -> &String;
    fn header(&self) -> &ProcessorHeader;
    fn lock() -> Result<MutexGuard<'static, Self>, ()> where Self: Sized;
    fn initialize(&mut self) -> Result<(), ()>;
    fn process(&mut self) -> Result<(), ()>;
    fn finalize(&mut self) -> Result<(), ()>;
    fn get_proc_state(&self) -> Result<(), ()>;
    fn get_stream_block(&self) -> &StreamBlock;
    fn get_stream_block_mut(&mut self) -> &mut StreamBlock;
    fn get_processor_type(&self) -> StreamType {
        self.get_stream_block().get_processor_type()
    }
}
pub struct ProcessorHeader {
    pub proc_name: String,
    pub version: String,
    pub author: String,
    pub description: String,
    pub license: String,
    pub repository: String,
}