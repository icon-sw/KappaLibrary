use std::{collections::HashMap, sync::{Mutex, OnceLock}};

use crate::{connections::ConnectionGraph, processors::{ProcessorTrait, StreamBlock, StreamType}};

static TASK_ID_COUNTER: OnceLock<Mutex<isize>> = OnceLock::new();

#[derive(Clone)]
pub struct Connection {
    from: String,
    to: String,
}

impl Connection {
    pub fn from(&self) -> &String { &self.from }
    pub fn to(&self) -> &String { &self.to }
}

pub struct Chain {
    pub name: String,
    stream_id: isize,
    task_id: isize,
    blocks: HashMap<String, Box<dyn ProcessorTrait>>,
    connections: ConnectionGraph,
    input_present: bool,
    initialized: bool,
}

impl Chain {
    pub fn new(name: String) -> Self {
        let mut task_lock = TASK_ID_COUNTER.get_or_init(|| Mutex::new(0)).lock().unwrap();
        let task_id = *task_lock;
        *task_lock = task_id + 1;
        Self {
            name,
            blocks: HashMap::new(),
            connections: ConnectionGraph::new(),
            input_present: false,
            initialized: false,
            stream_id: -1 as isize,
            task_id: task_id,
        }
    }
    pub fn get_stream_id(&self) -> isize {
        self.stream_id
    }
    pub fn get_task_id(&self) -> isize {
        self.task_id
    }
    pub fn set_stream_id(&mut self, stream_id: isize) {
        self.stream_id = stream_id;
    }
    pub fn add_block(&mut self, mut block: Box<dyn ProcessorTrait>) -> Result<(), ()> {
        if block.get_processor_type() == StreamType::RECEIVER {
            if self.input_present {
                return Err(())
            }
            self.input_present = true;
        }
        block.get_stream_block_mut().set_task_id(self.task_id);
        block.get_stream_block_mut().set_stream_id(self.stream_id);
        self.blocks.insert(block.name().clone(), block);
        Ok(())
    }
    pub fn connect<T: 'static + Clone>(&mut self, output_name: String, input_name: String) -> Result<(), ()> {
        let mut from_lock: &mut Box<dyn ProcessorTrait>;
        let mut to_lock: &mut Box<dyn ProcessorTrait>;
        let mut from_block: Option<&mut StreamBlock> = None;
        let mut to_block: Option<&mut StreamBlock> = None;
        let output_split: Vec<&str> = output_name.split(".").collect();
        let input_split: Vec<&str> = input_name.split(".").collect();
        if output_split.len() != 2 || input_split.len() != 2 {
            return Err(());
        }
        for process in self.blocks.values_mut() {
            if process.name() == &output_split.get(0).unwrap().to_string() {
                from_lock = process;
                from_block = Some(from_lock.get_stream_block_mut());
            } else if process.name() == &input_split.get(0).unwrap().to_string() {
                to_lock = process;
                to_block = Some(to_lock.get_stream_block_mut());
            }
            if from_block.is_some() && to_block.is_some() {
                break
            }
        }
        if let (Some(from_block), Some(to_block)) = (from_block, to_block) {
            from_block.connect::<T>(
                &output_split.get(1).unwrap().to_string(),
                &input_split.get(1).unwrap().to_string(),
                to_block)?;

            self.connections.add_connection(
                output_split.get(0).unwrap().to_string(),
                input_split.get(0).unwrap().to_string(),
            );
            Ok(())
        } else {
            Err(())
        }
    }
    pub fn get_blocks(&self, id: String) -> Result<&Box<dyn ProcessorTrait>, ()> {
        self.blocks.get(&id).ok_or(())
    }
    pub fn get_blocks_mut(&mut self, id: String) -> Result<&mut Box<dyn ProcessorTrait>, ()> {
        self.blocks.get_mut(&id).ok_or(())
    }

    pub fn initialize(&mut self) -> Result<(), ()> {
        if !self.initialized {
            if self.stream_id == -1 {
                return Err(())
            }
            self.connections.check()?;
        }
        for block_name in self.connections.get_nodes().rev() {
            let block = self.blocks.get_mut(block_name).ok_or(())?;
            if block.initialize().is_err() {
                return Err(())
            }
        }
        self.initialized = true;
        Ok(())
    }
    pub fn process(&mut self) -> Result<(), ()> {
        if !self.initialized {
            return Err(());
        }
        for block in self.blocks.values_mut() {
            if block.process().is_err() {
                return Err(())
            }
        }
        Ok(())
    }
    pub fn finalize(&mut self) -> Result<(), ()> {
        for block in self.blocks.values_mut() {
            if block.finalize().is_err() {
                return Err(())
            }
        }
        Ok(())
    }
}

pub struct OperativeMode {
    pub name: String,
    pub id: usize,
    stream_id: isize,
    chains: HashMap<String, Chain>,
    connections: ConnectionGraph,
}

impl OperativeMode {
    pub fn new(name: String, id: usize) -> Self {
        Self {
            name,
            id,
            chains: HashMap::new(),
            connections: ConnectionGraph::new(),
            stream_id: -1,
        }
    }
    pub fn get_stream_id(&self) -> isize {
        self.stream_id
    }
    pub fn set_stream_id(&mut self, stream_id: isize) {
        self.stream_id = stream_id;
    }
    pub fn add_chain(&mut self, name: String, mut chain: Chain) -> Result<(), ()> {
        if self.chains.contains_key(&name) {
            return Err(())
        }
        chain.set_stream_id(self.stream_id);
        self.chains.insert(name, chain);
        Ok(())
    }
    pub fn connect<T: 'static + Clone>(&mut self, output_name: String, input_name: String) -> Result<(),()>{
        let mut from_block: Option<&mut StreamBlock> = None;
        let mut to_block: Option<&mut StreamBlock> = None;
        let mut from_binding;
        let mut to_binding;
        let output_split: Vec<&str> = output_name.split(".").collect();
        let input_split: Vec<&str> = input_name.split(".").collect();
        if output_split.len() != 3 || input_split.len() != 3 {
            return Err(());
        }
        let output_chain = output_split.get(0).unwrap().to_string();
        let input_chain = input_split.get(0).unwrap().to_string();
        let output_name = format!("{}.{}", output_split.get(1).unwrap(), output_split.get(2).unwrap());
        let input_name = format!("{}.{}", input_split.get(1).unwrap(), input_split.get(2).unwrap());
        if output_chain.eq(&input_chain) {
            let chain = self.chains.get_mut(&output_chain).ok_or(())?;
            return chain.connect::<T>(output_name.clone(), input_name.clone())
        } else {
            let output_block = output_split.get(1).unwrap().to_string();
            let input_block = input_split.get(1).unwrap().to_string();    
            for chain in self.chains.values_mut() {
                if chain.name == output_chain.clone() {
                    from_binding = chain.get_blocks_mut(output_block.clone())?;
                    from_block = Some(from_binding.get_stream_block_mut());
                } else if chain.name == input_chain {
                    to_binding = chain.get_blocks_mut(input_block.clone())?;
                    to_block = Some(to_binding.get_stream_block_mut());
                }

                if from_block.is_some() && to_block.is_some() {
                    break;
                }
            }
            if let (Some(from_block), Some(to_block)) = (from_block, to_block) {
                from_block.connect::<T>(
                    &output_split.get(2).unwrap().to_string(), 
                    &input_split.get(2).unwrap().to_string(), 
                    to_block)?;
                self.connections.add_connection(
                    output_split.get(1).unwrap().to_string(),
                    input_split.get(1).unwrap().to_string());
                Ok(())
            } else {
                Err(())
            }
        }

    }
    pub fn initialize(&mut self) -> Result<(), ()> {
        if self.stream_id == -1 {
            return Err(())
        }
        for chain in self.chains.values_mut() {
            if chain.initialize().is_err() {
                return Err(())
            }
        }
        Ok(())
    }
    pub fn process(&mut self) -> Result<(), ()> {
        if self.stream_id == -1 {
            return Err(())
        }
        for chain in self.chains.values_mut() {
            // Todo: Gestione dei task
            std::thread::scope( |s | {
                s.spawn(|| chain.process());
            } );
        }
        Ok(())
    }
    pub fn finalize(&mut self) -> Result<(), ()> {
        for chain in self.chains.values_mut() {
            if chain.finalize().is_err() {
                return Err(())
            }
        }
        Ok(())
    }
}