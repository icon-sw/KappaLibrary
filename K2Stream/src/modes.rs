use std::{collections::HashMap, sync::{Arc, Mutex, OnceLock}, thread::JoinHandle};

use crate::{connections::ConnectionGraph, processors::{ProcessorTrait, StreamType}};

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
    running: Arc<Mutex<bool>>,
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
            running: Arc::new(Mutex::new(false)),
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
        *self.running.lock().unwrap() = true;
        while *self.running.lock().unwrap() {
            for block in self.blocks.values_mut() {
                if block.process().is_err() {
                    *self.running.lock().unwrap() = false;
                    return Err(())
                }
            }
        }   
        Ok(())
    }
    pub fn finalize(&mut self) -> Result<(), ()> {
        *self.running.lock().unwrap() = false;
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
    chains: HashMap<String, Arc<Mutex<Chain>>>,
    chain_results: HashMap<String, JoinHandle<Result<(), ()>>>,
}

impl OperativeMode {
    pub fn new(name: String, id: usize) -> Self {
        Self {
            name,
            id,
            chains: HashMap::new(),
            chain_results: HashMap::new(),
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
        self.chains.insert(name, Arc::new(Mutex::new(chain)));
        Ok(())
    }
    
    pub fn initialize(&mut self) -> Result<(), ()> {
        if self.stream_id == -1 {
            return Err(())
        }
        for chain in self.chains.values_mut() {
            let mut chain = chain.lock().map_err(|_| ())?;
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
            let chain_clone = chain.clone();
            let handle: JoinHandle<Result<(), ()>> = std::thread::spawn( move || {
                let mut chain = chain_clone.lock().map_err(|_| ())?;
                chain.process() 
            });
            let chain = chain.lock().map_err(|_| ())?;
            self.chain_results.insert(chain.name.clone(), handle);
        }
        Ok(())
    }
    pub fn finalize(&mut self) -> Result<(), ()> {
        let mut result = Ok(());
        for (chain_name, chain) in self.chains.iter() {
            let mut chain = chain.lock().map_err(|_| ())?;
            if chain.finalize().is_err() {
                result = Err(())
            }
            if let Some(handle) = self.chain_results.remove(chain_name) {
                if handle.join().map_err(|_| ())?.is_err() {
                    result = Err(())
                }
            } else {
                result = Err(())
            }
        }
        result
    }
}