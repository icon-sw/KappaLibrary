use std::{collections::HashMap, sync::{Arc, Mutex, OnceLock}, thread::JoinHandle};

use crate::{connections::ConnectionGraph, errors::{K2Error, K2ErrorCode}, k2err, processors::{StreamBlock, StreamType}, stream_controller::StreamController};

static TASK_ID_COUNTER: OnceLock<Mutex<isize>> = OnceLock::new();

pub type ChainType = Arc<Mutex<Chain>>;

pub struct ChainBuilder {}

impl ChainBuilder {
    pub fn create(name: String) -> Result<ChainType, K2Error>{
        let chain = Chain::new(name);
        Ok(Arc::new(Mutex::new(chain)))
    }
}
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
    blocks: Vec<String>,
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
            blocks: Vec::new(),
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
    pub fn 
    add_block(&mut self, block_name: String, block: &StreamBlock) -> Result<(),K2Error> {
        if block.get_processor_type() == StreamType::RECEIVER {
            if self.input_present {
                return Err(K2Error { code: K2ErrorCode::AlreadyExists, message: "Input block already exists".into() });
            }
            self.input_present = true;
        }
        self.blocks.push(block_name.clone());
        Ok(())
    }

    pub fn initialize(&mut self) -> Result<(), K2Error> {
        if !self.initialized {
            if self.stream_id == -1 {
                return Err(K2Error { code: K2ErrorCode::Uninitialized, message: "Stream ID is not set".into() });
            }
            self.connections.check()?;
        }
        let stream_processor_arc = StreamController::get_stream_by_id(self.get_stream_id())?;
        let mut stream_processor = stream_processor_arc.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream processor".into() })?;
        
        for block_name in self.blocks.iter().rev() {
            let block = stream_processor.get_processors_mut(block_name.clone())?;
            if block.initialize().is_err() {
                return Err(K2Error { code: K2ErrorCode::ProcessError, message: "Failed to initialize processor".into() });
            }
        }
        self.initialized = true;
        Ok(())
    }
    pub fn process(&mut self) -> Result<(), K2Error> {
        if !self.initialized {
            return Err(K2Error { code: K2ErrorCode::Uninitialized, message: "Chain is not initialized".into() });
        }
        let stream_processor_arc = StreamController::get_stream_by_id(self.get_stream_id())?;
        *self.running.lock().unwrap() = true;
        while *self.running.lock().unwrap() {
            for block_name in self.blocks.iter().rev() {
                let mut stream_processor = stream_processor_arc.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream processor".into() })?;
                let block = stream_processor.get_processors_mut(block_name.clone())?;
                if block.process().is_err() {
                    *self.running.lock().unwrap() = false;
                    return Err(K2Error { code: K2ErrorCode::ProcessError, message: "Failed to process chain".into() });
                }
            }
        }   
        Ok(())
    }
    pub fn finalize(&mut self) -> Result<(), K2Error> {
        *self.running.lock().unwrap() = false;
        let stream_processor_arc = StreamController::get_stream_by_id(self.get_stream_id())?;
        for block_name in self.blocks.iter().rev() {
            let mut stream_processor = stream_processor_arc.lock().map_err(|_| ())?;
            let block = stream_processor.get_processors_mut(block_name.clone())?;
            if block.finalize().is_err() {
                return Err(K2Error { code: K2ErrorCode::ProcessError, message: "Error in chain finalize".to_string() })
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
    chain_results: HashMap<String, JoinHandle<Result<(), K2Error>>>,
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
    pub fn add_chain(&mut self, name: String, chain: Arc<Mutex<Chain>>) -> Result<(), K2Error> {
        if self.chains.contains_key(&name) {
            return Err(K2Error { code: K2ErrorCode::AlreadyExists, message: "Chain already exists".into() });
        }
        match chain.lock() {
            Ok(mut chain) => {
                (*chain).set_stream_id(self.stream_id);
            }
            Err(_) => {
                return Err(k2err!(K2ErrorCode::LockError, ""));
            }
        }
        dbg!(self.stream_id);
        self.chains.insert(name, chain);
        Ok(())
    }
    pub fn get_chain(&self, name: &String) -> Result<&Arc<Mutex<Chain>>, K2Error> {
        self.chains.get(name).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "Chain not found".into() })
    }
    pub fn get_chain_mut(&mut self, name: &String) -> Result<&mut Arc<Mutex<Chain>>, K2Error> {
        self.chains.get_mut(name).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "Chain not found".into() })
    }
    pub fn initialize(&mut self) -> Result<(), K2Error> {
        if self.stream_id == -1 {
            return Err(K2Error { code: K2ErrorCode::Uninitialized, message: "Stream ID is not set".into() });
        }
        for chain in self.chains.values_mut() {
            let mut chain = chain.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock chain".into() })?;
            if chain.initialize().is_err() {
                return Err(K2Error { code: K2ErrorCode::ProcessError, message: "Failed to initialize chain".into() });
            }
        }
        Ok(())
    }
    pub fn process(&mut self) -> Result<(), K2Error> {
        if self.stream_id == -1 {
            return Err(K2Error { code: K2ErrorCode::Uninitialized, message: "Stream ID is not set".into() });
        }
        for chain in self.chains.values_mut() {
            // Todo: Gestione dei task
            let chain_clone = chain.clone();
            let handle: JoinHandle<Result<(), K2Error>> = std::thread::spawn( move || {
                let mut chain = chain_clone.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock chain".into() })?;
                chain.process() 
            });
            let chain = chain.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock chain".into() })?;
            self.chain_results.insert(chain.name.clone(), handle);
        }
        Ok(())
    }
    pub fn finalize(&mut self) -> Result<(), K2Error> {
        let mut result = Ok(());
        for (chain_name, chain) in self.chains.iter() {
            let mut chain = chain.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock chain".into() })?;
            if chain.finalize().is_err() {
                result = Err(K2Error { code: K2ErrorCode::ProcessError, message: "Failed to finalize chain".into() });
            }
            if let Some(handle) = self.chain_results.remove(chain_name) {
                if handle.join().map_err(|_| K2Error { code: K2ErrorCode::ProcessError, message: "Failed to join chain thread".into() })?.is_err() {
                    result = Err(K2Error { code: K2ErrorCode::ProcessError, message: "Failed to finalize chain thread".into() });
                }
            } else {
                result = Err(K2Error { code: K2ErrorCode::NotFound, message: "Chain handle not found".into() });
            }
        }
        result
    }
}