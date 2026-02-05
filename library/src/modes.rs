use std::{cell::{RefCell, RefMut}, collections::{HashMap, VecDeque}};

use crate::processors::{ProcessorTrait, StreamBlock, StreamType};

pub struct Connection {
    from: String,
    output_name: String,
    to: String,
    input_name: String,
}

impl Connection {
    pub fn from(&self) -> &String { &self.from }
    pub fn output(&self) -> &String { &self.output_name }
    pub fn to(&self) -> &String { &self.to }
    pub fn input(&self) -> &String { &self.input_name }
}

pub struct Chain {
    pub name: String,
    blocks: HashMap<String, RefCell<Box<dyn ProcessorTrait>>>,
    connections: Vec<Connection>,
    input_present: bool,
    is_initialized: bool,
}

impl Chain {
    pub fn new(name: String) -> Self {
        Self {
            name,
            blocks: HashMap::new(),
            connections: Vec::new(),
            input_present: false,
            is_initialized: false,
        }
    }
    pub fn add_block(&mut self, block: Box<dyn ProcessorTrait>) -> Result<(), ()> {
        if block.get_processor_type() == StreamType::RECEIVER {
            if self.input_present {
                return Err(())
            }
            self.input_present = true;
        }
        
        self.blocks.insert(block.name().clone(), RefCell::new(block));
        Ok(())
    }
    pub fn connect<T: 'static + Clone>(&mut self, output_block_id: String, output_name: String, input_block_id: String, input_name: String) -> Result<(), ()> {
        let mut from_lock: RefMut<'_, Box<dyn ProcessorTrait>>;
        let mut to_lock: RefMut<'_, Box<dyn ProcessorTrait>>;
        let mut from_block: Option<&mut StreamBlock> = None;
        let mut to_block: Option<&mut StreamBlock> = None;
        for process in self.blocks.values_mut() {
            let proc = process.borrow_mut();
            if proc.name() == &output_block_id {
                from_lock = proc;
                from_block = Some(from_lock.get_stream_block_mut());
            } else if proc.name() == &input_block_id {
                to_lock = proc;
                to_block = Some(to_lock.get_stream_block_mut());
            }
            if from_block.is_some() && to_block.is_some() {
                break
            }
        }
        if let (Some(from_block), Some(to_block)) = (from_block, to_block) {
            from_block.connect::<T>(&output_name, &input_name, to_block)?;
            self.connections.push(Connection {
                from: output_block_id,
                output_name,
                to: input_block_id,
                input_name,
            });
            Ok(())
        } else {
            Err(())
        }
    }
    pub fn get_blocks(&self, id: String) -> Option<&RefCell<Box<dyn ProcessorTrait>>> {
        self.blocks.get(&id)
    }
    pub fn get_blocks_mut(&mut self, id: String) -> Option<&mut RefCell<Box<dyn ProcessorTrait>>> {
        self.blocks.get_mut(&id)
    }
    pub fn initialize(&mut self) -> Result<(), ()> {
        if !self.is_initialized {
            // TODO: Sort of blocks
            self.is_initialized = true;
        }
        for block in self.blocks.values_mut() {
            if block.borrow_mut().initialize().is_err() {
                return Err(())
            }
        }
        Ok(())
    }
    pub fn process(&mut self) -> Result<(), ()> {
        for block in self.blocks.values_mut() {
            if block.borrow_mut().process().is_err() {
                return Err(())
            }
        }
        Ok(())
    }
    pub fn finalize(&mut self) -> Result<(), ()> {
        for block in self.blocks.values_mut() {
            if block.borrow_mut().finalize().is_err() {
                return Err(())
            }
        }
        Ok(())
    }
}

pub struct OperativeMode {
    pub name: String,
    pub id: usize,
    chains: HashMap<String, Chain>,
    connections: Vec<Connection>,
}

impl OperativeMode {
    pub fn new(name: String, id: usize) -> Self {
        Self {
            name,
            id,
            chains: HashMap::new(),
            connections: Vec::new(),
        }
    }
    pub fn add_chain(&mut self, id: String, chain: Chain) -> Result<(), ()> {
        if self.chains.contains_key(&id) {
            return Err(())
        }
        let name  = format!("{}.{}", self.name, id);
        self.chains.insert(name, chain);
        Ok(())
    }
    pub fn connect<T: 'static + Clone>(&mut self, output_block_id: String, output_name: String, input_block_id: String, input_name: String) -> Result<(), ()> {
        let mut from_block: Option<&mut StreamBlock> = None;
        let mut to_block: Option<&mut StreamBlock> = None;
        let mut from_binding;
        let mut to_binding;
        for chain in self.chains.values_mut() {
            if chain.get_blocks(output_block_id.clone()).is_some() {
                from_binding = chain.get_blocks_mut(output_block_id.clone()).ok_or(())?.borrow_mut();
                from_block = Some(from_binding.get_stream_block_mut());
            } else {
                if chain.get_blocks(input_block_id.clone()).is_some() {
                    to_binding = chain.get_blocks_mut(input_block_id.clone()).ok_or(())?.borrow_mut();
                    to_block = Some(to_binding.get_stream_block_mut());
                }
            }
            if from_block.is_some() && to_block.is_some() {
                break
            }
        }
        if let (Some(from_block), Some(to_block)) = (from_block, to_block) {
            from_block.connect::<T>(&output_name, &input_name, to_block)?;
            self.connections.push(Connection {
                from: output_block_id.clone(),
                output_name,
                to: input_block_id.clone(),
                input_name,
            });
            Ok(())
        } else {
            Err(())
        }
    }
    pub fn initialize(&mut self) -> Result<(), ()> {
        for chain in self.chains.values_mut() {
            if chain.initialize().is_err() {
                return Err(())
            }
        }
        Ok(())
    }
    pub fn process(&mut self) -> Result<(), ()> {
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