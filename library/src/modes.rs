use std::{cell::{RefCell, RefMut}, collections::HashMap};

use crate::{connections::ConnectionGraph, processors::{ProcessorTrait, StreamBlock, StreamType}};
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
    blocks: HashMap<String, RefCell<Box<dyn ProcessorTrait>>>,
    connections: ConnectionGraph,
    input_present: bool,
    initialized: bool,
}

impl Chain {
    pub fn new(name: String) -> Self {
        Self {
            name,
            blocks: HashMap::new(),
            connections: ConnectionGraph::new(),
            input_present: false,
            initialized: false,
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
    pub fn connect<T: 'static + Clone>(&mut self, output_name: String, input_name: String) -> Result<(), ()> {
        let mut from_lock: RefMut<'_, Box<dyn ProcessorTrait>>;
        let mut to_lock: RefMut<'_, Box<dyn ProcessorTrait>>;
        let mut from_block: Option<&mut StreamBlock> = None;
        let mut to_block: Option<&mut StreamBlock> = None;
        let output_split: Vec<&str> = output_name.split(".").collect();
        let input_split: Vec<&str> = input_name.split(".").collect();
        if output_split.len() != 2 || input_split.len() != 2 {
            return Err(());
        }
        for process in self.blocks.values_mut() {
            let proc = process.borrow_mut();
            if proc.name() == &output_split.get(0).unwrap().to_string() {
                from_lock = proc;
                from_block = Some(from_lock.get_stream_block_mut());
            } else if proc.name() == &input_split.get(0).unwrap().to_string() {
                to_lock = proc;
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
    pub fn get_blocks(&self, id: String) -> Option<&RefCell<Box<dyn ProcessorTrait>>> {
        self.blocks.get(&id)
    }
    pub fn get_blocks_mut(&mut self, id: String) -> Option<&mut RefCell<Box<dyn ProcessorTrait>>> {
        self.blocks.get_mut(&id)
    }

    pub fn initialize(&mut self) -> Result<(), ()> {
        if !self.initialized {
            self.connections.check()?;
            self.initialized = true;
        }
        for block_name in self.connections.get_nodes().rev() {
            let block = self.blocks.get_mut(block_name).ok_or(())?;
            if block.borrow_mut().initialize().is_err() {
                return Err(())
            }
        }
        Ok(())
    }
    pub fn process(&mut self) -> Result<(), ()> {
        if !self.initialized {
            return Err(());
        }
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
    connections: ConnectionGraph,
}

impl OperativeMode {
    pub fn new(name: String, id: usize) -> Self {
        Self {
            name,
            id,
            chains: HashMap::new(),
            connections: ConnectionGraph::new(),
        }
    }
    pub fn add_chain(&mut self, name: String, chain: Chain) -> Result<(), ()> {
        if self.chains.contains_key(&name) {
            return Err(())
        }
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
                    from_binding = chain.get_blocks_mut(output_block.clone()).ok_or(())?.borrow_mut();
                    from_block = Some(from_binding.get_stream_block_mut());
                } else if chain.name == input_chain {
                    to_binding = chain.get_blocks_mut(input_block.clone()).ok_or(())?.borrow_mut();
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