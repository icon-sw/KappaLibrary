use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use k2_lang::{K2Object, K2ReturnStruct};
use k2_stream::k2err;
use k2_stream::processor::processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState};
use k2_stream::{errors::{K2ErrorCode, K2Error}, processor::memory::{DataHeader, MemoryTrait}};
use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;

use crate::application_coder::ApplicationCoder;
use crate::cargo_interface::CargoInterface;
use crate::library_coder::LibraryCoder;
use crate::processor_coder::ProcessCoder;

pub static CARGO_IF: OnceLock<CargoInterface> = OnceLock::new();
pub trait CoderTrait : Send + Sync {
    fn new(name: String, object: &K2Object) -> Result<Box<dyn CoderTrait>, String> where Self: Sized;
    fn get_name(&self) -> String;
    fn get_path(&self) -> String;
    fn set_parent_directory(&mut self, path: String);
    fn proc_new(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String>;
    fn proc_add(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String>;
    fn proc_delete(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String>;
    fn proc_set(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String>;
    fn proc_connect(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String>;
    fn proc_disconnect(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String>;
    fn proc_exec(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String>;
    fn generate(&self) -> Result<String, String>;
}


#[derive(K2Memory, K2ProcessorBlock)]
pub struct Coder {
    name: String,
    header: ProcessorHeader,
    stream_block: StreamBlock,
    state: Arc<Mutex<StreamState>>,
    coder_map: HashMap<String, Box<dyn CoderTrait>>,
}


impl ProcessorTrait for Coder {
    fn new(name: String) -> ProcessorNewReturn {
        let mut self_instance = Self {
            name: name.clone(),
            header: ProcessorHeader {
                proc_name: "Coder".to_string(),
                description: "A fake processor with no functionality".to_string(),
                version: "0.1.0".to_string(),
                author: "Sofia".to_string(),
                email: "ms.sofia.silvestri@gmail.com".to_string(),
                license: "LGPLv2.0".to_string(),
                repository: "".to_string(),
            },
            stream_block: StreamBlock::new(),
            state: Arc::new(Mutex::new(StreamState::Uninitialized)),
            coder_map: HashMap::new(),
        };
        self_instance.get_stream_block_mut().add_input::<K2ReturnStruct>("node_input".to_string())?;
        self_instance.get_stream_block_mut().add_output::<String>("response".to_string())?;
        Ok(Box::new(self_instance))
    }
    fn initialize(&mut self ) -> Result<(), K2Error> {
        *self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to update status"))? = StreamState::Initialized;
        Ok(())
    }
    fn process(&mut self) -> Result<(), K2Error> {
        let command_input = self.stream_block.get_input::<K2ReturnStruct>(&"command".to_string())?;
        let command = command_input.receive()?;
        let mut response = command.message.clone();
        if command.success {
            match self.execute(&command) {
                Ok(s) => {response = s;}
                Err(e) => {response = e;}
            }
        }
        let response_output = self.stream_block.get_output::<String>(&"response".to_string())?;
        response_output.send(response)?;
        Ok(())
    }
    fn finalize(&mut self) -> Result<(), K2Error> {
        *self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to update status"))? = StreamState::Waiting;
        Ok(())
    }
}

impl Coder {
    fn execute(&mut self, input_struct: &K2ReturnStruct) -> Result<String, String> {
        let command = input_struct.tokens[0].clone();
        let response;
        if command == "new".to_string() {
            if input_struct.tokens.len() < 3 {
                return Err("Wrong argument number".to_string());
            }
            let object_type = input_struct.tokens[1].clone();
            let object_name = input_struct.tokens[2].clone();
            if self.coder_map.contains_key(&object_name.clone()) {
                return Err("Object already present".to_string());
            }
            let object_vec = input_struct.data.clone();
            if object_vec.len() == 0 {
                return Err("Missing objects to create".to_string());
            }
            if object_vec.len() != 1 {
                return Err("Can create one object at time".to_string());
            }
            let object = object_vec[0].clone();
            match object_type.as_str() {
                "library" => {
                    self.coder_map.insert(object_name.clone(), LibraryCoder::new(object_name.clone(), &object)?);
                    response = format!("Library {} successfull created", object_name.clone());
                }
                "application" => {
                    self.coder_map.insert(object_name.clone(), ApplicationCoder::new(object_name.clone(), &object)?);
                    response = format!("Application {} successfull created", object_name.clone());
                }
                _ => {
                    let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                    if self.coder_map.contains_key(&split_name[0].clone()) {
                        response = self.coder_map.get_mut(&split_name[0]).unwrap().proc_new(&input_struct)?;
                    } else {
                        return Err("Not valid object".to_string());
                    }
                }
            }
        } else {
            let object_name = input_struct.tokens[1].clone();
            let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
            let coder: &mut Box<dyn CoderTrait>;
            if self.coder_map.contains_key(&split_name[0].clone()) {
                if self.coder_map.contains_key(&format!("{}.{}",split_name[0], split_name[1])) {
                    coder = self.coder_map.get_mut(&format!("{}.{}",split_name[0], split_name[1])).unwrap();
                } else {
                    coder = self.coder_map.get_mut(&split_name[0]).unwrap();
                }
            } else {
                return Err("Not valid object".to_string());
            }
            response = match command.as_str() {
                "add" => {coder.proc_add(input_struct)?}
                "delete" => {coder.proc_delete(input_struct)?}
                "set" => {coder.proc_set(input_struct)?}
                "connect" => {coder.proc_connect(input_struct)?}
                "disconnect" => {coder.proc_disconnect(input_struct)?}
                "exec" => {coder.proc_exec(input_struct)?}
                _ => {return Err("Unknow command".to_string());}
            }
        }
        Ok(response)
    }
    
}