use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::io::Write;

use rand::{RngExt, rng};

use k2stream::processor::processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState};
use k2stream::{errors::{K2ErrorCode, K2Error}, k2err, processor::memory::{DataHeader, MemoryTrait}};
use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;

use k2lang::{K2LangReturn, K2LangStruct};

use crate::application_coder::ApplicationCoder;
use crate::library_coder::LibraryCoder;

pub type K2CoderReturn = Result<String, K2Error>;
pub trait CoderTrait: Send + Sync {
    fn execute(&mut self, input: &K2LangStruct) -> K2CoderReturn;
    fn generate(&mut self) -> K2CoderReturn;
    fn build(&self) -> K2CoderReturn;

    fn get_tmp_file(&self) -> String {
        const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let mut rng = rng();
        let random_string: String = (0..16)
            .map(|_| {
                let idx = rng.random_range(0..CHARSET.len());
                CHARSET[idx] as char
            })
            .collect();
        
        format!("/tmp/processor_coder_{}.rs", random_string)
    }

    fn file_write(&self, path: String, content: String) -> Result<(), K2Error> {
        let mut file = match std::fs::File::create(&path) {
            Ok(file) => file,
            Err(e) => return Err(k2err!(K2ErrorCode::GenericError, format!("Error creating file {}: {}", path, e))),
        };
        match file.write_all(content.as_bytes()) {
            Ok(_) => Ok(()),
            Err(e) => Err(k2err!(K2ErrorCode::GenericError, format!("Error writing to file {}: {}", path, e))),
        }
    }

    fn file_move(&self, src: &String, dest: &String) -> Result<(), K2Error> {
        match std::fs::rename(src, dest) {
            Ok(_) => Ok(()),
            Err(e) => Err(k2err!(K2ErrorCode::GenericError, format!("Error moving file from {} to {}: {}", src, dest, e))),
        }
    }
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
        self_instance.get_stream_block_mut().add_input::<K2LangReturn>("input".to_string())?;
        self_instance.get_stream_block_mut().add_output::<K2CoderReturn>("output".to_string())?;
        Ok(Box::new(self_instance))
    }
    fn initialize(&mut self ) -> Result<(), K2Error> {
        let mut state = self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to set state"))?;
        *state = StreamState::Initialized;
        Ok(())
    }
    fn process(&mut self) -> Result<(), K2Error> {
        let command_input = self.get_stream_block_mut().receive_input::<K2LangReturn>(&"input".to_string())?;
        let command_return: K2CoderReturn;
        match command_input {
            Ok(input) => {
                let coder = self.get_coder(&input)?;
                command_return = coder.execute(&input);
            }
            Err(err) => {
                command_return = Err(err);
            }
        }
        self.get_stream_block_mut().send_output::<K2CoderReturn>(&"output".to_string(), command_return)?;
        Ok(())
    }
    fn finalize(&mut self) -> Result<(), K2Error> {
        let mut state = self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to set state"))?;
        *state = StreamState::Waiting;
        Ok(())
    }
}

impl Coder {
    fn get_coder(&mut self, input: &K2LangStruct) -> Result<&mut Box<dyn CoderTrait>, K2Error> {
        let split_name: Vec<String> = input.data[0].name.split(".").map(|s| s.to_string()).collect();
        if split_name[0] == input.data[0].name && input.tokens[0] == "new".to_string() {
            if self.coder_map.contains_key(&input.data[0].name) {
                return Err(k2err!(K2ErrorCode::AlreadyExists, format!("Coder {} already exists", input.data[0].name)));
            }
            if input.data[0].object_type == "library" {
                self.coder_map.insert(input.data[0].name.clone(), Box::new(LibraryCoder::new(&input.data[0])?));
            } else {
                self.coder_map.insert(input.data[0].name.clone(), Box::new(ApplicationCoder::new(&input.data[0])?));
            }
        }
        let coder = self.coder_map.get_mut(&split_name[0]).ok_or(k2err!(K2ErrorCode::NotFound, format!("Coder {} not found", split_name[0])))?;
        Ok(coder)
    }
    pub fn to_snake_case(s: &String) -> String {
        let mut result = String::new();
        let mut chars = s.chars().peekable();

        while let Some(c) = chars.next() {
            if c.is_ascii_uppercase() {
                if !result.is_empty() {
                    let next_char_is_lowercase = chars.peek().map_or(false, |&next| next.is_ascii_lowercase());
                    if next_char_is_lowercase {
                        result.push('_');
                    } else if result.chars().last().map_or(false, |last| !last.is_ascii_uppercase() && last != '_') {
                        result.push('_');
                    }
                }
                result.push(c.to_ascii_lowercase());
            } else {
                result.push(c);
            }
        }
        result
    }
}