use std::{collections::HashMap};

use k2lang::{K2LangObject, K2LangStruct};
use k2stream::{errors::{K2Error, K2ErrorCode}, k2err};

use crate::{CARGO_IF, coder::{CoderTrait, K2CoderReturn}, streamer_coder::StreamerCoder};

pub struct ApplicationCoder {
    application_path: String,
    file_path: String,
    stream_coder: HashMap<String, Box<dyn CoderTrait>>
}

impl ApplicationCoder {
    pub fn new(object: &K2LangObject) -> Result<Self, K2Error> {
        let path = object.properties.get(&"path".to_string())
            .ok_or(k2err!(K2ErrorCode::NotFound, "Path properties not found"))?;
        let application_path = format!("{}/{}", path, object.name.clone());
        let cargo_if = CARGO_IF.get()
            .ok_or(k2err!(K2ErrorCode::Uninitialized, "Cargo interface not initialized".to_string()))?
            .lock().map_err(|_| k2err!(K2ErrorCode::LockError, "".to_string()))?;
        cargo_if.cargo_new_application(application_path.clone()).map_err(|err| k2err!(K2ErrorCode::GenericError, err))?;
        cargo_if.cargo_add_commands(application_path.clone()).map_err(|err| k2err!(K2ErrorCode::GenericError, err))?;
        
        Ok(Self {
            application_path: application_path.clone(),
            file_path: format!("{}/src/main.rs", application_path.clone()),
            stream_coder: HashMap::new()
        })
    }
}
impl CoderTrait for ApplicationCoder {
    fn execute(&mut self, input: &K2LangStruct) -> K2CoderReturn {
        if input.tokens[0] == "new".to_string() && input.data[0].object_type == "application".to_string() {    
            return Ok("Ok".to_string());
        }
        match input.tokens[0].as_str() {
            "new" => {
                if input.data[0].object_type == "stream" {
                    if self.stream_coder.contains_key(&input.data[0].name) {
                        return Err(k2err!(K2ErrorCode::AlreadyExists, format!("Coder {} already exists", input.data[0].name)));
                    }
                    self.stream_coder.insert(input.data[0].name.clone(), Box::new(StreamerCoder::new()));
                } else {
                    let split_name: Vec<String> = input.data[0].name.split(".").map(|s| s.to_string()).collect();
                    let stream_name = format!("{}.{}", split_name[0], split_name[1]);
                    self.stream_coder.get_mut(&stream_name)
                        .ok_or(k2err!(K2ErrorCode::NotFound, format!("Coder {} not found", stream_name)))?
                        .execute(input)?;
                }
            }
            "delete" => {
                if input.data[0].object_type == "stream" {
                    self.stream_coder.remove(&input.data[0].name);
                } else {
                    let split_name: Vec<String> = input.data[0].name.split(".").map(|s| s.to_string()).collect();
                    let stream_name = format!("{}.{}", split_name[0], split_name[1]);
                    self.stream_coder.get_mut(&stream_name)
                        .ok_or(k2err!(K2ErrorCode::NotFound, format!("Coder {} not found", stream_name)))?
                        .execute(input)?;
                }
            }
            "add" | "set" | "connect" | "disconnect" => {
                let split_name: Vec<String> = input.data[0].name.split(".").map(|s| s.to_string()).collect();
                let stream_name = format!("{}.{}", split_name[0], split_name[1]);
                self.stream_coder.get_mut(&stream_name)
                    .ok_or(k2err!(K2ErrorCode::NotFound, format!("Coder {} not found", stream_name)))?
                    .execute(input)?;
            }
            _ => {return Err(k2err!(K2ErrorCode::InvalidOperation, format!("Command {} not supported", input.tokens[0])));}
        }
        Ok("Ok".to_string())
    }
    fn generate(&mut self) -> K2CoderReturn {
        Ok("Ok".to_string())
    }
    fn build(&self) -> K2CoderReturn {
        let cargo_if = CARGO_IF.get()
            .ok_or(k2err!(K2ErrorCode::Uninitialized, "Cargo interface not initialized".to_string()))?
            .lock().map_err(|_| k2err!(K2ErrorCode::LockError, "".to_string()))?;
        cargo_if.cargo_build(self.application_path.clone(), "debug".to_string())
            .map_err(|err| k2err!(K2ErrorCode::GenericError, err))?;
        Ok("Ok".to_string())
    }
}