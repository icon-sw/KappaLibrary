use std::collections::HashMap;

use k2lang::{K2LangObject, K2LangStruct};
use k2stream::{errors::{K2ErrorCode, K2Error}, k2err};

use crate::{CARGO_IF, coder::{CoderTrait, K2CoderReturn}, processor_coder::ProcessorCoder};

pub struct LibraryCoder {
    library_path: String,
    file_path: String,
    processor_coder: HashMap<String, Box<dyn CoderTrait>>
}

impl LibraryCoder {
    pub fn new(object: &K2LangObject) -> Result<Self, K2Error> {
        let path = object.properties.get(&"path".to_string())
            .ok_or(k2err!(K2ErrorCode::NotFound, "Path properties not found"))?;
        let library_path = format!("{}/{}", path, object.name.clone());
        let cargo_if = CARGO_IF.get()
            .ok_or(k2err!(K2ErrorCode::Uninitialized, "Cargo interface not initialized".to_string()))?
            .lock().map_err(|_| k2err!(K2ErrorCode::LockError, "".to_string()))?;
        cargo_if.cargo_new_library(library_path.clone()).map_err(|err| k2err!(K2ErrorCode::GenericError, err))?;
        cargo_if.cargo_add_commands(library_path.clone()).map_err(|err| k2err!(K2ErrorCode::GenericError, err))?;
        Ok(Self {
            library_path: library_path.clone(),
            file_path: format!("{}/src/lib.rs", library_path),
            processor_coder: HashMap::new(),
        })
    }
}
impl CoderTrait for LibraryCoder {
    fn execute(&mut self, input: &K2LangStruct) -> K2CoderReturn {
        if input.tokens[0] == "new".to_string() && input.data[0].object_type == "library".to_string() {    
            return Ok("Ok".to_string());
        }
        match input.tokens[0].as_str() {
            "new" => {
                if input.data[0].object_type == "processor" {
                    if self.processor_coder.contains_key(&input.data[0].name) {
                        return Err(k2err!(K2ErrorCode::AlreadyExists, format!("Coder {} already exists", input.data[0].name)));
                    }
                    self.processor_coder.insert(
                        input.data[0].name.clone(), 
                        Box::new(ProcessorCoder::new(input.data[0].name.clone(), self.library_path.clone())?));
                } else {
                    let split_name: Vec<String> = input.data[0].name.split(".").map(|s| s.to_string()).collect();
                    let processor_name = format!("{}.{}", split_name[0], split_name[1]);
                    self.processor_coder.get_mut(&processor_name)
                        .ok_or(k2err!(K2ErrorCode::NotFound, format!("Coder {} not found", processor_name)))?
                        .execute(input)?;
                }
            }
            "delete" => {
                if input.data[0].object_type == "processor" {
                    self.processor_coder.remove(&input.data[0].name);
                } else {
                    let split_name: Vec<String> = input.data[0].name.split(".").map(|s| s.to_string()).collect();
                    let processor_name = format!("{}.{}", split_name[0], split_name[1]);
                    self.processor_coder.get_mut(&processor_name)
                        .ok_or(k2err!(K2ErrorCode::NotFound, format!("Coder {} not found", processor_name)))?
                        .execute(input)?;
                }
            }
            "set" => {
                let split_name: Vec<String> = input.data[0].name.split(".").map(|s| s.to_string()).collect();
                let processor_name = format!("{}.{}", split_name[0], split_name[1]);
                self.processor_coder.get_mut(&processor_name)
                    .ok_or(k2err!(K2ErrorCode::NotFound, format!("Coder {} not found", processor_name)))?
                    .execute(input)?;
            }
            "code" => {
                self.processor_coder.get_mut(&input.data[0].name)
                    .ok_or(k2err!(K2ErrorCode::NotFound, format!("Coder {} not found", input.data[0].name)))?
                    .execute(input)?;
            }
            _ => {
                return Err(k2err!(K2ErrorCode::InvalidOperation, format!("Command not supported for library")));
            }
        }
        Ok("Ok".to_string())
    }
    fn generate(&mut self) -> K2CoderReturn {
        let code_file = self.get_tmp_file();
        let mut code_lines: Vec<String> = Vec::new();
        for proc in self.processor_coder.keys() {
            code_lines.push(format!("pub mod {}", proc));
        }
        let full_code = code_lines.join("\n");
        self.file_write(code_file.clone(), full_code)?;
        std::fs::rename(&code_file.clone(), &self.file_path)
            .map_err(|e| k2err!(K2ErrorCode::GenericError, format!("Error renaming temp file to {}: {}", self.file_path, e)))?;
        Ok("Ok".to_string())
    }
    fn build(&self) -> K2CoderReturn {
        let cargo_if = CARGO_IF.get()
            .ok_or(k2err!(K2ErrorCode::Uninitialized, "Cargo interface not initialized".to_string()))?
            .lock().map_err(|_| k2err!(K2ErrorCode::LockError, "".to_string()))?;
        cargo_if.cargo_build(self.library_path.clone(), "debug".to_string())
            .map_err(|err| k2err!(K2ErrorCode::GenericError, err))?;
        Ok("Ok".to_string())
    }
}