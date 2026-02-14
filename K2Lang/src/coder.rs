use std::{collections::HashMap, fmt, sync::{Arc, Mutex, MutexGuard}};
use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;

use k2_stream::{memory::{DataHeader, MemoryTrait}, processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorTrait, StreamBlock, StreamState}};

use crate::K2ReturnStruct;

type CoderReturn = Result<K2ReturnStruct, String>;
type CoderCallback = fn(&mut Coder, &K2ReturnStruct) -> CoderReturn;
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ProcessorCoderParts {
    HeadMod,
    UsedDefinedCode,
    HeadStruct,
    UserDefinedStruct,
    EndStruct,
    HeadBuilder,
    UserDefinedBuilder,
    UserMemberCreation,
    UserDefinedImplStruct,
    InitBody,
    ProcessBody,
    FinalizeBody,
}

impl TryFrom<u8> for ProcessorCoderParts {
    type Error = ();
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ProcessorCoderParts::HeadMod),
            1 => Ok(ProcessorCoderParts::UsedDefinedCode),
            2 => Ok(ProcessorCoderParts::HeadStruct),
            3 => Ok(ProcessorCoderParts::UserDefinedStruct),
            4 => Ok(ProcessorCoderParts::EndStruct),
            5 => Ok(ProcessorCoderParts::HeadBuilder),
            6 => Ok(ProcessorCoderParts::UserDefinedBuilder),
            7 => Ok(ProcessorCoderParts::UserMemberCreation),
            8 => Ok(ProcessorCoderParts::UserDefinedImplStruct),
            9 => Ok(ProcessorCoderParts::InitBody),
            10 => Ok(ProcessorCoderParts::ProcessBody),
            11 => Ok(ProcessorCoderParts::FinalizeBody),
            _ => Err(()),
        }
    }
}
impl TryFrom<String> for ProcessorCoderParts {
    type Error = ();
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "header_processor" => Ok(ProcessorCoderParts::HeadMod),
            "user_defined_code" => Ok(ProcessorCoderParts::UsedDefinedCode),
            "processor_struct_header" => Ok(ProcessorCoderParts::HeadStruct),
            "user_defined_struct" => Ok(ProcessorCoderParts::UserDefinedStruct),
            "end_struct" => Ok(ProcessorCoderParts::EndStruct),
            "processor_builder_header" => Ok(ProcessorCoderParts::HeadBuilder),
            "user_defined_builder" => Ok(ProcessorCoderParts::UserDefinedBuilder),
            "user_member_creation" => Ok(ProcessorCoderParts::UserMemberCreation),
            "user_defined_impl_struct" => Ok(ProcessorCoderParts::UserDefinedImplStruct),
            "init_body" => Ok(ProcessorCoderParts::InitBody),
            "process_body" => Ok(ProcessorCoderParts::ProcessBody),
            "finalize_body" => Ok(ProcessorCoderParts::FinalizeBody),
            _ => Err(()),
        }
    }
}
impl fmt::Display for ProcessorCoderParts {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProcessorCoderParts::HeadMod => write!(f, "header_processor"),
            ProcessorCoderParts::UsedDefinedCode => write!(f, "user_defined_code"),
            ProcessorCoderParts::HeadStruct => write!(f, "processor_struct_header"),
            ProcessorCoderParts::UserDefinedStruct => write!(f, "user_defined_struct"),
            ProcessorCoderParts::EndStruct => write!(f, "end_struct"),
            ProcessorCoderParts::HeadBuilder => write!(f, "processor_builder_header"),
            ProcessorCoderParts::UserDefinedBuilder => write!(f, "user_defined_builder"),
            ProcessorCoderParts::UserMemberCreation => write!(f, "user_member_creation"),
            ProcessorCoderParts::UserDefinedImplStruct => write!(f, "user_defined_impl_struct"),
            ProcessorCoderParts::InitBody => write!(f, "init_body"),
            ProcessorCoderParts::ProcessBody => write!(f, "process_body"),
            ProcessorCoderParts::FinalizeBody => write!(f, "finalize_body"),
        }
    }
}
#[derive(Debug, Clone)]
pub struct CoderObject {
    pub name: String,
    pub type_name: String,
    pub code_parts: HashMap<ProcessorCoderParts, String>,
    pub children: Vec<String>,
    pub parameters: HashMap<String, String>,
}

impl CoderObject {
    pub fn read_code_template(part: &ProcessorCoderParts) -> String {
        let template_path = format!("templates/{}.template", part);
        std::fs::read_to_string(&template_path).unwrap_or_else(|_| String::new())
    }
    fn init_processor_code(&mut self) {
        self.code_parts.insert(ProcessorCoderParts::HeadMod, CoderObject::read_code_template(&ProcessorCoderParts::HeadMod));
        self.code_parts.insert(ProcessorCoderParts::UsedDefinedCode, CoderObject::read_code_template(&ProcessorCoderParts::UsedDefinedCode));
        self.code_parts.insert(ProcessorCoderParts::HeadStruct, CoderObject::read_code_template(&ProcessorCoderParts::HeadStruct));
        self.code_parts.insert(ProcessorCoderParts::UserDefinedStruct, CoderObject::read_code_template(&ProcessorCoderParts::UserDefinedStruct));
        self.code_parts.insert(ProcessorCoderParts::EndStruct, CoderObject::read_code_template(&ProcessorCoderParts::EndStruct));
        self.code_parts.insert(ProcessorCoderParts::HeadBuilder, CoderObject::read_code_template(&ProcessorCoderParts::HeadBuilder));
        self.code_parts.insert(ProcessorCoderParts::UserDefinedBuilder, CoderObject::read_code_template(&ProcessorCoderParts::UserDefinedBuilder));
        self.code_parts.insert(ProcessorCoderParts::UserMemberCreation, CoderObject::read_code_template(&ProcessorCoderParts::UserMemberCreation));
        self.code_parts.insert(ProcessorCoderParts::UserDefinedImplStruct, CoderObject::read_code_template(&ProcessorCoderParts::UserDefinedImplStruct));
        self.code_parts.insert(ProcessorCoderParts::InitBody, CoderObject::read_code_template(&ProcessorCoderParts::InitBody));
        self.code_parts.insert(ProcessorCoderParts::ProcessBody, CoderObject::read_code_template(&ProcessorCoderParts::ProcessBody));
        self.code_parts.insert(ProcessorCoderParts::FinalizeBody, CoderObject::read_code_template(&ProcessorCoderParts::FinalizeBody));
    }
}
#[derive(K2Memory, K2ProcessorBlock)]
pub struct Coder {
    pub name: String,
    pub header: ProcessorHeader,
    stream_block: StreamBlock,
    state: Arc<Mutex<StreamState>>,
    cargo_path: String,
    coder_objects: HashMap<String, CoderObject>,
    callbacks_cmd: HashMap<String, CoderCallback>,
}

impl Coder {
    pub fn new(name: String) -> Result<Self, ()> {
        let mut callbacks_cmd: HashMap<String, CoderCallback> = HashMap::new();
        callbacks_cmd.insert("new".to_string(), Coder::proc_new);
        callbacks_cmd.insert("add".to_string(), Coder::proc_add);
        callbacks_cmd.insert("delete".to_string(), Coder::proc_delete);
        callbacks_cmd.insert("set".to_string(), Coder::proc_set);
        callbacks_cmd.insert("connect".to_string(), Coder::proc_connect);
        callbacks_cmd.insert("disconnect".to_string(), Coder::proc_disconnect);
        callbacks_cmd.insert("exec".to_string(), Coder::proc_exec);
        let cargo_path;
        if let Some(path) = std::env::var_os("HOME") {
            cargo_path = format!("{}/.cargo/bin/cargo", path.into_string().unwrap());
        } else {
            cargo_path = "cargo".to_string();
        }
        let mut coder = Self {
            name: name.clone(),
            header: ProcessorHeader {
                proc_name: "Coder".to_string(),
                description: "A processor that parses the commands and executes the corresponding actions".to_string(),
                version: "0.1.0".to_string(),
                author: "Sofia Silvestri".to_string(),
                email: "ms.sofia.silvestri@gmail.com".to_string(),
                license: "LGPLv2.0".to_string(),
                repository: "".to_string(),
            },
            stream_block: StreamBlock::new(),
            state: Arc::new(Mutex::new(StreamState::Initialized)),
            cargo_path,
            coder_objects: HashMap::new(),
            callbacks_cmd,
        };
        coder.stream_block.add_input::<K2ReturnStruct>("command".to_string())?;
        coder.stream_block.add_output::<K2ReturnStruct>("response".to_string())?;
        Ok(coder)
    }
    
    fn generate(&self) -> CoderReturn {
        // Placeholder for code generation logic
        Ok(K2ReturnStruct {
            success: true,
            command: "generate".to_string(),
            tokens: vec![],
            message: "Code generated successfully".to_string(),
            data: None,
        })

    }
    fn proc_new(&mut self, command: &K2ReturnStruct) -> CoderReturn {
        let mut response = command.clone();
        if command.tokens.len() < 3 {
            return Err(format!("New command requires at least 3 tokens: 'new', type, and name"));
        }
        let data_type = command.tokens[1].clone();
        let data_name = command.tokens[2].clone();
        if self.coder_objects.contains_key(&data_name) {
            return Err(format!("An object with the name {} already exists", data_name));
        }
        match data_type.as_str() {
            "input" | "output" => {
                Ok(response)
            }
            "parameter" => {
                Ok(response)
            }
            "state" => {
                Ok(response)
            }
            "command" => {
                Ok(response)
            }
            "processor" => {
                let split_name: Vec<&str> = data_name.split(".").collect();
                if split_name.len() != 2 {
                    return Err(format!("Processor name must be in the format 'library_name.processor_name'"));
                }
                let library_name = split_name[0].to_string();
                let processor_path: String;
                match self.coder_objects.get_mut(&library_name) {
                    None => {
                        return Err(format!("Library {} does not exist", library_name));
                    }
                    Some(coder_library) => {
                        let library_path = coder_library.parameters.get("path").ok_or("Missing library Path")?.clone();
                        processor_path = format!("{}/src/{}.rs", library_path, data_name);
                        if !std::path::Path::new(&processor_path).exists() {
                            std::fs::write(&processor_path, "").map_err(|e| format!("Failed to create processor file: {}", e))?;
                        }
                        if !coder_library.children.contains(&data_name) {
                            coder_library.children.push(data_name.clone());
                        }
                    }
                }
                let mut parameters = HashMap::new();
                parameters.insert("path".to_string(), processor_path.clone());
                let mut coder_object = CoderObject {
                    name: data_name.clone(),
                    type_name: data_type.clone(),
                    code_parts: HashMap::new(),
                    children: Vec::new(),
                    parameters,
                };
                coder_object.init_processor_code();
                self.coder_objects.insert(data_name.clone(), coder_object);
                Ok(response)
            }
            "library" => {
                let library_name = command.tokens[2].clone();
                let command_data = command.data.as_ref();
                if command_data.is_none() || command_data.unwrap().is_empty() {
                    return Err(format!("Library creation requires data with at least one K2Object representing the library"));
                }
                let library_object = &command_data.unwrap()[0];
                if library_object.object_type != "library" {
                    return Err(format!("The K2Object representing the library must have object_type 'library'"));
                }
                let library_path_param = library_object.properties.get("path");
                if library_path_param.is_none() {
                    return Err(format!("The K2Object representing the library must have a 'path' property"));
                }
                let library_path = library_path_param.unwrap();
                std::fs::create_dir_all(&library_path).map_err(|e| format!("Failed to create library path: {}", e))?;
                // Run cargo new to create the library
                let output = std::process::Command::new(&self.cargo_path)
                    .arg("new")
                    .arg("--lib")
                    .arg(&library_name)
                    .output();
                match output {
                    Ok(output) => {
                        response.success = output.status.success();
                        if response.success {
                            response.message = String::from_utf8_lossy(&output.stdout).to_string();
                        } else {
                            response.message = String::from_utf8_lossy(&output.stderr).to_string();
                        }
                    }
                    Err(e) => {
                        return Err(format!("Failed to execute cargo new: {}", e));
                    }
                };
                let mut parameters = HashMap::new();
                parameters.insert("path".to_string(), format!("{}/{}", library_path, library_name.clone()));
                self.coder_objects.insert(library_name.clone(), CoderObject {
                    name: library_name.clone(),
                    type_name: data_type.clone(),
                    code_parts: HashMap::new(),
                    children: Vec::new(),
                    parameters,
                });
                Ok(response)
            }
            "stream" => {
                Ok(response)
            }
            "chain" => {
                Ok(response)
            }
            "mode" => {
                Ok(response)
            }
            "application" => {
                Ok(response)
            }

            _ => {
                // Return an error for unknown subcommands
                Err(format!("Unknown type {}", command.tokens[2]))
            }
        }
    }
    fn proc_add(&mut self, command: &K2ReturnStruct) -> CoderReturn {
        Ok(command.clone())
    }
    fn proc_delete(&mut self, command: &K2ReturnStruct) -> CoderReturn {
        Ok(command.clone())
    }
    fn proc_set(&mut self, command: &K2ReturnStruct) -> CoderReturn {
        Ok(command.clone())
    }
    fn proc_connect(&mut self, command: &K2ReturnStruct) -> CoderReturn {
        Ok(command.clone())
    }
    fn proc_disconnect(&mut self, command: &K2ReturnStruct) -> CoderReturn {
        Ok(command.clone())
    }
    fn proc_exec(&mut self, command: &K2ReturnStruct) -> CoderReturn {
        Ok(command.clone())
    }
    fn execute(&mut self, command: &K2ReturnStruct) -> K2ReturnStruct {
       let mut response = command.clone();
        if !command.success {
            return response;
        }
        if let Some(callback) = self.callbacks_cmd.get(command.tokens[0].clone().as_str()) {
            let result = callback(self, &command);
            match result {
                Ok(res) => {
                    match self.generate() {
                        Ok(_) => {
                            response = res;
                        }
                        Err(e) => {
                            response = K2ReturnStruct {
                                success: false,
                                command: command.command.clone(),
                                tokens: command.tokens.clone(),
                                message: format!("Code generation failed: {}", e),
                                data: None,
                            };
                        }
                    }
                }
                Err(_) => {
                    response = K2ReturnStruct {
                        success: false,
                        command: command.command.clone(),
                        tokens: command.tokens.clone(),
                        message: format!("Error"),
                        data: None,
                    };
                }
            }
        } else {
            response = K2ReturnStruct {
                success: false,
                command: command.command.clone(),
                tokens: command.tokens.clone(),
                message: format!("Unknown"),
                data: None,
            };
        }
        response
    }
}

impl ProcessorTrait for Coder {
    fn initialize(&mut self ) -> Result<(), ()> {
        Ok(())
    }
    fn process(&mut self) -> Result<(), ()> {
        let command_input = self.stream_block.get_input::<K2ReturnStruct>(&"command".to_string())?;
        let command = command_input.receive()?;
        let mut response = command.clone();
        if command.success {
            response = self.execute(&command);
        }
        let response_output = self.stream_block.get_output::<K2ReturnStruct>(&"response".to_string())?;
        response_output.send(response)?;
        Ok(())
    }
    fn finalize(&mut self) -> Result<(), ()> {
        *self.state.lock().map_err(|_| ())? = StreamState::Waiting;
        Ok(())
    }
}