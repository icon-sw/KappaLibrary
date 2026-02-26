use std::{collections::HashMap, fmt, sync::{Arc, Mutex, MutexGuard}};
use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;

use k2_stream::{errors::{K2Error, K2ErrorCode}, k2err, processor::memory::{DataHeader, MemoryTrait}, processor::processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState}};

use crate::K2ReturnStruct;

type CoderReturn = Result<K2ReturnStruct, String>;
type CoderCallback = fn(&mut Coder, &K2ReturnStruct) -> CoderReturn;
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ProcessorCodePart {
    K2Import,
    UserImport,
    UserStruct,
    K2InitCode,
    UserInitCode,
    K2MemberCreation,
    UserMemberCreation,
    InitializeCode,
    ProcessCode,
    FinalizeCode,
    UserCode,
}

impl TryFrom<u8> for ProcessorCodePart {
    type Error = K2Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0  => Err(K2Error { code: K2ErrorCode::NotAllowed, message: format!("Read-only code part value: {}", value) }),
            1  => Ok(ProcessorCodePart::UserImport),
            2  => Ok(ProcessorCodePart::UserStruct),
            3  => Err(K2Error { code: K2ErrorCode::NotAllowed, message: format!("Read-only code part value: {}", value) }),
            4  => Ok(ProcessorCodePart::UserInitCode),
            5  => Err(K2Error { code: K2ErrorCode::NotAllowed, message: format!("Read-only code part value: {}", value) }),
            6  => Ok(ProcessorCodePart::UserMemberCreation),
            7  => Ok(ProcessorCodePart::InitializeCode),
            8  => Ok(ProcessorCodePart::ProcessCode),
            9  => Ok(ProcessorCodePart::FinalizeCode),
            10 => Ok(ProcessorCodePart::UserCode),
            _  => Err(K2Error { code: K2ErrorCode::InvalidValue, message: format!("Unknown code part value: {}", value) }),
        }
    }
}

impl TryFrom<String> for ProcessorCodePart {
    type Error = K2Error;
    fn try_from(value: String) -> Result<Self, K2Error> {
        match value.as_str() {
            "k2_import" => Ok(ProcessorCodePart::K2Import),
            "user_import" => Ok(ProcessorCodePart::UserImport),
            "user_struct" => Ok(ProcessorCodePart::UserStruct),
            "k2_init_code" => Ok(ProcessorCodePart::K2InitCode),
            "user_init_code" => Ok(ProcessorCodePart::UserInitCode),
            "k2_member_creation" => Ok(ProcessorCodePart::K2MemberCreation),
            "user_member_creation" => Ok(ProcessorCodePart::UserMemberCreation),
            "initialize_code" => Ok(ProcessorCodePart::InitializeCode),
            "process_code" => Ok(ProcessorCodePart::ProcessCode),
            "finalize_code" => Ok(ProcessorCodePart::FinalizeCode),
            "user_code" => Ok(ProcessorCodePart::UserCode),
            _ => Err(K2Error { code: K2ErrorCode::InvalidValue, message: format!("Unknown code part: {}", value) }),
        }
    }
}
impl fmt::Display for ProcessorCodePart {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProcessorCodePart::K2Import => write!(f, "k2_import"),
            ProcessorCodePart::UserImport => write!(f, "user_import"),
            ProcessorCodePart::UserStruct => write!(f, "user_struct"),
            ProcessorCodePart::K2InitCode => write!(f, "k2_init_code"),
            ProcessorCodePart::UserInitCode => write!(f, "user_init_code"),
            ProcessorCodePart::K2MemberCreation => write!(f, "k2_member_creation"),
            ProcessorCodePart::UserMemberCreation => write!(f, "user_member_creation"),
            ProcessorCodePart::InitializeCode => write!(f, "initialize_code"),
            ProcessorCodePart::ProcessCode => write!(f, "process_code"),
            ProcessorCodePart::FinalizeCode => write!(f, "finalize_code"),
            ProcessorCodePart::UserCode => write!(f, "user_code"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoderObject {
    pub name: String,
    pub type_name: String,
    pub code_parts: HashMap<ProcessorCodePart, String>,
    pub children: Vec<String>,
    pub parameters: HashMap<String, String>,
}

impl CoderObject {
    pub fn read_code_template(part: &ProcessorCodePart) -> String {
        let template_path = format!("templates/{}.template", part);
        std::fs::read_to_string(&template_path).unwrap_or_else(|_| String::new())
    }
    fn init_processor_code(&mut self) {
        self.code_parts.insert(ProcessorCodePart::K2Import, CoderObject::read_code_template(&ProcessorCodePart::K2Import));
        self.code_parts.insert(ProcessorCodePart::UserImport, CoderObject::read_code_template(&ProcessorCodePart::UserImport));
        self.code_parts.insert(ProcessorCodePart::UserStruct, CoderObject::read_code_template(&ProcessorCodePart::UserStruct));
        self.code_parts.insert(ProcessorCodePart::K2InitCode, CoderObject::read_code_template(&ProcessorCodePart::K2InitCode));
        self.code_parts.insert(ProcessorCodePart::UserInitCode, CoderObject::read_code_template(&ProcessorCodePart::UserInitCode));
        self.code_parts.insert(ProcessorCodePart::K2MemberCreation, CoderObject::read_code_template(&ProcessorCodePart::K2MemberCreation));
        self.code_parts.insert(ProcessorCodePart::UserMemberCreation, CoderObject::read_code_template(&ProcessorCodePart::UserMemberCreation));
        self.code_parts.insert(ProcessorCodePart::InitializeCode, CoderObject::read_code_template(&ProcessorCodePart::InitializeCode));
        self.code_parts.insert(ProcessorCodePart::ProcessCode, CoderObject::read_code_template(&ProcessorCodePart::ProcessCode));
        self.code_parts.insert(ProcessorCodePart::FinalizeCode, CoderObject::read_code_template(&ProcessorCodePart::FinalizeCode));
        self.code_parts.insert(ProcessorCodePart::UserCode, CoderObject::read_code_template(&ProcessorCodePart::UserCode));
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
        let k2_data = command.data.as_ref().ok_or("Missing data for new command")?;
        if k2_data.is_empty() {
            return Err("Data for new command cannot be empty".to_string());
        }
        if k2_data.len() > 1 {
            return Err("Data for new command must contain only one K2Object".to_string());
        }
        let mut coder_object = CoderObject {
            name: data_name.clone(),
            type_name: data_type.clone(),
            code_parts: HashMap::new(),
            children: Vec::new(),
            parameters: k2_data[0].properties.clone(),
        };
        match data_type.as_str() {
            "input" | "output" | "parameter" | "state" | "command" => {
                let split_name: Vec<&str> = data_name.split(".").collect();
                if split_name.len() != 3 {
                    return Err(format!("Processor name must be in the format 'library_name.processor_name'"));
                }
                let processor_name = format!("{}.{}", split_name[0], split_name[1]);
                if !self.coder_objects.contains_key(&processor_name) {
                    return Err(format!("Processor {} does not exist", processor_name));
                }
                let connector_type = data_type.clone();
                let data_properties = command.
                    data.as_ref().ok_or("Missing data for input/output creation")?.
                    get(0).ok_or("Missing data object")?
                    .properties.clone();
                let data_type = data_properties.get("data_type").ok_or("Missing data_type property for input/output creation")?.clone();
                match connector_type.as_str() {
                    "input" | "output" => {
                        coder_object.code_parts.insert(ProcessorCodePart::K2MemberCreation, 
                            format!("ret.get_stream_block_mut().add_{}::<{}>(\"{}\");", connector_type, data_type, data_name));
                
                    }
                    "parameter" => {
                        let value = data_properties.get("value").ok_or("Missing value property for parameter creation")?.clone();
                        let parameter_type = data_properties.get("kind").ok_or("Missing kind property for parameter creation")?.clone();
                        match parameter_type.as_str() {
                            "static" | "dynamic" => {}
                            _ => return Err("Parameter kind must be either static or dynamic".to_string()),
                        }
                        let declaration = format!("ret.get_stream_block_mut().add_parameter::<{}>(\"{}\", {});", data_type, data_name, parameter_type);
                        let initialization = format!("ret.get_stream_block_mut().set_parameter::<{}>(\"{}\", {});", data_type, data_name, value);
                        coder_object.code_parts.insert(ProcessorCodePart::K2MemberCreation, 
                            format!("{}\n{}", declaration, initialization));
                    }
                    "state" => {
                        let value = data_properties.get("value").ok_or("Missing value property for state creation")?.clone();
                        let declaration = format!("ret.get_stream_block_mut().add_state::<{}>(\"{}\");", data_type, data_name);
                        let initialization = format!("ret.get_stream_block_mut().set_state::<{}>(\"{}\", {});", data_type, data_name, value);
                        coder_object.code_parts.insert(ProcessorCodePart::K2MemberCreation, 
                            format!("{}\n{}", declaration, initialization));
                    }   
                    "command" => {
                    }
                    _ => {}
                }
                self.coder_objects.insert(data_name, coder_object);
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
    fn new(name: String) -> ProcessorNewReturn {
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
        Ok(Box::new(coder))
    }
    fn initialize(&mut self ) -> Result<(), K2Error> {
        Ok(())
    }
    fn process(&mut self) -> Result<(), K2Error> {
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
    fn finalize(&mut self) -> Result<(), K2Error> {
        *self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to update status"))? = StreamState::Waiting;
        Ok(())
    }
}

#[cfg(test)]
mod test 
{
    use super::*;
    #[test]
    fn test() {
        
    }
}