use std::{collections::HashMap, fmt::{self}};

use k2_lang::{K2Object, K2ReturnStruct};
use k2_stream::errors::{K2Error, K2ErrorCode};

use crate::coder::{Coder, CoderTrait};

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
            0  => Ok(ProcessorCodePart::K2Import),
            1  => Ok(ProcessorCodePart::UserImport),
            2  => Ok(ProcessorCodePart::UserStruct),
            3  => Ok(ProcessorCodePart::K2InitCode),
            4  => Ok(ProcessorCodePart::UserInitCode),
            5  => Ok(ProcessorCodePart::K2MemberCreation),
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
            "k2_import" => Err(K2Error { code: K2ErrorCode::NotAllowed, message: format!("Read-only code part: {}", value) }),
            "user_import" => Ok(ProcessorCodePart::UserImport),
            "user_struct" => Ok(ProcessorCodePart::UserStruct),
            "k2_init_code" => Err(K2Error { code: K2ErrorCode::NotAllowed, message: format!("Read-only code part value: {}", value) }),
            "user_init_code" => Ok(ProcessorCodePart::UserInitCode),
            "k2_member_creation" => Err(K2Error { code: K2ErrorCode::NotAllowed, message: format!("Read-only code part value: {}", value) }),
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

pub struct ProcessCoder {
    name: String,
    path: Option<String>,
    children: HashMap<String, K2Object>,
    code_parts: HashMap<ProcessorCodePart, String>,
}

impl CoderTrait for ProcessCoder {
    fn new(name: String, object: &K2Object) -> Result<Box<dyn CoderTrait>, String> where Self: Sized {
        if object.object_type != "processor".to_string() {
            return Err(format!("Object {} is not a processor", name.clone()));
        }
        let mut instance = Self {
            name,
            path: None,
            children: HashMap::new(),
            code_parts: HashMap::new(),
        };
        instance.init_processor_code();
        Ok(Box::new(instance))
    }
    fn get_name(&self) -> String {
        self.name.clone()
    }
    fn get_path(&self) -> String {
        self.path.clone().unwrap_or_default()
    }
    fn set_parent_directory(&mut self, path: String) {
        self.path = Some(format!("{}/{}.rs", path, self.name));
    }
    fn proc_new(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String> {
        let object = k2_struct.data.get(0).ok_or("Missing data")?;
        let object_name = object.name.clone();
        if self.children.contains_key(&object_name.clone()) {
            return Err(format!("Object {} already exists", object_name));
        }
        self.children.insert(object_name.clone(), object.clone());
        Ok(format!("Object {} created", object_name.clone()))
    }
    fn proc_add(&mut self, _k2_struct: &K2ReturnStruct) -> Result<String, String> {
        Err("Add not applicable to processor".to_string())
    }
    fn proc_delete(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String> {
        let object = k2_struct.data.get(0).ok_or("Missing data")?;
        if object.name == self.name {
            // Remove file
            let path = self.path.clone().ok_or("Path not set")?;
            std::fs::remove_file(path).map_err(|_| "Error in deleting object file")?;
            Ok(format!("Processor {} deleted with success", object.name))
        } else {
            if self.children.contains_key(&object.name.clone()) {
                self.children.remove(&object.name.clone());
                Ok(format!("Object {} deleted with success", object.name.clone()))
            } else {
                Err(format!("Object {} not found", object.name.clone()))
            }
        }
    }
    fn proc_set(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String> {
        let object = k2_struct.data.get(0).ok_or("Missing data")?;
        if object.name == self.name {
            let mut code_found = false;
            for (key, value) in object.properties.iter() {
                let code_part = ProcessorCodePart::try_from(key.clone()).map_err(|e| e.message);
                if code_part.is_ok() {
                    self.code_parts.insert(code_part.unwrap(), value.clone());
                    code_found = true;
                    break;
                }
            }
            if !code_found {
                return Err(format!("Invalid set on processor {}", self.name));
            }
        } else {
            if !self.children.contains_key(&object.name.clone()) {
                return Err(format!("Object {} does not exist", object.name.clone()));
            }
            if !object.properties.contains_key(&"value".to_string()) {
                return Err(format!("Missing value"));
            }
            let children = self.children.get_mut(&object.name.clone()).unwrap();
            children.properties.insert("value".to_string(), object.properties.get(&"value".to_string()).unwrap().clone());
        }
        Ok(format!("Set value for {}", object.name.clone()))
    }
    fn proc_connect(&mut self, _k2_struct: &K2ReturnStruct) -> Result<String, String> {
        Err("Connect not applicable to processor".to_string())
    }
    fn proc_disconnect(&mut self, _k2_struct: &K2ReturnStruct) -> Result<String, String> {
        Err("Disconnect not applicable to processor".to_string())
    }
    fn proc_exec(&mut self, _k2_struct: &K2ReturnStruct) -> Result<String, String> {
        Err("Exec not applicable to processor".to_string())
    }
    fn generate(&mut self) -> Result<String, String> {
        let code_file = Coder::get_tmp_file();
        self.generate_k2_member_creation()?;
        let mut code_lines: Vec<String> = Vec::new();
        for index in 0..=10 {
            let code_part = ProcessorCodePart::try_from(index).map_err(|e| e.message)?;
            match code_part {
                ProcessorCodePart::K2InitCode => {
                    code_lines.push(format!("impl ProcessorTrait for {} {{", self.get_name()));
                    code_lines.push(format!("    fn new(name: String) -> ProcessorNewReturn {{"));
                    code_lines.push(format!("        let mut self_instance = Self {{"))
                }
                ProcessorCodePart::InitializeCode => {
                    code_lines.push(format!("        }};"));
                    code_lines.push(format!("        Ok(Box::new(self_instance))"));
                    code_lines.push(format!("    }}"));
                }
                ProcessorCodePart::UserCode => {
                    code_lines.push(format!("}}"));
                }
                _ => {},
            }
            code_lines.push(self.code_parts.get(&code_part).unwrap().clone());
        }
        let full_code = code_lines.join("\n");
        Coder::file_write(code_file.clone(), full_code)?;
        Coder::file_move(&code_file, &self.get_path())?;
        Ok(format!("Processor {} code generate with success", self.name.clone()))
    }
    fn build(&self) -> Result<String, String> {
        Ok("".to_string())
    }
}

impl ProcessCoder {
    pub fn generate_k2_member_creation(&mut self) -> Result<(), String>{
        let mut input_creation: Vec<String> = Vec::new();
        let mut output_creation: Vec<String> = Vec::new();
        let mut parameter_creation: Vec<String> = Vec::new();
        let mut state_creation: Vec<String> = Vec::new();
        let mut command_creation: Vec<String> = Vec::new();
        for (object_name, object) in self.children.iter() {
            match object.object_type.clone().as_str() {
                "input" => {
                    let input_type = object.properties.get("type").ok_or("Missing type".to_string())?;
                    input_creation.push(format!("self_instance.get_stream_block_mut().add_input::<{}>(\"{}\".to_string())?;", input_type, object_name));
                }
                "output" => {
                    let output_type = object.properties.get("type").ok_or("Missing type".to_string())?;
                    output_creation.push(format!("self_instance.get_stream_block_mut().add_output::<{}>(\"{}\".to_string())?;", output_type, object_name));
                }
                "parameter" => {
                    let parameter_type = object.properties.get("type").ok_or("Missing type".to_string())?;
                    let parameter_value = object.properties.get("value").ok_or("Missing value".to_string())?;
                    let parameter_kind = object.properties.get("kind").ok_or("Missing kind".to_string())?.to_uppercase();
                    parameter_creation.push(format!("self_instance.get_stream_block_mut().add_parameter::<{}>(\"{}\".to_string(), ParameterType::{})?;", parameter_type, object_name, parameter_kind));
                    parameter_creation.push(format!("self_instance.get_stream_block_mut().set_param_value::<{}>(\"{}\", {}", parameter_type, object_name, parameter_value));
                }
                "state" => {
                    let state_type = object.properties.get("type").ok_or("Missing type".to_string())?;
                    let state_value = object.properties.get("value").ok_or("Missing value".to_string())?;
                    state_creation.push(format!("self_instance.get_stream_block_mut().add_state::<{}>(\"{}\".to_string())?;", state_type, object_name));
                    state_creation.push(format!("self_instance.get_stream_block_mut().set_state_value::<{}>(\"{}\", {}", state_type, object_name, state_value));
                }
                "command" => {
                    let callback = object.properties.get("callback").ok_or("Missing callback".to_string())?;
                    command_creation.push(format!("self_instance.get_stream_block_mut().add_command(\"{}\".to_string(), {});?", object_name, callback));
                }
                _ => {}
            }
        }
        let mut code_block = Vec::new();
        code_block.push(input_creation.join("\n"));
        code_block.push(output_creation.join("\n"));
        code_block.push(parameter_creation.join("\n"));
        code_block.push(state_creation.join("\n"));
        code_block.push(command_creation.join("\n"));
        self.code_parts.insert(ProcessorCodePart::K2MemberCreation, code_block.join("\n"));
        Ok(())
    }
    pub fn read_code_template(part: &ProcessorCodePart) -> String {
        let template_path = format!("templates/{}.template", part);
        std::fs::read_to_string(&template_path).unwrap_or_else(|_| String::new())
    }
    fn init_processor_code(&mut self) {
        self.code_parts.insert(ProcessorCodePart::K2Import, ProcessCoder::read_code_template(&ProcessorCodePart::K2Import));
        self.code_parts.insert(ProcessorCodePart::UserImport, ProcessCoder::read_code_template(&ProcessorCodePart::UserImport));
        self.code_parts.insert(ProcessorCodePart::UserStruct, ProcessCoder::read_code_template(&ProcessorCodePart::UserStruct));
        self.code_parts.insert(ProcessorCodePart::K2InitCode, ProcessCoder::read_code_template(&ProcessorCodePart::K2InitCode));
        self.code_parts.insert(ProcessorCodePart::UserInitCode, ProcessCoder::read_code_template(&ProcessorCodePart::UserInitCode));
        self.code_parts.insert(ProcessorCodePart::K2MemberCreation, ProcessCoder::read_code_template(&ProcessorCodePart::K2MemberCreation));
        self.code_parts.insert(ProcessorCodePart::UserMemberCreation, ProcessCoder::read_code_template(&ProcessorCodePart::UserMemberCreation));
        self.code_parts.insert(ProcessorCodePart::InitializeCode, ProcessCoder::read_code_template(&ProcessorCodePart::InitializeCode));
        self.code_parts.insert(ProcessorCodePart::ProcessCode, ProcessCoder::read_code_template(&ProcessorCodePart::ProcessCode));
        self.code_parts.insert(ProcessorCodePart::FinalizeCode, ProcessCoder::read_code_template(&ProcessorCodePart::FinalizeCode));
        self.code_parts.insert(ProcessorCodePart::UserCode, ProcessCoder::read_code_template(&ProcessorCodePart::UserCode));
    }
}