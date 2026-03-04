use std::{collections::HashMap, fmt};

use k2lang::K2LangStruct;
use k2stream::{errors::{K2ErrorCode, K2Error}, k2err};

use crate::coder::{Coder, CoderTrait, K2CoderReturn};
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ProcessorCodePart {
    K2Import,
    UserImport,
    K2ProcessorStruct,
    UserStruct,
    K2InitCode,
    UserInitCode,
    K2MemberCreation,
    UserMemberCreation,
    Initialize,
    Process,
    Finalize,
    UserCode,
}

impl ProcessorCodePart {
    pub fn is_writable(&self) -> bool {
        match self {
            ProcessorCodePart::K2Import => false,
            ProcessorCodePart::K2ProcessorStruct => false,
            ProcessorCodePart::K2MemberCreation => false,
            ProcessorCodePart::K2InitCode => false,
            _ => true,
        }
    }
}
impl TryFrom<u8> for ProcessorCodePart {
    type Error = K2Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0  => Ok(ProcessorCodePart::K2Import),
            1  => Ok(ProcessorCodePart::UserImport),
            2  => Ok(ProcessorCodePart::K2ProcessorStruct),
            3  => Ok(ProcessorCodePart::UserStruct),
            4  => Ok(ProcessorCodePart::K2InitCode),
            5  => Ok(ProcessorCodePart::UserInitCode),
            6  => Ok(ProcessorCodePart::K2MemberCreation),
            7  => Ok(ProcessorCodePart::UserMemberCreation),
            8  => Ok(ProcessorCodePart::Initialize),
            9  => Ok(ProcessorCodePart::Process),
            10 => Ok(ProcessorCodePart::Finalize),
            11  => Ok(ProcessorCodePart::UserCode),
            _  => Err(k2err!(K2ErrorCode::ErrorRange, "Value {} not valid code part"))
        }
    }
}
impl TryFrom<String> for ProcessorCodePart {
    type Error = K2Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "k2_import"             => {Ok(ProcessorCodePart::K2Import)}
            "user_import"           => {Ok(ProcessorCodePart::UserImport)}
            "k2_processor_struct"   => {Ok(ProcessorCodePart::K2ProcessorStruct)}
            "user_struct"           => {Ok(ProcessorCodePart::UserStruct)}
            "k2_init_code"          => {Ok(ProcessorCodePart::K2InitCode)}
            "user_init_code"        => {Ok(ProcessorCodePart::UserInitCode)}
            "k2_member_creation"    => {Ok(ProcessorCodePart::K2MemberCreation)}
            "user_member_creation"  => {Ok(ProcessorCodePart::UserMemberCreation)}
            "initialize"            => {Ok(ProcessorCodePart::Initialize)}
            "process"               => {Ok(ProcessorCodePart::Process)}
            "finalize"              => {Ok(ProcessorCodePart::Finalize)}
            "user_code"             => {Ok(ProcessorCodePart::UserCode)}
            _ => {Err(k2err!(K2ErrorCode::InvalidValue, format!("{} not valid", value)))}
        }
    }
}

impl fmt::Display for ProcessorCodePart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProcessorCodePart::K2Import => write!(f, "k2_import"),
            ProcessorCodePart::UserImport => write!(f, "user_import"),
            ProcessorCodePart::K2ProcessorStruct => write!(f, "k2_processor_struct"),
            ProcessorCodePart::UserStruct => write!(f, "user_struct"),
            ProcessorCodePart::K2InitCode => write!(f, "k2_init_code"),
            ProcessorCodePart::UserInitCode => write!(f, "user_init_code"),
            ProcessorCodePart::K2MemberCreation => write!(f, "k2_member_creation"),
            ProcessorCodePart::UserMemberCreation => write!(f, "user_member_creation"),
            ProcessorCodePart::Initialize => write!(f, "initialize"),
            ProcessorCodePart::Process => write!(f, "process"),
            ProcessorCodePart::Finalize => write!(f, "finalize"),
            ProcessorCodePart::UserCode => write!(f, "user_code"),
        }
    }
}
pub struct ProcessorChild {
    pub name: String,
    pub k2_type: String,
    pub properties: HashMap<String, String>,
}
pub struct ProcessorCoder {
    name: String,
    file_path: String,
    object_map: HashMap<String, ProcessorChild>,
    code_map: HashMap<ProcessorCodePart, String>,
    properties: HashMap<String, String>,
}

impl ProcessorCoder {
    pub fn new(name: String, library_path: String) -> Result<Self, K2Error> {
        let file_name = Coder::to_snake_case(&name);
        let mut self_instance = Self {
            name,
            file_path: format!("{}/src/{}.rs", library_path, file_name),
            object_map: HashMap::new(),
            code_map: HashMap::new(),
            properties: HashMap::new(),
        };
        for code in 0..12 {
            let code = ProcessorCodePart::try_from(code)?;
            self_instance.code_map.insert(code.clone(), self_instance.read_template(&code)?);
        }
        Ok(self_instance)
    }
    pub fn generate_k2_member_creation(&mut self) -> Result<(), K2Error> {
        let mut join_lines: Vec<String> = Vec::new();
        for child in self.object_map.values() {
            let line_head = "    self_instance.get_stream_block_mut()".to_string();
            match child.k2_type.as_str() {
                "input" | "output" => {
                    join_lines.push(format!("{}.add_{}::<{}>(\"{}\");",
                        line_head, 
                        child.k2_type, 
                        child.properties.get("type").ok_or(k2err!(K2ErrorCode::NotFound, format!("Type is mandatory for {}", child.k2_type)))?, 
                        child.name));
                }
                "state" => {
                    join_lines.push(format!("{}.add_{}::<{}>(\"{}\");",
                        line_head, 
                        child.k2_type, 
                        child.properties.get("type").ok_or(k2err!(K2ErrorCode::NotFound, format!("Type is mandatory for {}", child.k2_type)))?, 
                        child.name));
                    join_lines.push(format!("{}.set_state_value::<T>(\"{}\",{})",
                        line_head,
                        child.properties.get("value").ok_or(k2err!(K2ErrorCode::NotFound, format!("Value is mandatory for {}", child.k2_type)))?, 
                        child.name));
                }
                "parameter" => {
                    join_lines.push(format!("{}.add_{}::<{}>(\"{}, ParameterType::{}\");",
                        line_head, 
                        child.k2_type,
                        child.properties.get("type").ok_or(k2err!(K2ErrorCode::NotFound, format!("Type is mandatory for {}", child.k2_type)))?, 
                        child.name, 
                        child.properties.get("kind").ok_or(k2err!(K2ErrorCode::NotFound, format!("Static/Dynamic properties is mandatory for {}", child.k2_type)))?));
                    join_lines.push(format!("{}.set_param_value::<T>(\"{}\",{})",
                        line_head,
                        child.properties.get("value").ok_or(k2err!(K2ErrorCode::NotFound, format!("Value is mandatory for {}", child.k2_type)))?, 
                        child.name));
                }
                "command" => {
                     join_lines.push(format!("{}.add_{}(\"{}, ParameterType::{}\");",
                        line_head, 
                        child.k2_type,
                        child.name,
                        child.properties.get("callback").ok_or(k2err!(K2ErrorCode::NotFound, format!("Static/Dynamic properties is mandatory for {}", child.k2_type)))?));
                }
                _ => {
                }
            }
        }
        self.code_map.insert(ProcessorCodePart::K2MemberCreation, join_lines.join("\n"));
        Ok(())
    }
    pub fn read_template(&self, code: &ProcessorCodePart) -> Result<String, K2Error>{
        let template_name = format!("templates/{}.template", code);
        let result = std::fs::read(template_name).map_err(|err| k2err!(K2ErrorCode::GenericError, format!("{}", err)))?;
        let result = String::from_utf8(result).map_err(|err| k2err!(K2ErrorCode::GenericError, format!("{}", err)))?;
        Ok(result)
    }
}
impl CoderTrait for ProcessorCoder {
    fn execute(&mut self, input: &K2LangStruct) -> K2CoderReturn {
        if input.tokens[0] == "new".to_string() && input.data[0].object_type == "processor".to_string() {    
            return Ok("Ok".to_string());
        }
        match input.tokens[0].as_str() {
            "new" => {
                if self.object_map.contains_key(&input.data[0].name) {
                    return Err(k2err!(K2ErrorCode::AlreadyExists, format!("Object {} already exist", input.data[0].name)));
                }
                let mut new_object = ProcessorChild {
                    name: input.data[0].name.clone(),
                    k2_type: input.data[0].object_type.clone(),
                    properties: HashMap::new(),
                };
                match new_object.k2_type.as_str() {
                    "input" | "output" => {
                        new_object.properties.insert("type".to_string(), input.data[0].properties.get("type").ok_or(k2err!(K2ErrorCode::NotFound, "Type field not found"))?.clone());
                    }
                    "state" => {
                        new_object.properties.insert("type".to_string(), input.data[0].properties.get("type").ok_or(k2err!(K2ErrorCode::NotFound, "Type field not found"))?.clone());
                        new_object.properties.insert("value".to_string(), input.data[0].properties.get("value").ok_or(k2err!(K2ErrorCode::NotFound, "Type field not found"))?.clone());
                    }
                    "parameter" => {
                        new_object.properties.insert("type".to_string(), input.data[0].properties.get("type").ok_or(k2err!(K2ErrorCode::NotFound, "Type field not found"))?.clone());
                        new_object.properties.insert("value".to_string(), input.data[0].properties.get("value").ok_or(k2err!(K2ErrorCode::NotFound, "Type field not found"))?.clone());
                        new_object.properties.insert("kind".to_string(), input.data[0].properties.get("kind").ok_or(k2err!(K2ErrorCode::NotFound, "Type field not found"))?.clone());
                    }
                    "command" => {
                        new_object.properties.insert("callback".to_string(), input.data[0].properties.get("callback").ok_or(k2err!(K2ErrorCode::NotFound, "Type field not found"))?.clone());
                    }
                    _ => {return Err(k2err!(K2ErrorCode::BadFormat, format!("Type {} not exists", new_object.k2_type)));}
                }
                self.object_map.insert(new_object.name.clone(), new_object);
            }
            "delete" => {
                self.object_map.remove(&input.data[0].name);
            }
            "set" => {
                if input.data[0].name == self.name {
                    self.properties.insert(input.tokens[2].clone(), input.tokens[3].clone());
                } else {
                    let child = self.object_map.get_mut(&input.data[0].name)
                        .ok_or(k2err!(K2ErrorCode::NotFound, format!("Object {} does not exist", input.data[0].name)))?;
                    child.properties.insert(input.tokens[2].clone(), input.tokens[3].clone());
                }
            }
            "code" => {
                let code_part = ProcessorCodePart::try_from(input.tokens[2].clone())?;
                if !code_part.is_writable() {
                    return Err(k2err!(K2ErrorCode::NotAllowed, format!("Code part {} is read-only", input.tokens[2])));
                }
                let code = input.tokens[3].clone();
                self.code_map.insert(code_part, code);
            }
            _ => {
                return Err(k2err!(K2ErrorCode::InvalidOperation, format!("Command not supported for library")));
            }
        }
        Ok("Ok".to_string())
    }
    fn generate(&mut self) -> K2CoderReturn {
        self.generate_k2_member_creation()?;
        let mut join_lines: Vec<String> = Vec::new();
        for code in 0..12 {
            let code = ProcessorCodePart::try_from(code)?;
            let mut lines = self.code_map.get(&code).ok_or(k2err!(K2ErrorCode::ProcessError, format!("Missing {} code part", code)))?.clone();
            if code == ProcessorCodePart::K2InitCode {
                let new_lines = lines.replace("@processor", &self.name);
                let new_lines = new_lines.replace("@description", self.properties.get(&"description".to_string()).unwrap_or(&"".to_string()));
                let new_lines = new_lines.replace("@version", self.properties.get(&"version".to_string()).unwrap_or(&"".to_string()));
                let new_lines = new_lines.replace("@author", self.properties.get(&"author".to_string()).unwrap_or(&"".to_string()));
                let new_lines = new_lines.replace("@email", self.properties.get(&"email".to_string()).unwrap_or(&"".to_string()));
                let new_lines = new_lines.replace("@licence", self.properties.get(&"licence".to_string()).unwrap_or(&"LGPLv2.0".to_string()));
                let new_lines = new_lines.replace("@repository", self.properties.get(&"repository".to_string()).unwrap_or(&"".to_string()));
                lines = new_lines.clone();
            }
            join_lines.push(lines.clone());
            if code == ProcessorCodePart::UserStruct {
                join_lines.push("}".to_string());
            }
            if code == ProcessorCodePart::UserInitCode {
                join_lines.push("       }".to_string());
            }
            if code == ProcessorCodePart::UserMemberCreation {
                join_lines.push("   }".to_string());
            }
            if code == ProcessorCodePart::Finalize {
                join_lines.push("}".to_string());
            }
        }
        let full_code = join_lines.join("\n");
        let code_file = self.get_tmp_file();
        self.file_write(code_file.clone(), full_code)?;
        std::fs::rename(&code_file.clone(), &self.file_path)
            .map_err(|e| k2err!(K2ErrorCode::GenericError, format!("Error renaming temp file to {}: {}", self.file_path, e)))?;
        
        Ok("Ok".to_string())
    }
    fn build(&self) -> K2CoderReturn {
        Ok("Ok".to_string())
    }
}