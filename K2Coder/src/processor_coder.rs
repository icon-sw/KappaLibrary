use std::fmt;

use k2_lang::K2ReturnStruct;
use k2_stream::errors::{K2Error, K2ErrorCode};

use crate::coder::CoderTrait;

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

pub struct ProcessCoder {
    name: String,
    path: Option<String>
}

impl CoderTrait for ProcessCoder {
    fn new(name: String) -> Result<Box<dyn CoderTrait>, String> where Self: Sized {
        let instance = Self {
            name,
            path: None
        };
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
    fn proc_new(&mut self, object: &K2ReturnStruct) -> Result<String, String> {
        Ok(object.message.clone())
    }
    fn proc_add(&mut self, _object: &K2ReturnStruct) -> Result<String, String> {
        Err("Add not applicable to processor".to_string())
    }
    fn proc_delete(&mut self, object: &K2ReturnStruct) -> Result<String, String> {
        Ok(object.message.clone())
    }
    fn proc_set(&mut self, object: &K2ReturnStruct) -> Result<String, String> {
        Ok(object.message.clone())
    }
    fn proc_connect(&mut self, _object: &K2ReturnStruct) -> Result<String, String> {
        Err("Connect not applicable to processor".to_string())
    }
    fn proc_disconnect(&mut self, _object: &K2ReturnStruct) -> Result<String, String> {
        Err("Disconnect not applicable to processor".to_string())
    }
    fn proc_exec(&mut self, _object: &K2ReturnStruct) -> Result<String, String> {
        Err("Exec not applicable to processor".to_string())
    }
}