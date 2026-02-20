use std::{collections::HashMap, sync::{Arc, Mutex, MutexGuard}};

use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;
use k2_stream::{errors::{K2Error, K2ErrorCode}, k2err, memory::{DataHeader, MemoryTrait}, processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState}};
use crate::{K2Object, K2ReturnStruct, coder::ProcessorCodePart};

type AstReturn = Result<K2ReturnStruct, String>;
type AstCallback = fn(&mut AstProcessor, &K2ReturnStruct) -> AstReturn;

#[derive(K2Memory, K2ProcessorBlock)]
pub struct AstProcessor {
    // fields for the parser
    pub name: String,
    pub header: ProcessorHeader,
    stream_block: StreamBlock,
    state: Arc<Mutex<StreamState>>,
    objects: HashMap<String, K2Object>,
    callbacks_cmd: HashMap<String, AstCallback>,
}

impl AstProcessor {
    pub fn parse_new(&mut self, k2_parse_struct: &K2ReturnStruct) -> AstReturn {

        let object_type = &k2_parse_struct.tokens[1];
        let object_name = &k2_parse_struct.tokens[2];
        
        if self.objects.get(object_name).is_some() {
            return Err("Object with the same name already exists".to_string());
        }
        let mut processing_object = K2Object {
            name: object_name.to_string(),
            object_type: object_type.to_string(),
            parent: Vec::new(),
            children: Vec::new(),
            properties: HashMap::new(),
        };
        match object_type.as_str() {
            "input" | "output" => {
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 3 {
                    return Err("Invalid object name format for input/output".to_string());
                }
                let parent_name = format!("{}.{}", split_name[0], split_name[1]);
                match self.objects.get_mut(&parent_name) {
                    Some(parent_object) => {
                        if parent_object.object_type != "processor" {
                            return Err("Parent object must be a processor".to_string());
                        }
                        parent_object.children.push(object_name.to_string());
                    },
                    None => {
                        return Err("Parent object does not exist".to_string());
                    }
                }
                processing_object.parent.push(parent_name);
                if k2_parse_struct.tokens.len() < 4 {
                    return Err("Invalid k2_parse_struct.tokens length for input/output".to_string());
                }
                let data_type = &k2_parse_struct.tokens[3];
                if data_type.is_empty() {
                    return Err("Data type cannot be empty".to_string());
                }
                processing_object.properties.insert("type".to_string(), data_type.to_string());
            },
            "parameter" | "state" => {
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 3 {
                    return Err("Invalid object name format for parameter/state".to_string());
                }
                let parent_name = format!("{}.{}", split_name[0], split_name[1]);
                match self.objects.get_mut(&parent_name) {
                    Some(parent_object) => {
                        if parent_object.object_type != "processor" {
                            return Err("Parent object must be a processor".to_string());
                        }
                        parent_object.children.push(object_name.to_string());
                    },
                    None => {
                        return Err("Parent object does not exist".to_string());
                    }
                }
                processing_object.parent.push(parent_name);
                if k2_parse_struct.tokens.len() < 5 {
                    return Err("Invalid k2_parse_struct.tokens length for parameter/state".to_string());
                }
                let data_type = &k2_parse_struct.tokens[3];
                if data_type.is_empty() {
                    return Err("Data type cannot be empty".to_string());
                }
                let value = &k2_parse_struct.tokens[4];
                if value.is_empty() {
                    return Err("Value cannot be empty".to_string());
                }
                processing_object.properties.insert("type".to_string(), data_type.to_string());
                processing_object.properties.insert("value".to_string(), value.to_string());
                if object_type == "parameter" {
                    let mut parameter_kind = "dynamic".to_string();
                    if k2_parse_struct.tokens.len() == 6 {
                        parameter_kind = k2_parse_struct.tokens[5].to_string();
                    }
                    if parameter_kind != "static" && parameter_kind != "dynamic" {
                        return Err("Parameter kind must be either static or dynamic".to_string());
                    }
                    processing_object.properties.insert("kind".to_string(), parameter_kind.to_string());
                }
            }
            "processor" => {
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 2 {
                    return Err("Invalid object name format for processor".to_string());
                }
                match self.objects.get_mut(&split_name[0]) {
                    Some(parent_object) => {
                        if parent_object.object_type != "library" {
                            return Err("Parent object must be a library".to_string());
                        }
                        parent_object.children.push(object_name.to_string());
                    },
                    None => {
                        return Err("Parent object does not exist".to_string());
                    }
                }
                processing_object.parent.push(split_name[0].to_string());
            }
            "library" | "application" => {
                if k2_parse_struct.tokens.len() < 3 {
                    return Err("Invalid k2_parse_struct.tokens length for library/application".to_string());
                }
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 1 {
                    return Err("Invalid object name format".to_string());
                }
                if self.objects.get(&split_name[0]).is_some() {
                    return Err("Object with the same name already exists".to_string());
                }
                processing_object.properties.insert("path".to_string(), k2_parse_struct.tokens[2].to_string());
            }
            "chain" | "mode" => {
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 2 {
                    return Err("Invalid object name format for chain/mode".to_string());
                }
                match self.objects.get_mut(&split_name[0]) {
                    Some(parent_object) => {
                        if parent_object.object_type != "application" {
                            return Err("Parent object must be an application".to_string());
                        }
                        parent_object.children.push(object_name.to_string());
                    },
                    None => {
                        return Err("Parent object does not exist".to_string());
                    }
                }
                processing_object.parent.push(split_name[0].to_string());
            }
            "stream" => {
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 2 {
                    return Err("Invalid object name format for stream".to_string());
                }
                if k2_parse_struct.tokens.len() < 4 {
                    return Err("Invalid k2_parse_struct.tokens length for stream".to_string());
                }
                if self.objects.get(&split_name[0]).is_none() {
                    return Err("Parent object does not exist".to_string());
                }
                match self.objects.get_mut(&split_name[0]) {
                    Some(parent_object) => {
                        if parent_object.object_type != "application" {
                            return Err("Parent object must be an application".to_string());
                        }

                        parent_object.children.push(object_name.to_string());
                    },
                    None => {
                        return Err("Parent object does not exist".to_string());
                    }
                }
                processing_object.parent.push(split_name[0].to_string());
                let processor_type = &k2_parse_struct.tokens[3];
                if processor_type.is_empty() {
                    return Err("Processor cannot be empty".to_string());
                }
                processing_object.properties.insert("type".to_string(), processor_type.to_string());
                processing_object.properties.insert("connection".to_string(), "0".to_string());
            }
            "command" => {
                if !object_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    return Err("Invalid object name".to_string());
                }
            },
            _ => return Err("Invalid object type".to_string()),
        }
        self.objects.insert(object_name.to_string(), processing_object.clone());
        // Here you would implement the logic to parse the k2_parse_struct.tokens and return the appropriate response
        Ok(K2ReturnStruct {
            success: true,
            command: k2_parse_struct.command.clone(),
            tokens: k2_parse_struct.tokens.clone(),
            message: "Parsed".to_string(),
            data: Some(vec![processing_object]),
        })
    }
    pub fn parse_add(&mut self, k2_parse_struct: &K2ReturnStruct) -> AstReturn {
        // parse the add k2_parse_struct.tokens and execute the corresponding action
        if k2_parse_struct.tokens.len() < 3 {
            return Err("Invalid k2_parse_struct.tokens length".to_string());
        }
        let parent_name = &k2_parse_struct.tokens[1];
        let child_name = &k2_parse_struct.tokens[2];
        if self.objects.get(parent_name).is_none() {
            return Err("Parent object does not exist".to_string());
        }
        if self.objects.get(child_name).is_none() {
            return Err("Child object does not exist".to_string());
        }
        let parent_object = self.objects.get(parent_name).unwrap();
        let child_object = self.objects.get(child_name).unwrap();
        match parent_object.object_type.as_str() {
            "mode" => {
                if child_object.object_type != "chain" {
                    return Err("Child object must be a chain".to_string());
                }
            },
            "chain" => {
                if child_object.object_type != "stream" {
                    return Err("Child object must be a processor".to_string());
                }
            },
            _ => return Err("Parent object must be a mode or a chain".to_string()),
        }
        let mut parent_object = self.objects.get(parent_name).unwrap().clone();
        parent_object.children.push(child_name.to_string());
        let mut child_object = self.objects.get_mut(child_name).unwrap().clone();
        child_object.parent.push(parent_name.to_string());
        self.objects.insert(parent_name.to_string(), parent_object.clone());
        self.objects.insert(child_name.to_string(), child_object.clone());
        Ok(
            K2ReturnStruct {
                success: true,
                command: k2_parse_struct.command.clone(),
                tokens: k2_parse_struct.tokens.clone(),
                message: "Add".to_string(),
                data: Some(vec![parent_object.clone(), child_object.clone()]),
        })
    }
    pub fn parse_delete(&mut self, k2_parse_struct: &K2ReturnStruct) -> AstReturn {
        if k2_parse_struct.tokens.len() < 2 {
            return Err("Invalid k2_parse_struct.tokens length".to_string());
        }
        let object_name = &k2_parse_struct.tokens[1];
        if self.objects.get(object_name).is_none() {
            return Err("Object does not exist".to_string());
        }
        let object = self.objects.get_mut(object_name).unwrap().clone();
        for parent_name in &object.parent {
            if let Some(parent_object) = self.objects.get_mut(parent_name) {
                parent_object.children.retain(|child| child != object_name);
            }
        }
        let mut deleted_objects = vec![object.clone()];
        for child_name in &object.children {
                if let Some(child_object) = self.objects.get_mut(child_name) {
                    child_object.parent.retain(|parent| parent != object_name);
                    deleted_objects.push(child_object.clone());
                }
            self.objects.remove(child_name);
        }
        self.objects.remove(object_name);
        Ok(
            K2ReturnStruct {
                success: true,
                command: k2_parse_struct.command.clone(),
                tokens: k2_parse_struct.tokens.clone(),
                message: "Delete".to_string(),
                data: Some(deleted_objects),
        })
    }
    pub fn parse_set(&mut self, k2_parse_struct: &K2ReturnStruct) -> AstReturn {
        if k2_parse_struct.tokens.len() < 4 {
            return Err("Invalid k2_parse_struct.tokens length".to_string());
        }
        let object_name = &k2_parse_struct.tokens[1];
        if self.objects.get(object_name).is_none() {
            let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
            if split_name.len() == 3 {
                let parent_name = format!("{}.{}", split_name[0], split_name[1]);
                if let Some(parent_object) = self.objects.get(&parent_name) {
                    if parent_object.object_type == "processor" {
                        let code_part = &split_name[2];
                        ProcessorCodePart::try_from(code_part.clone()).map_err(|_| "Invalid code part".to_string())?;
                    } else {
                        return Err("Object does not exist".to_string());
                    }
                } else {
                    return Err("Object does not exist".to_string());
                }
            } else {
                return Err("Object does not exist".to_string());
            }
        }
        let property_name = &k2_parse_struct.tokens[2];
        let property_value = k2_parse_struct.tokens[3..].join(" ");
        let object = self.objects.get_mut(object_name).unwrap();
        object.properties.insert(property_name.to_string(), property_value);
        Ok(
            K2ReturnStruct {
                success: true,
                command: k2_parse_struct.command.clone(),
                tokens: k2_parse_struct.tokens.clone(),
                message: "Set".to_string(),
                data: Some(vec![object.clone()]),
        })
    }
    pub fn parse_connect(&mut self, k2_parse_struct: &K2ReturnStruct) -> AstReturn {
        if k2_parse_struct.tokens.len() < 3 {
            return Err("Invalid k2_parse_struct.tokens length".to_string());
        }
        let source_name = &k2_parse_struct.tokens[1];
        let target_name = &k2_parse_struct.tokens[2];
        let source_name_split: Vec<String> = source_name.split(".").map(|s| s.to_string()).collect();
        let target_name_split: Vec<String> = target_name.split(".").map(|s| s.to_string()).collect();
        if source_name_split.len() != 3 || target_name_split.len() != 3 {
            return Err("Invalid object name format".to_string());
        }

        let source_parent_name = format!("{}.{}", source_name_split[0], source_name_split[1]);
        let target_parent_name = format!("{}.{}", target_name_split[0], target_name_split[1]);
        
        if self.objects.get(&source_parent_name).is_none() || self.objects.get(&target_parent_name).is_none() {
            return Err("Source or target parent object does not exist".to_string());
        }
        let source_parent_object = self.objects.get(&source_parent_name).unwrap();
        let target_parent_object = self.objects.get(&target_parent_name).unwrap();
        if source_parent_object.object_type != "stream" || target_parent_object.object_type != "stream" {
            return Err("Source and target parent objects must be streams".to_string());
        }
        let connection_number = source_parent_object.properties.get("connection").unwrap_or(&"0".to_string()).parse::<u32>().unwrap_or(0) + 1;
        let source_parent_object = self.objects.get_mut(&source_parent_name).unwrap();
        source_parent_object.properties.insert(format!("connection_{}", connection_number), format!("{}-{}", source_name, target_name));
        source_parent_object.properties.insert("connection".to_string(), connection_number.to_string());
        Ok(
            K2ReturnStruct {
                success: true,
                command: k2_parse_struct.command.clone(),
                tokens: k2_parse_struct.tokens.clone(),
                message: "Connect".to_string(),
                data: Some(vec![source_parent_object.clone()]),
        })
    }
    pub fn parse_disconnect(&mut self, k2_parse_struct: &K2ReturnStruct) -> AstReturn {
        // parse the disconnect k2_parse_struct.tokens and execute the corresponding action
        if k2_parse_struct.tokens.len() < 3 {
            return Err("Invalid k2_parse_struct.tokens length".to_string());
        }
        let source_name = &k2_parse_struct.tokens[1];
        let target_name = &k2_parse_struct.tokens[2];
        let source_name_split: Vec<String> = source_name.split(".").map(|s| s.to_string()).collect();
        let target_name_split: Vec<String> = target_name.split(".").map(|s| s.to_string()).collect();
        if source_name_split.len() != 3 || target_name_split.len() != 3 {
            return Err("Invalid object name format".to_string());
        }

        let source_parent_name = format!("{}.{}", source_name_split[0], source_name_split[1]);
        let target_parent_name = format!("{}.{}", target_name_split[0], target_name_split[1]);
        
        if self.objects.get(&source_parent_name).is_none() || self.objects.get(&target_parent_name).is_none() {
            return Err("Source or target parent object does not exist".to_string());
        }
        let source_parent_object = self.objects.get(&source_parent_name).unwrap();
        let target_parent_object = self.objects.get(&target_parent_name).unwrap();
        if source_parent_object.object_type != "stream" || target_parent_object.object_type != "stream" {
            return Err("Source and target parent objects must be streams".to_string());
        }
        let source_parent_object = self.objects.get_mut(&source_parent_name).unwrap();
        let properties = source_parent_object.properties.clone();
        if let Some(connection) = properties.iter().find(|(_, v)| v == &&format!("{}-{}", source_name, target_name)) {
            source_parent_object.properties.remove(connection.0);
            let connection_number = source_parent_object.properties.get("connection").unwrap_or(&"0".to_string()).parse::<i32>().unwrap_or(0) - 1;
            if connection_number >= 0 {
                source_parent_object.properties.insert("connection".to_string(), connection_number.to_string());
            }
            return Ok(
                K2ReturnStruct {
                    success: true,
                    command: k2_parse_struct.command.clone(),
                    tokens: k2_parse_struct.tokens.clone(),
                    message: "Disconnect".to_string(),
                    data: Some(vec![source_parent_object.clone()]),
            })
        } else {
            return Err("Connection does not exist".to_string());
        }
    }
    fn parse_exec(&mut self, k2_parse_struct: &K2ReturnStruct) -> AstReturn {
        // parse the exec k2_parse_struct.tokens and execute the corresponding action
         if k2_parse_struct.tokens.len() < 2 {
            return Err("Invalid k2_parse_struct.tokens length".to_string());
        }
        Ok(k2_parse_struct.clone())
    }
    pub fn parse_command(&mut self, k2_parse_struct: &K2ReturnStruct) -> K2ReturnStruct {
        let mut response = k2_parse_struct.clone();
        if !k2_parse_struct.success {
            return response;
        }
        if let Some(callback) = self.callbacks_cmd.get(k2_parse_struct.tokens[0].clone().as_str()) {
            let result = callback(self, &k2_parse_struct);
            match result {
                Ok(res) => {
                    response = res.clone();
                }
                Err(_) => {
                    response = K2ReturnStruct {
                        success: false,
                        command: k2_parse_struct.command.clone(),
                        tokens: k2_parse_struct.tokens.clone(),
                        message: format!("Error"),
                        data: None,
                    };
                }
            }
        } else {
            response = K2ReturnStruct {
                success: false,
                command: k2_parse_struct.command.clone(),
                tokens: k2_parse_struct.tokens.clone(),
                message: format!("Unknown"),
                data: None,
            };
        }
        response
    }
}

impl ProcessorTrait for AstProcessor {
    fn new(name: String) -> ProcessorNewReturn {
        let mut callbacks_cmd: HashMap<String, AstCallback> = HashMap::new();
        callbacks_cmd.insert("new".to_string(), AstProcessor::parse_new);
        callbacks_cmd.insert("add".to_string(), AstProcessor::parse_add);
        callbacks_cmd.insert("delete".to_string(), AstProcessor::parse_delete);
        callbacks_cmd.insert("set".to_string(), AstProcessor::parse_set);
        callbacks_cmd.insert("connect".to_string(), AstProcessor::parse_connect);
        callbacks_cmd.insert("disconnect".to_string(), AstProcessor::parse_disconnect);
        callbacks_cmd.insert("exec".to_string(), AstProcessor::parse_exec);
        let ret = AstProcessor {
            name,
            header: ProcessorHeader {
                proc_name: "AstProcessor".to_string(),
                description: "The ast analyzer of K2Lang".to_string(),
                version: "0.1.0".to_string(),
                author: "Sofia Silvestri".to_string(),
                email: "ms.sofia.silvestri@gmail.com".to_string(),
                license: "LGPLv2.0".to_string(),
                repository: "".to_string(),
            },
            stream_block: StreamBlock::new(),
            state: Arc::new(Mutex::new(StreamState::Uninitialized)),
            objects: HashMap::new(),
            callbacks_cmd,
        };
        Ok(Box::new(ret))
    }
    fn initialize(&mut self ) -> Result<(), K2Error> {
        self.stream_block.add_input::<K2ReturnStruct>("command".to_string())?;
        self.stream_block.add_output::<K2ReturnStruct>("response".to_string())?;
        Ok(())
    }
    fn process(&mut self) -> Result<(), K2Error> {
        *self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to update status"))? = StreamState::Running;
        let command_input = self.stream_block.get_input::<K2ReturnStruct>(&"command".to_string())?;
        let k2_parse_struct = command_input.receive()?;
        let mut response = k2_parse_struct.clone();
        if k2_parse_struct.success {
            // Here you would implement the logic to parse the k2_parse_struct.tokens and return the appropriate response
            response = self.parse_command(&k2_parse_struct);
        }
        let response_output = self.stream_block.get_output::<K2ReturnStruct>(&"response".to_string())?;
        response_output.send(response)?;
        Ok(())
    }
    fn finalize(&mut self) -> Result<(), K2Error> {
        *self.state.lock()
            .map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to update status"))? 
                = StreamState::Waiting;
        Ok(())
    }
}