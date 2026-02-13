use std::{collections::HashMap, sync::{Arc, Mutex, MutexGuard}};

use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;
use k2_stream::{memory::{DataHeader, MemoryTrait}, processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorTrait, StreamBlock, StreamState}};
use crate::{K2Object, K2ReturnStruct, coder::ProcessorCoderParts};


type ParserReturn = Result<K2ReturnStruct, String>;
type ParserCallback = fn(&mut Parser, &Vec<String>) -> ParserReturn;

#[derive(K2Memory, K2ProcessorBlock)]
pub struct Parser {
    // fields for the parser
    pub name: String,
    pub header: ProcessorHeader,
    stream_block: StreamBlock,
    state: Arc<Mutex<StreamState>>,
    objects: HashMap<String, K2Object>,
    callbacks_cmd: HashMap<String, ParserCallback>,
}

impl Parser {
    pub fn new() -> Result<Self, ()> {
        let mut callbacks_cmd: HashMap<String, ParserCallback> = HashMap::new();
        callbacks_cmd.insert("new".to_string(), Parser::parse_new);
        callbacks_cmd.insert("add".to_string(), Parser::parse_add);
        callbacks_cmd.insert("delete".to_string(), Parser::parse_delete);
        callbacks_cmd.insert("set".to_string(), Parser::parse_set);
        callbacks_cmd.insert("connect".to_string(), Parser::parse_connect);
        callbacks_cmd.insert("disconnect".to_string(), Parser::parse_disconnect);
        callbacks_cmd.insert("exec".to_string(), Parser::parse_exec);

        let mut self_instance = Self {
            name: "Parser".to_string(),
            header: ProcessorHeader {
                proc_name: "Parser".to_string(),
                description: "A processor that parses the commands and executes the corresponding actions".to_string(),
                version: "0.1.0".to_string(),
                author: "Sofia Silvestri".to_string(),
                email: "ms.sofia.silvestri@gmail.com".to_string(),
                license: "LGPLv2.0".to_string(),
                repository: "".to_string(),
            },
            stream_block: StreamBlock::new(),
            state: Arc::new(Mutex::new(StreamState::Initialized)),
            objects: HashMap::new(),
            callbacks_cmd,
        };
        self_instance.stream_block.add_input::<String>("command".to_string())?;
        self_instance.stream_block.add_output::<K2ReturnStruct>("response".to_string())?;
        Ok(self_instance)
    }
    pub fn split_commands(&self, command: &String) -> Vec<Vec<String>> {
        let lines = command.lines();
        let mut commands: Vec<String> = Vec::new();
        for line in lines {
            let mut line_commands = line.split(";").map(|s| s.to_string()).collect();
            commands.append(&mut line_commands);
        }
        commands.into_iter().map(|cmd| cmd.split_whitespace().map(|s| s.to_string()).collect()).collect()
        //
    }
    pub fn parse_new(&mut self, command: &Vec<String>) -> ParserReturn {
        if command.len() < 3 {
            return Err("Invalid command length".to_string());
        }
        let object_type = &command[1];
        let object_name = &command[2];
        if self.objects.get(object_name).is_some() {
            return Err("Object with the same name already exists".to_string());
        }
        if !object_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err("Invalid object name".to_string());
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
                if command.len() < 4 {
                    return Err("Invalid command length for input/output".to_string());
                }
                let data_type = &command[3];
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
                if command.len() < 5 {
                    return Err("Invalid command length for parameter/state".to_string());
                }
                let data_type = &command[3];
                if data_type.is_empty() {
                    return Err("Data type cannot be empty".to_string());
                }
                let value = &command[4];
                if value.is_empty() {
                    return Err("Value cannot be empty".to_string());
                }
                processing_object.properties.insert("type".to_string(), data_type.to_string());
                processing_object.properties.insert("value".to_string(), value.to_string());
            }
            "code" => {
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 3 {
                    return Err("Invalid object name format for parameter/state".to_string());
                }
                if command.len() < 5 {
                    return Err("Invalid command length for code".to_string());
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
                ProcessorCoderParts::try_from(command[3].to_string())
                    .map_err(|_| format!("Invalid processor code part: {}", command[3].to_string()))?;
                processing_object.properties.insert("block".to_string(), command[3].to_string());
                processing_object.properties.insert("code".to_string(), command[4].to_string());
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
                if command.len() < 3 {
                    return Err("Invalid command length for library/application".to_string());
                }
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 1 {
                    return Err("Invalid object name format".to_string());
                }
                if self.objects.get(&split_name[0]).is_some() {
                    return Err("Object with the same name already exists".to_string());
                }
                processing_object.properties.insert("path".to_string(), command[2].to_string());
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
                if command.len() < 4 {
                    return Err("Invalid command length for stream".to_string());
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
                let processor_type = &command[3];
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
        Ok(
            K2ReturnStruct {
                success: true,
                command: "".to_string(),
                message: "Create".to_string(),
                data: Some(vec![processing_object]),
        })
    }
    pub fn parse_add(&mut self, command: &Vec<String>) -> ParserReturn {
        // parse the add command and execute the corresponding action
        if command.len() < 3 {
            return Err("Invalid command length".to_string());
        }
        let parent_name = &command[1];
        let child_name = &command[2];
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
                command: "".to_string(),
                message: "Add".to_string(),
                data: Some(vec![parent_object.clone(), child_object.clone()]),
        })
    }
    pub fn parse_delete(&mut self, command: &Vec<String>) -> ParserReturn {
        if command.len() < 2 {
            return Err("Invalid command length".to_string());
        }
        let object_name = &command[1];
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
                command: "".to_string(),
                message: "Delete".to_string(),
                data: Some(deleted_objects),
        })
    }
    pub fn parse_set(&mut self, command: &Vec<String>) -> ParserReturn {
        if command.len() < 3 {
            return Err("Invalid command length".to_string());
        }
        let object_name = &command[1];
        if self.objects.get(object_name).is_none() {
            return Err("Object does not exist".to_string());
        }
        let property_name = &command[2];
        let property_value = if command.len() > 3 {
            command[3..].join(" ")
        } else {
            return Err("Property value is missing".to_string());
        };
        let object = self.objects.get_mut(object_name).unwrap();
        object.properties.insert(property_name.to_string(), property_value);
        Ok(
            K2ReturnStruct {
                success: true,
                command: "".to_string(),
                message: "Set".to_string(),
                data: Some(vec![object.clone()]),
        })
    }
    pub fn parse_connect(&mut self, command: &Vec<String>) -> ParserReturn {
        if command.len() < 3 {
            return Err("Invalid command length".to_string());
        }
        let source_name = &command[1];
        let target_name = &command[2];
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
                command: "".to_string(),
                message: "Connect".to_string(),
                data: Some(vec![source_parent_object.clone()]),
        })
    }
    pub fn parse_disconnect(&mut self, command: &Vec<String>) -> ParserReturn {
        // parse the disconnect command and execute the corresponding action
        if command.len() < 3 {
            return Err("Invalid command length".to_string());
        }
        let source_name = &command[1];
        let target_name = &command[2];
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
                    command: "".to_string(),
                    message: "Disconnect".to_string(),
                    data: Some(vec![source_parent_object.clone()]),
            })
        } else {
            return Err("Connection does not exist".to_string());
        }
    }
    pub fn parse_exec(&mut self, command: &Vec<String>) -> ParserReturn {
        if command.len() < 2 {
            return Err("Invalid command length".to_string());
        }
        let message = &command[1];
        Ok(
            K2ReturnStruct {
                success: true,
                command: "".to_string(),
                message: message.to_string(),
                data: None,
            }
        )
        
    }
    pub fn parse_command(&mut self, command: &String) -> K2ReturnStruct {
        let tokenized_commands = self.split_commands(&command);
        let mut response = K2ReturnStruct {
            success: false,
            command: command.to_string(),
            message: "Unknown".to_string(),
            data: None,
        };
        for cmd in tokenized_commands {
            if cmd.is_empty() {
                continue;
            }
            if let Some(callback) = self.callbacks_cmd.get(&cmd[0]) {
                let result = callback(self, &cmd);
                match result {
                    Ok(mut res) => {
                        res.command = command.to_string();
                        response.command = res.command.clone();
                    }
                    Err(_) => {
                        response = K2ReturnStruct {
                            success: false,
                            command: command.to_string(),
                            message: format!("Error"),
                            data: None,
                        };
                        break;
                    }
                }
            } else {
                response = K2ReturnStruct {
                    success: false,
                    command: command.to_string(),
                    message: format!("Unknown"),
                    data: None,
                };
                break;
            }
        }
        response
    }
}

impl ProcessorTrait for Parser {
    fn initialize(&mut self) -> Result<(), ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        *state = StreamState::Initialized;
        Ok(())
    }
    fn process(&mut self) -> Result<(), ()> {
        *self.state.lock().map_err(|_| ())? = StreamState::Running;
        let command_input = self.stream_block.get_input::<String>(&"command".to_string())?;
        let command_str = command_input.receive()?;
        let response = self.parse_command(&command_str);
        let response_output = self.stream_block.get_output::<K2ReturnStruct>(&"response".to_string())?;
        response_output.send(response)?;
        Ok(())
    }
    fn finalize(&mut self) -> Result<(), ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        *state = StreamState::Waiting;
        Ok(())
    }
}