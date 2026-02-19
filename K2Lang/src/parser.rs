use std::{collections::HashMap, sync::{Arc, Mutex, MutexGuard}};

use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;
use k2_stream::{memory::{DataHeader, MemoryTrait}, processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState}};
use crate::{K2ReturnStruct, Token};

type ParserReturn = Result<Token, String>;
type ParserCallback = fn(&mut Parser, &Vec<String>) -> ParserReturn;

#[derive(K2Memory, K2ProcessorBlock)]
pub struct Parser {
    // fields for the parser
    pub name: String,
    pub header: ProcessorHeader,
    stream_block: StreamBlock,
    state: Arc<Mutex<StreamState>>,
    callbacks_cmd: HashMap<String, ParserCallback>,
}

impl Parser {
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
        if !object_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err("Invalid object name".to_string());
        }
        match object_type.as_str() {
            "input" | "output" => {
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 3 {
                    return Err("Invalid object name format for input/output".to_string());
                }
                if command.len() < 4 {
                    return Err("Invalid command length for input/output".to_string());
                }
                let data_type = &command[3];
                if data_type.is_empty() {
                    return Err("Data type cannot be empty".to_string());
                }
            },
            "parameter" | "state" => {
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 3 {
                    return Err("Invalid object name format for parameter/state".to_string());
                }
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
            }
            "processor" => {
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 2 {
                    return Err("Invalid object name format for processor".to_string());
                }
            }
            "library" | "application" => {
                if command.len() < 3 {
                    return Err("Invalid command length for library/application".to_string());
                }
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 1 {
                    return Err("Invalid object name format".to_string());
                }
            }
            "chain" | "mode" => {
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 2 {
                    return Err("Invalid object name format for chain/mode".to_string());
                }
            }
            "stream" => {
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 2 {
                    return Err("Invalid object name format for stream".to_string());
                }
                if command.len() < 4 {
                    return Err("Invalid command length for stream".to_string());
                }
                let processor_type = &command[3];
                if processor_type.is_empty() {
                    return Err("Processor cannot be empty".to_string());
                }
            }
            "command" => {
                if !object_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    return Err("Invalid object name".to_string());
                }
            },
            _ => return Err("Invalid object type".to_string()),
        }
        Ok(command.clone())
    }
    pub fn parse_add(&mut self, command: &Vec<String>) -> ParserReturn {
        // parse the add command and execute the corresponding action
        if command.len() < 3 {
            return Err("Invalid command length".to_string());
        }
        Ok(command.clone())
    }
    pub fn parse_delete(&mut self, command: &Vec<String>) -> ParserReturn {
        if command.len() < 2 {
            return Err("Invalid command length".to_string());
        }
        Ok(command.clone())
    }
    pub fn parse_set(&mut self, command: &Vec<String>) -> ParserReturn {
        if command.len() < 4 {
            return Err("Invalid command length".to_string());
        }
        let split_name: Vec<String> = command[1].split(".").map(|s| s.to_string()).collect();
        if split_name.len() != 3 {
            return Err("Invalid object name format".to_string());
        }
        Ok(command.clone())
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

        Ok(command.clone())
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
        Ok(command.clone())
    }
    pub fn parse_exec(&mut self, command: &Vec<String>) -> ParserReturn {
        if command.len() < 2 {
            return Err("Invalid command length".to_string());
        }
        let message = &command[1];
        if message.is_empty() {
            return Err("Message cannot be empty".to_string());
        }
        Ok(command.clone())
    }
    pub fn parse_command(&mut self, command: &String, tokenized_commands: Vec<String>) -> K2ReturnStruct {
        let mut response = K2ReturnStruct {
            success: false,
            command: command.to_string(),
            tokens: Vec::new(),
            message: "Unknown".to_string(),
            data: None,
        };
        
        if let Some(callback) = self.callbacks_cmd.get(tokenized_commands[0].clone().as_str()) {
            let result = callback(self, &tokenized_commands);
            match result {
                Ok(res) => {
                    response.tokens = res.clone();
                }
                Err(_) => {
                    response = K2ReturnStruct {
                        success: false,
                        command: command.to_string(),
                        tokens: Vec::new(),
                        message: format!("Error"),
                        data: None,
                    };
                }
            }
        } else {
            response = K2ReturnStruct {
                success: false,
                command: command.to_string(),
                tokens: Vec::new(),
                message: format!("Unknown"),
                data: None,
            };
        }
        response
    }
}

impl ProcessorTrait for Parser {
    fn new(name: String) -> ProcessorNewReturn {
        let mut callbacks_cmd: HashMap<String, ParserCallback> = HashMap::new();
        callbacks_cmd.insert("new".to_string(), Parser::parse_new);
        callbacks_cmd.insert("add".to_string(), Parser::parse_add);
        callbacks_cmd.insert("delete".to_string(), Parser::parse_delete);
        callbacks_cmd.insert("set".to_string(), Parser::parse_set);
        callbacks_cmd.insert("connect".to_string(), Parser::parse_connect);
        callbacks_cmd.insert("disconnect".to_string(), Parser::parse_disconnect);
        callbacks_cmd.insert("exec".to_string(), Parser::parse_exec);

        let mut self_instance = Self {
            name: name.clone(),
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
            callbacks_cmd,
        };
        self_instance.stream_block.add_input::<String>("command".to_string())?;
        self_instance.stream_block.add_output::<K2ReturnStruct>("response".to_string())?;
        Ok(Box::new(self_instance))
    }
    fn initialize(&mut self) -> Result<(), ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        *state = StreamState::Initialized;
        Ok(())
    }
    fn process(&mut self) -> Result<(), ()> {
        *self.state.lock().map_err(|_| ())? = StreamState::Running;
        let command_input = self.stream_block.get_input::<String>(&"command".to_string())?;
        let command_str = command_input.receive()?;
        let tokenized_commands = self.split_commands(&command_str);
        for cmd in &tokenized_commands {
            if cmd.is_empty() {
                continue;
            }
            let response = self.parse_command(&command_str, cmd.clone());
            let response_output = self.stream_block.get_output::<K2ReturnStruct>(&"response".to_string())?;
            response_output.send(response)?;
        }
        
        Ok(())
    }
    fn finalize(&mut self) -> Result<(), ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        *state = StreamState::Waiting;
        Ok(())
    }
}