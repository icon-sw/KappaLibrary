use std::{collections::HashMap, sync::{Arc, Mutex, MutexGuard}};

use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;
use k2_stream::{k2err, errors::{K2Error, K2ErrorCode}, processor::memory::{DataHeader, MemoryTrait}, processor::processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState}};
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
    pub fn check_name(name: &String) -> Result<Token , String> {
        if !name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.') {
            return Err("Invalid object name".to_string());
        }
        let split_name: Vec<String> = name.split(".").map(|s| s.to_string()).collect();
        for string in split_name.clone() {
            if string.is_empty() {
                return Err("Invalid object name".to_string());
            }
        }
        Ok(split_name)
    }
    pub fn split_commands(command: &String) -> Vec<Vec<String>> {
        let lines: std::str::Lines<'_> = command.lines();
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
        let split_name: Vec<String> = Parser::check_name(object_name)?;

        match object_type.as_str() {
            "input" | "output" => {
                if command.len() != 4 {
                    return Err("Invalid command length for input/output".to_string());
                }
                if split_name.len() != 3 {
                    return Err("Invalid object name format for input/output".to_string());
                }
                let data_type = &command[3];
                if data_type.is_empty() {
                    return Err("Data type cannot be empty".to_string());
                }
            },
            "parameter" | "state" => {
                if command.len() < 5 {
                    return Err("Invalid command length for parameter/state".to_string());
                }
                if split_name.len() != 3 {
                    return Err("Invalid object name format for parameter/state".to_string());
                }
                let data_type = &command[3];
                if data_type.is_empty() {
                    return Err("Data type cannot be empty".to_string());
                }
                let value = &command[4];
                if value.is_empty() {
                    return Err("Value cannot be empty".to_string());
                }
                if object_type.as_str() == "parameter" && command.len() != 6 {
                    return Err("Missing parameter type".to_string());
                }
            }
            "processor" => {
                if command.len() != 3 {
                    return Err("Invalid command length for processor".to_string());
                }
                if split_name.len() != 2 {
                    return Err("Invalid object name format for processor".to_string());
                }
            }
            "library" | "application" => {
                if command.len() != 4 {
                    return Err("Invalid command length for library/application".to_string());
                }
                if split_name.len() != 1 {
                    return Err("Invalid object name format".to_string());
                }
            }
            "chain" | "mode" => {
                if command.len() != 3 {
                    return Err("Invalid command length for chain/mode".to_string());
                }
                if split_name.len() != 3 {
                    return Err("Invalid object name format for chain/mode".to_string());
                }
            }
            "block" => {
                if command.len() != 4 {
                    return Err("Invalid command length for block".to_string());
                }
                if split_name.len() != 3 {
                    return Err("Invalid object name format for block".to_string());
                }
                let processor_type = &command[3];
                if processor_type.is_empty() {
                    return Err("Processor cannot be empty".to_string());
                }
                let split_name: Vec<String> = processor_type.split(".").map(|s| s.to_string()).collect();
                if split_name.len() != 2 {
                    return Err("Invalid object name format for processor".to_string());
                }
            }
            "stream" => {
                if command.len() != 3 {
                    return Err("Invalid command length for stream".to_string());
                }
                if split_name.len() != 2 {
                    return Err("Invalid object name format for stream".to_string());
                }
            }
            "command" => {
                if command.len() != 4 {
                    return Err("Invalid command length for command".to_string());
                }
                if !object_name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' ) {
                    return Err("Invalid object name".to_string());
                }
                if split_name.len() != 3 || split_name[2].is_empty() {
                    return Err("Invalid object name format for command".to_string());
                }
            },
            _ => return Err("Invalid object type".to_string()),
        }
        Ok(command.clone())
    }
    pub fn parse_add(&mut self, command: &Vec<String>) -> ParserReturn {
        // parse the add command and execute the corresponding action
        if command.len() != 3 {
            return Err("Invalid command length".to_string());
        }
        let split_name: Vec<String> = Parser::check_name(&command[1])?;
        let level_parent = split_name.len();
        let split_name: Vec<String> = Parser::check_name(&command[2])?;
        let level_children = split_name.len();
        if level_children >= level_parent {
            return Err("Invalid gerarchy".to_string());
        }
        Ok(command.clone())
    }
    pub fn parse_delete(&mut self, command: &Vec<String>) -> ParserReturn {
        if command.len() != 2 {
            return Err("Invalid command length".to_string());
        }
        Parser::check_name(&command[1])?;
        Ok(command.clone())
    }
    pub fn parse_set(&mut self, command: &Vec<String>) -> ParserReturn {
        if command.len() != 3 {
            return Err("Invalid command length".to_string());
        }
        let split_name: Vec<String> = Parser::check_name(&command[1])?;
        if split_name.len() != 3 {
            return Err("Invalid object name format".to_string());
        }
        Ok(command.clone())
    }
    pub fn parse_connect(&mut self, command: &Vec<String>) -> ParserReturn {
        if command.len() != 3 {
            return Err("Invalid command length".to_string());
        }
        let source_name = &command[1];
        let target_name = &command[2];
        let source_name_split: Vec<String> = Parser::check_name(source_name)?;
        let target_name_split: Vec<String> = Parser::check_name(target_name)?;
        if source_name_split.len() != 3 || target_name_split.len() != 3 {
            return Err("Invalid object name format".to_string());
        }

        Ok(command.clone())
    }
    pub fn parse_disconnect(&mut self, command: &Vec<String>) -> ParserReturn {
        // parse the disconnect command and execute the corresponding action
        if command.len() != 3 {
            return Err("Invalid command length".to_string());
        }
        let source_name = &command[1];
        let target_name = &command[2];
        let source_name_split: Vec<String> = Parser::check_name(source_name)?;
        let target_name_split: Vec<String> = Parser::check_name(target_name)?;
        if source_name_split.len() != 3 || target_name_split.len() != 3 {
            return Err("Invalid object name format".to_string());
        }
        Ok(command.clone())
    }
    pub fn parse_exec(&mut self, command: &Vec<String>) -> ParserReturn {
        if command.len() != 2 {
            return Err("Invalid command length".to_string());
        }
        let message = &command[1];
        if message.is_empty() {
            return Err("Message cannot be empty".to_string());
        }
        let split_name: Vec<String> = Parser::check_name(message)?;
        if !(split_name.len() == 3 || split_name.len() == 2) {
            return Err("Invalid object name format".to_string());
        }
        Ok(command.clone())
    }
    pub fn parse_command(&mut self, command: &String, tokenized_commands: Vec<String>) -> K2ReturnStruct {
        let response: K2ReturnStruct;
        
        if let Some(callback) = self.callbacks_cmd.get(tokenized_commands[0].clone().as_str()) {
            let result = callback(self, &tokenized_commands);
            match result {
                Ok(res) => {
                    response = K2ReturnStruct {
                        success: true,
                        command: command.to_string(),
                        tokens: res.clone(),
                        message: format!("Ok"),
                        data: Vec::new(),
                    };
                }
                Err(e) => {
                    response = K2ReturnStruct {
                        success: false,
                        command: command.to_string(),
                        tokens: Vec::new(),
                        message: e,
                        data: Vec::new(),
                    };
                }
            }
        } else {
            response = K2ReturnStruct {
                success: false,
                command: command.to_string(),
                tokens: Vec::new(),
                message: format!("Unknown"),
                data: Vec::new(),
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
    fn initialize(&mut self) -> Result<(), K2Error> {
        let mut state = self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to set state"))?;
        *state = StreamState::Initialized;
        Ok(())
    }
    fn process(&mut self) -> Result<(), K2Error> {
        *self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to set state"))? = StreamState::Running;
        let command_input = self.stream_block.get_input::<String>(&"command".to_string())?;
        let command_str = command_input.receive()?;
        let tokenized_commands = Self::split_commands(&command_str);
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
    fn finalize(&mut self) -> Result<(), K2Error> {
        let mut state = self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to set state"))?;
        *state = StreamState::Waiting;
        Ok(())
    }
}

#[cfg(test)]
mod test
{
    use std::{fs::File, io::{BufRead, BufReader}, sync::mpsc};

    use super::*;
    #[test]
    fn test_parser() {
        let parser = Parser::new("parser".to_string());
        assert!(parser.is_ok());
        let ( sender,  receiver) = mpsc::sync_channel::<K2ReturnStruct>(10);
        let mut parser = parser.unwrap();

        let response_port = parser.get_stream_block_mut().get_output_mut::<K2ReturnStruct>(&"response".to_string());
        assert!(response_port.is_ok());
        let response_port = response_port.unwrap();
        response_port.connect(sender);

        let command_port = parser.get_stream_block_mut().get_input::<String>(&"command".to_string());
        assert!(command_port.is_ok());
        let command_port = command_port.clone().unwrap().get_sender().clone();

        assert!(parser.initialize().is_ok());
        let file = File::open("test/ok_parser_command").map_err(|_| k2err!(K2ErrorCode::NotFound,""));
        let reader = BufReader::new(file.unwrap());
        for (count, line) in reader.lines().enumerate() {
            assert!(line.is_ok());
            let line = line.unwrap();
            dbg!(line.clone());
            assert!(command_port.send(line).is_ok());
            assert!(parser.process().is_ok());
            let response = receiver.recv();
            assert!(response.is_ok());
            let response = response.unwrap();
            dbg!(response.clone());
            assert!(response.success);
            dbg!(count);
        }
        let file = File::open("test/err_parser_command").map_err(|_| k2err!(K2ErrorCode::NotFound,""));
        let reader = BufReader::new(file.unwrap());
        for (count, line) in reader.lines().enumerate() {
            assert!(line.is_ok());
            let line = line.unwrap();
            dbg!(line.clone());
            assert!(command_port.send(line).is_ok());
            assert!(parser.process().is_ok());
            let response = receiver.recv();
            assert!(response.is_ok());
            let response = response.unwrap();
            dbg!(response.clone());
            assert!(!response.success);
            dbg!(count);
        }
    }
}