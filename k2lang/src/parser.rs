use std::{collections::HashMap, sync::{Arc, Mutex, MutexGuard}};

use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;
use k2stream::{k2err, errors::{K2Error, K2ErrorCode}, processor::memory::{DataHeader, MemoryTrait}, processor::processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState}};

use crate::{K2LangObject, K2LangReturn, K2LangStruct};

type K2ParserCallback = fn(&K2LangStruct) -> K2LangReturn;

#[derive(K2Memory, K2ProcessorBlock)]
pub struct Parser {
    // fields for the parser
    pub name: String,
    pub header: ProcessorHeader,
    stream_block: StreamBlock,
    state: Arc<Mutex<StreamState>>,
    callbacks_cmd: HashMap<String, K2ParserCallback>,
}

impl Parser {
    pub fn check_name(name: &String) -> Result<Vec<String> , K2Error> {
        if !name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.') {
            return Err(k2err!(K2ErrorCode::BadFormat,"Invalid object name".to_string()));
        }
        let split_name: Vec<String> = name.split(".").map(|s| s.to_string()).collect();
        for string in split_name.clone() {
            if string.is_empty() {
                return Err(k2err!(K2ErrorCode::BadFormat, "Invalid object name".to_string()));
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
    }
    pub fn parse_new(input: &K2LangStruct) -> K2LangReturn {
        let mut output = input.clone();
        let token_name = Parser::check_name(&input.tokens[2].clone())?;
        match input.tokens[1].clone().as_str() {
            "input" | "output" => {
                if input.tokens.len() != 4 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for new input/output".to_string()));
                }
                if token_name.len() != 3 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Invalid name for input/output".to_string()));
                }
            }
            "state" => {
                if input.tokens.len() != 5 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for new state".to_string()));
                }
                if token_name.len() != 3 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Invalid name for state".to_string()));
                }
            }
            "parameter" => {
                if input.tokens.len() != 6 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for new parameter".to_string()));
                }
                if token_name.len() != 3 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Invalid name for parameter".to_string()));
                }
            }
            "command" => {
                if input.tokens.len() != 4 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for new command".to_string()));
                }
                if token_name.len() != 3 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Invalid name for command".to_string()));
                }
            }
            "processor" => {
                if input.tokens.len() != 3 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for new processor".to_string()));
                }
                if token_name.len() != 2 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Invalid name for processor".to_string()));
                }
            }
            "library" => {
                if input.tokens.len() != 4 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for new library".to_string()));
                }
                if token_name.len() != 1 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Invalid name for library".to_string()));
                }
            }
            "block" => {
                if input.tokens.len() != 4 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for new block".to_string()));
                }
                if token_name.len() != 3 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Invalid name for block".to_string()));
                }
            }
            "chain" => {
                if input.tokens.len() != 3 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for new chain".to_string()));
                }
                if token_name.len() != 3 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Invalid name for chain".to_string()));
                }
            }
            "mode" => {
                if input.tokens.len() != 3 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for new mode".to_string()));
                }
                if token_name.len() != 3 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Invalid name for mode".to_string()));
                }
            }
            "stream" => {
                if input.tokens.len() != 3 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for new stream".to_string()));
                }
                if token_name.len() != 2 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Invalid name for stream".to_string()));
                }
            }
            "application" => {
                if input.tokens.len() != 4 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for new application".to_string()));
                }
                if token_name.len() != 1 {
                    return Err(k2err!(K2ErrorCode::BadFormat, "Invalid name for application".to_string()));
                }
            }
            _ => {return Err(k2err!(K2ErrorCode::BadFormat, format!("Unknow type {}", input.tokens[1].clone())));}
        }
        let object = K2LangObject {
            name: input.tokens[2].clone(),
            object_type: input.tokens[1].clone(),
            parent: Vec::new(),
            children: Vec::new(),
            properties: HashMap::new()
        };
        output.data.push(object);
        Ok(output)
    }
    pub fn parse_add(input: &K2LangStruct) -> K2LangReturn {
        let mut output = input.clone();
        if input.tokens.len() != 3 {
            return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for add command".to_string()));
        }
        output.data.push(
            K2LangObject { 
                name: input.tokens[1].clone(), 
                object_type: "".to_string(), 
                parent: Vec::new(), 
                children: Vec::new(), 
                properties: HashMap::new() });
        output.data.push(
            K2LangObject { 
                name: input.tokens[1].clone(), 
                object_type: "".to_string(), 
                parent: Vec::new(), 
                children: Vec::new(), 
                properties: HashMap::new() });
        Ok(output)
    }
    pub fn parse_delete(input: &K2LangStruct) -> K2LangReturn {
        let mut output = input.clone();
        if input.tokens.len() != 2 {
            return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for delete command".to_string()));
        }
        output.data.push(
            K2LangObject { 
                name: input.tokens[1].clone(), 
                object_type: "".to_string(), 
                parent: Vec::new(), 
                children: Vec::new(), 
                properties: HashMap::new() });
        Ok(output)
    }
    pub fn parse_set(input: &K2LangStruct) -> K2LangReturn {
        let mut output = input.clone();
        if input.tokens.len() != 4 {
            return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for set command".to_string()));
        }
         output.data.push(
            K2LangObject { 
                name: input.tokens[1].clone(), 
                object_type: "".to_string(), 
                parent: Vec::new(), 
                children: Vec::new(), 
                properties: HashMap::new() });
        Ok(output)
    }
    pub fn parse_connect(input: &K2LangStruct) -> K2LangReturn {
        let mut output = input.clone();
        let token_name_source = Parser::check_name(&input.tokens[1].clone())?;
        let token_name_dest = Parser::check_name(&input.tokens[2].clone())?;
        if input.tokens.len() != 3 {
            return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for connect/disconnect command".to_string()));
        }
        if token_name_source.len() != 4 {
            return Err(k2err!(K2ErrorCode::InvalidOperation, "Object can not be connectted".to_string()));
        }
        if token_name_dest.len() != 4 {
            return Err(k2err!(K2ErrorCode::InvalidOperation, "Object can not be connectted".to_string()));
        }
        output.data.push(
            K2LangObject { 
                name: input.tokens[1].clone(), 
                object_type: "".to_string(), 
                parent: Vec::new(), 
                children: Vec::new(), 
                properties: HashMap::new() });
        output.data.push(
            K2LangObject { 
                name: input.tokens[2].clone(), 
                object_type: "".to_string(), 
                parent: Vec::new(), 
                children: Vec::new(), 
                properties: HashMap::new() });
        Ok(input.clone())
    }
    pub fn parse_disconnect(input: &K2LangStruct) -> K2LangReturn {
        Ok(input.clone())
    }
    pub fn parse_exec(input: &K2LangStruct) -> K2LangReturn {
        let mut output = input.clone();
        let token_name_cmd = Parser::check_name(&input.tokens[1].clone())?;
        if input.tokens.len() != 2 {
            return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for exec command".to_string()));
        }
        if token_name_cmd.len() != 4 {
            return Err(k2err!(K2ErrorCode::InvalidOperation, "Object can not be command".to_string()));
        }
        output.data.push(
            K2LangObject { 
                name: input.tokens[1].clone(), 
                object_type: "".to_string(), 
                parent: Vec::new(), 
                children: Vec::new(), 
                properties: HashMap::new() });
        Ok(output)
    }
    pub fn parse_code(input: &K2LangStruct) -> K2LangReturn {
        let mut output = input.clone();
        let token_name_cmd = Parser::check_name(&input.tokens[1].clone())?;
        if input.tokens.len() != 4 {
            return Err(k2err!(K2ErrorCode::BadFormat, "Wrong number of parameter for exec command".to_string()));
        }
        if token_name_cmd.len() != 2 {
            return Err(k2err!(K2ErrorCode::InvalidOperation, "Object can not be command".to_string()));
        }
        output.data.push(
            K2LangObject { 
                name: input.tokens[1].clone(), 
                object_type: "".to_string(), 
                parent: Vec::new(), 
                children: Vec::new(), 
                properties: HashMap::new() });
        Ok(output)
    }
    pub fn parse_command(&self, command: &String, token: &Vec<String>) -> K2LangReturn {
        if token.len() < 1 {
            return Err(k2err!(K2ErrorCode::BadFormat, "Empty command".to_string()));
        }
        let key_word = token[0].clone();
        let ok_response = K2LangStruct {
            command: command.clone(),
            message: "".to_string(),
            tokens: token.clone(),
            data: Vec::new(),
        };
        if let Some(callback) = self.callbacks_cmd.get(&key_word) {
            return callback(&ok_response);
        } else {
            return Err(k2err!(K2ErrorCode::InvalidValue, format!("Keyword {} not valid", key_word)));
        }

    }
}
impl ProcessorTrait for Parser {
    fn new(name: String) -> ProcessorNewReturn {
        let mut callbacks_cmd: HashMap<String, K2ParserCallback> = HashMap::new();
        callbacks_cmd.insert("new".to_string(), Parser::parse_new);
        callbacks_cmd.insert("add".to_string(), Parser::parse_add);
        callbacks_cmd.insert("delete".to_string(), Parser::parse_delete);
        callbacks_cmd.insert("set".to_string(), Parser::parse_set);
        callbacks_cmd.insert("connect".to_string(), Parser::parse_connect);
        callbacks_cmd.insert("disconnect".to_string(), Parser::parse_connect);
        callbacks_cmd.insert("exec".to_string(), Parser::parse_exec);
        callbacks_cmd.insert("code".to_string(), Parser::parse_code);
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
        self_instance.get_stream_block_mut().add_input::<String>("command".to_string())?;
        self_instance.get_stream_block_mut().add_output::<K2LangReturn>("response".to_string())?;
        Ok(Box::new(self_instance))
    }
    fn initialize(&mut self) -> Result<(), K2Error> {
        let mut state = self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to set state"))?;
        *state = StreamState::Initialized;
        Ok(())
    }
    fn process(&mut self) -> Result<(), K2Error> {
        let command_input = self.stream_block.get_input::<String>(&"command".to_string())?;
        let command_str = command_input.receive()?;
        let tokenized_commands = Self::split_commands(&command_str);
        for cmd in &tokenized_commands {
            if cmd.is_empty() {
                continue;
            }
            let response = self.parse_command(&command_str, cmd);
            let response_output = self.stream_block.get_output::<K2LangReturn>(&"response".to_string())?;
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