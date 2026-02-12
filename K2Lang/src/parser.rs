use std::{collections::HashMap, hash::Hash, sync::{Arc, Mutex, MutexGuard}};

use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;
use k2_stream::{memory::{DataHeader, MemoryTrait}, processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorTrait, StreamBlock, StreamState}};

static COMMANDS: [&str; 9] = ["new", "add", "delete", "set", "connect", "disconnect", "init", "run", "stop"];
static OBJECTS: [&str; 10] = ["input", "output", "parameter", "state", "processor", "stream", "chain", "mode", "command", "code"];

type ParserReturn = Result<bool, String>;
type ParserCallback = fn(&mut Parser, &Vec<String>) -> ParserReturn;

pub struct ParseObject {
    pub name: String,
    pub object_type: String,
    pub parent: Vec<String>,
    pub children: Vec<String>,
}

#[derive(K2Memory, K2ProcessorBlock)]
pub struct Parser {
    // fields for the parser
    pub name: String,
    pub header: ProcessorHeader,
    stream_block: StreamBlock,
    state: Arc<Mutex<StreamState>>,
    objects: HashMap<String, ParseObject>,
    callbacks_cmd: HashMap<String, ParserCallback>,
}

impl Parser {
    pub fn new() -> Result<Self, ()> {
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
            callbacks_cmd: HashMap::new(),
        };
        self_instance.callbacks_cmd.insert("new".to_string(), Parser::parse_new);
        self_instance.stream_block.add_input::<String>("command".to_string())?;
        self_instance.stream_block.add_output::<ParserReturn>("response".to_string())?;
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
        if !OBJECTS.contains(&object_type.as_str()) {
            return Err("Invalid object type".to_string());
        }
        Ok(true)
    }
    pub fn parse_add(command: &Vec<String>) -> bool {
        // parse the add command and execute the corresponding action
        true
    }
    pub fn parse_delete(command: &Vec<String>) -> bool {
        // parse the delete command and execute the corresponding action
        true
    }
    pub fn parse_set(command: &Vec<String>) -> bool {
        // parse the set command and execute the corresponding action
        true
    }
    pub fn parse_connect(command: &Vec<String>) -> bool {
        // parse the connect command and execute the corresponding action
        true
    }
    pub fn parse_disconnect(command: &Vec<String>) -> bool {
        // parse the disconnect command and execute the corresponding action
        true
    }
    pub fn parse_init(command: &Vec<String>) -> bool {
        // parse the init command and execute the corresponding action
        true
    }
    pub fn parse_run(command: &Vec<String>) -> bool {
        // parse the run command and execute the corresponding action
        true
    }
    pub fn parse_stop(command: &Vec<String>) -> bool {
        // parse the stop command and execute the corresponding action
        true
    }
    pub fn parse_command(&mut self, command: &String) -> ParserReturn {
        let tokenized_commands = self.split_commands(&command);
        let mut response = Ok(true);
        for cmd in tokenized_commands {
            if cmd.is_empty() {
                continue;
            }
            if let Some(callback) = self.callbacks_cmd.get(&cmd[0]) {
                if callback(self, &cmd)? {
                    response = Ok(true);
                } else {
                    response = Ok(false);
                    break;
                }
            } else {
                response = Ok(false);
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
        loop {
            let command_input = self.stream_block.get_input::<String>(&"command".to_string())?;
            let command_str = command_input.receive()?;
            if *self.state.lock().map_err(|_| ())? == StreamState::Waiting {
                break;
            }
            let response = self.parse_command(&command_str);
            let response_output = self.stream_block.get_output::<ParserReturn>(&"response".to_string())?;
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