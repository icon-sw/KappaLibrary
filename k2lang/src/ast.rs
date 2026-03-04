use std::{collections::HashMap, sync::{Arc, Mutex, MutexGuard}};

use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;
use k2stream::{errors::{K2Error, K2ErrorCode}, k2err, processor::{memory::{DataHeader, MemoryTrait}, processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState}}};

use crate::{K2LangObject, K2LangReturn, K2LangStruct};

type K2AstCallback = fn(&mut AbstractSyntaxTree, &K2LangStruct) -> K2LangReturn;

#[derive(K2Memory, K2ProcessorBlock)]
pub struct AbstractSyntaxTree {
    // fields for the parser
    pub name: String,
    pub header: ProcessorHeader,
    stream_block: StreamBlock,
    state: Arc<Mutex<StreamState>>,
    callbacks_cmd: HashMap<String, K2AstCallback>,
    object_table: HashMap<String, K2LangObject>,
}

impl AbstractSyntaxTree {
    fn get_parent_name(child_name: String) -> Result<String, K2Error> {
        if let Some((parent_name, _)) = child_name.rsplit_once('.') {
            Ok(parent_name.to_string())
        } else {
            Err(k2err!(K2ErrorCode::BadFormat, "Bad format name"))
        }
    }
    fn check_parent_children(&self, parent_name: &String, object: &K2LangObject) -> Result<(), K2Error> {
        let parent_object = self.object_table.get(parent_name).ok_or(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", parent_name)))?;
        match parent_object.object_type.as_str() {
            "processor" => {
                match object.object_type.as_str() {
                    "input" | "output" | "parameter" | "state" | "command" => {},
                    _ => {return Err(k2err!(K2ErrorCode::InvalidOperation, format!("Type processor can not have type {} as children", object.object_type)));}
                }
            }
            "library" => {
                if object.object_type != "processor".to_string() {
                    return Err(k2err!(K2ErrorCode::InvalidOperation,format!("Type application can have only processor children")));
                }
            }
            "stream" => {
                match object.object_type.as_str() {
                    "block" | "chain" | "mode" => {},
                    _ => {return Err(k2err!(K2ErrorCode::InvalidOperation, format!("Type stream can not have type {} as children", object.object_type)));}
                }
            }
            "application" => {
                if object.object_type != "stream".to_string() {
                    return Err(k2err!(K2ErrorCode::InvalidOperation,format!("Type application can have only stream children")));
                }
            }
            _ => {
                return Err(k2err!(K2ErrorCode::InvalidOperation,format!("Type {} can not have children", parent_object.object_type)));
            }
        }
        Ok(())
    }
    fn load_block(&mut self, block: &K2LangObject, block_type: &String) -> Result<K2LangObject, K2Error> {
        let processor = self.object_table.get(block_type).ok_or(k2err!(K2ErrorCode::NotFound, format!("Type {} not found", block_type)))?;
        let children = processor.children.clone();
        let block_name = block.name.clone();
        let mut block = block.clone();
        for child in children {
            let child_name = child.split(".").last().ok_or(k2err!(K2ErrorCode::BadFormat, format!("Error in {} name", child)))?.to_string();
            let new_name = format!("{}.{}", block_name, child_name);
            block.children.push(new_name.clone());
            let children_block = self.object_table.get(&child).ok_or(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", child)))?;
            self.object_table.insert(new_name.clone(), children_block.clone());
        }
        Ok(block)
    }
    pub fn parse_new(&mut self, input: &K2LangStruct) -> K2LangReturn {
        let mut output = input.clone();
        let data = input.data.clone();
        if data.len() != 1 {
            return Err(k2err!(K2ErrorCode::InvalidOperation,"Can create an object at time".to_string()));
        }
        let mut object = data[0].clone();
        let object_name = object.name.clone();
        if self.object_table.contains_key(&object.name) {
            return Err(k2err!(K2ErrorCode::InvalidOperation,format!("Object {} already exists", object_name)));
        }
        let parent_name = AbstractSyntaxTree::get_parent_name(object_name.clone())?;
        if !self.object_table.contains_key(&parent_name) {
            return Err(k2err!(K2ErrorCode::InvalidOperation,format!("Parent {} does not exist", parent_name)));
        }
        self.check_parent_children(&parent_name, &object)?;
        let object_type = object.object_type.clone();
        match object_type.as_str() {
            "input" | "output" => {
                object.properties.insert("type".to_string(), output.tokens[3].clone());
            }
            "state" => {
                object.properties.insert("type".to_string(), output.tokens[3].clone());
                object.properties.insert("value".to_string(), output.tokens[4].clone());
            }
            "parameter" => {
                object.properties.insert("type".to_string(), output.tokens[3].clone());
                object.properties.insert("value".to_string(), output.tokens[4].clone());
                object.properties.insert("kind".to_string(), output.tokens[5].clone());
            }
            "command" => {
                object.properties.insert("callback".to_string(), output.tokens[3].clone());
            }
            "block" => {
                let block_type = output.tokens[3].clone();
                if self.object_table.contains_key(&block_type) {
                    object = self.load_block(&object, &block_type)?;
                    object.properties.insert("type".to_string(), block_type);
                } else {
                    return Err(k2err!(K2ErrorCode::InvalidValue, format!("Unknow processor {}", block_type)));
                }
            }
            "library" | "application" => {
                object.properties.insert("path".to_string(), output.tokens[3].clone());
            }
            _ => {}
        }
        let parent_object = self.object_table.get_mut(&parent_name).unwrap();        
        parent_object.children.push(object_name.clone());
        output.data = vec![parent_object.clone(), object.clone()];
        object.parent.push(parent_name);
        self.object_table.insert(object_name, object.clone());
        Ok(output)
    }

    pub fn parse_add(&mut self, input: &K2LangStruct) -> K2LangReturn {
        let mut output = input.clone();
        let children_name = input.tokens[1].clone();
        let parent_name = input.tokens[2].clone();
        if !self.object_table.contains_key(&children_name) {
            return Err(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", children_name)));
        }
        if !self.object_table.contains_key(&parent_name) {
            return Err(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", parent_name)));
        }
        let mut object_children = self.object_table.get(&children_name).unwrap().clone();
        let mut object_parent = self.object_table.get(&parent_name).unwrap().clone();
        if object_children.object_type == "block".to_string() && object_parent.object_type == "chain".to_string() {
            object_parent.children.push(children_name.clone());
            object_children.parent.push(parent_name.clone());
        } else if object_children.object_type == "chain".to_string() && object_parent.object_type == "mode".to_string() {
            object_parent.children.push(children_name.clone());
            object_children.parent.push(parent_name.clone());
        } else {
            return Err(k2err!(K2ErrorCode::InvalidOperation, format!("Cannot add {} to {}", object_children.object_type, object_parent.object_type)));
        }
        self.object_table.insert(parent_name.clone(), object_parent.clone());
        self.object_table.insert(children_name, object_children.clone());
        output.data = vec![object_parent, object_children];
        Ok(output)
    }
    pub fn parse_delete(&mut self, input: &K2LangStruct) -> K2LangReturn {
        let object_name = input.data[0].name.clone();

        if !self.object_table.contains_key(&object_name) {
            return Err(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", object_name)));
        }
        let object = self.object_table.get(&object_name).unwrap().clone();
        for child in object.children.clone() {
            let child_obj = self.object_table.get_mut(&child).ok_or(k2err!(K2ErrorCode::NotFound,format!("Object {} not found", child)))?;
            child_obj.parent.retain(|s| s != &object_name);
            if child_obj.parent.is_empty() {
                self.object_table.remove(&child);
            }
        }
        for parent in object.parent {
            let parent_obj = self.object_table.get_mut(&parent).ok_or(k2err!(K2ErrorCode::NotFound,format!("Object {} not found", parent)))?;
            parent_obj.children.retain(|s| s != &object_name);
        }
        self.object_table.remove(&object_name);
        let output = input.clone();
        Ok(output)
    }
    pub fn parse_set(&mut self, input: &K2LangStruct) -> K2LangReturn {
        let object_name = input.data[0].name.clone();
        let object = self.object_table.get_mut(&object_name).ok_or(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", object_name)))?;
        if object.object_type != "parameter".to_string() && object.object_type != "state".to_string() {
            return Err(k2err!(K2ErrorCode::InvalidOperation, format!("Type {} is not settable", object.object_type)));
        }
        object.properties.insert("value".to_string(), input.tokens[2].clone());
        let mut output = input.clone();
        output.data = vec![object.clone()];
        Ok(output)
    }
    pub fn parse_connect(&mut self, input: &K2LangStruct) -> K2LangReturn {
        let mut output = input.clone();
        let connector_1_name = input.data[0].name.clone();
        let connector_2_name = input.data[1].name.clone();
        let mut connector_1 = self.object_table.get(&connector_1_name).ok_or(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", connector_1_name)))?.clone();
        let mut connector_2 = self.object_table.get(&connector_2_name).ok_or(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", connector_2_name)))?.clone();
        if connector_1.object_type == "output".to_string() && connector_2.object_type == "input".to_string() {
            connector_1.properties.insert(connector_2_name, "connection".to_string());
            self.object_table.insert(connector_1_name, connector_1.clone());
        } else if connector_2.object_type == "output".to_string() && connector_1.object_type == "input".to_string() {
            connector_2.properties.insert(connector_1_name, "connection".to_string());
            self.object_table.insert(connector_2_name, connector_2.clone());
        } else {
            return Err(k2err!(K2ErrorCode::InvalidOperation, format!("{} and {} shall be input and output", connector_1_name, connector_2_name)));
        }
        output.data = vec![connector_1, connector_2];
        Ok(output)
    }
    pub fn parse_disconnect(&mut self, input: &K2LangStruct) -> K2LangReturn {
        let mut output = input.clone();
        let connector_1_name = input.data[0].name.clone();
        let connector_2_name = input.data[1].name.clone();
        let mut connector_1 = self.object_table.get(&connector_1_name).ok_or(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", connector_1_name)))?.clone();
        let mut connector_2 = self.object_table.get(&connector_2_name).ok_or(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", connector_2_name)))?.clone();
        if connector_1.object_type == "output".to_string() && connector_2.object_type == "input".to_string() {
            connector_1.properties.remove(&connector_2_name);
            self.object_table.insert(connector_1_name, connector_1.clone());
        } else if connector_2.object_type == "output".to_string() && connector_1.object_type == "input".to_string() {
            connector_2.properties.remove(&connector_1_name);
            self.object_table.insert(connector_2_name, connector_2.clone());
        } else {
            return Err(k2err!(K2ErrorCode::InvalidOperation, format!("{} and {} shall be input and output", connector_1_name, connector_2_name)));
        }
        output.data = vec![connector_1, connector_2];
        Ok(output)
    }
    pub fn parse_exec(&mut self, input: &K2LangStruct) -> K2LangReturn {
        let output = input.clone();
        let command_name = input.tokens[2].clone();
        if !self.object_table.contains_key(&command_name) {
            return Err(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", command_name)));
        }
        let object = self.object_table.get(&command_name).unwrap();
        if object.object_type != "command".to_string() {
            return Err(k2err!(K2ErrorCode::InvalidOperation, format!("Object {} is not a command", command_name)));
        }
        Ok(output)
    }
    pub fn parse_code(&mut self, input: &K2LangStruct) -> K2LangReturn {
        let mut output = input.clone();
        if !self.object_table.contains_key(&input.tokens[1]) {
            return Err(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", &input.tokens[1])));
        }
        let object = self.object_table.get_mut(&input.tokens[1]).unwrap();
        object.properties.insert(input.tokens[2].clone(), input.tokens[3].clone());
        output.data = vec![object.clone()];
        Ok(output)
    }
    pub fn parse_command(&mut self, input: K2LangReturn) -> K2LangReturn {
        match input {
            Ok(_) => {},
            Err(err) => {return Err(err);}
        }
        let output = input.clone().unwrap();
        if output.tokens.len() < 1 {
            return Err(k2err!(K2ErrorCode::BadFormat, "Empty command".to_string()));
        }
        let key_word = output.tokens[0].clone();
        if let Some(callback) = self.callbacks_cmd.get(&key_word) {
            return callback(self, &output);
        } else {
            return Err(k2err!(K2ErrorCode::InvalidValue, format!("Keyword {} not valid", key_word)));
        }
    }
}

impl ProcessorTrait for AbstractSyntaxTree {
    fn new(name: String) -> ProcessorNewReturn {
        let mut callbacks_cmd: HashMap<String, K2AstCallback> = HashMap::new();
        callbacks_cmd.insert("new".to_string(), AbstractSyntaxTree::parse_new);
        callbacks_cmd.insert("add".to_string(), AbstractSyntaxTree::parse_add);
        callbacks_cmd.insert("delete".to_string(), AbstractSyntaxTree::parse_delete);
        callbacks_cmd.insert("set".to_string(), AbstractSyntaxTree::parse_set);
        callbacks_cmd.insert("connect".to_string(), AbstractSyntaxTree::parse_connect);
        callbacks_cmd.insert("disconnect".to_string(), AbstractSyntaxTree::parse_disconnect);
        callbacks_cmd.insert("exec".to_string(), AbstractSyntaxTree::parse_exec);
        let mut self_instance = Self {
            name: name.clone(),
            header: ProcessorHeader {
                proc_name: "AST".to_string(),
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
            object_table: HashMap::new(),
        };
        self_instance.get_stream_block_mut().add_input::<K2LangReturn>("input".to_string())?;
        self_instance.get_stream_block_mut().add_output::<K2LangReturn>("output".to_string())?;
        Ok(Box::new(self_instance))
    }
    fn initialize(&mut self ) -> Result<(),K2Error> {
        let mut state = self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to set state"))?;
        *state = StreamState::Initialized;
        Ok(())
    }
    fn process(&mut self) -> Result<(), K2Error> {
        let command_input = self.get_stream_block_mut().receive_input::<K2LangReturn>(&"input".to_string())?;
        let output = self.parse_command(command_input);
        self.get_stream_block_mut().send_output(&"output".to_string(), output)
    }
    fn finalize(&mut self) -> Result<(), K2Error> {
        let mut state = self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to set state"))?;
        *state = StreamState::Waiting;
        Ok(())
    }
}
