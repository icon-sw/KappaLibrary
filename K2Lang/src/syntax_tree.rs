use std::{collections::HashMap, sync::OnceLock};


use std::{sync::{Arc, Mutex, MutexGuard}};

use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;
use k2_stream::{errors::{K2Error, K2ErrorCode}, k2err, processor::memory::{DataHeader, MemoryTrait}, processor::processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState}};
use crate::{K2Object, K2ReturnStruct};

type SyntaxTreeReturn = Result<K2ReturnStruct, String>;
type SyntaxTreeCallback = fn(&mut SyntaxTreeProcessor, &K2ReturnStruct) -> SyntaxTreeReturn;

static PARENT_RELATIONSHIP: OnceLock<HashMap<String, String>> = OnceLock::new();
static ADD_RELATIONSHIP: OnceLock<HashMap<String, String>> = OnceLock::new();
static CALLBACKS: OnceLock<HashMap<String, SyntaxTreeCallback>> = OnceLock::new();

#[derive(K2Memory, K2ProcessorBlock)]
pub struct SyntaxTreeProcessor {
    // fields for the parser
    pub name: String,
    pub header: ProcessorHeader,
    stream_block: StreamBlock,
    state: Arc<Mutex<StreamState>>,
    objects: HashMap<String, K2Object>,
}

impl ProcessorTrait for SyntaxTreeProcessor {
    fn new(name: String) -> ProcessorNewReturn {
        let mut self_instance = Self {
            name: name.clone(),
            header: ProcessorHeader {
                proc_name: "SyntaxTree".to_string(),
                description: "The syntax tree processor".to_string(),
                version: "0.1.0".to_string(),
                author: "Sofia".to_string(),
                email: "ms.sofia.silvestri@gmail.com".to_string(),
                license: "LGPLv2.0".to_string(),
                repository: "".to_string(),
            },
            stream_block: StreamBlock::new(),
            state: Arc::new(Mutex::new(StreamState::Uninitialized)),
            objects: HashMap::new(),
        };
        self_instance.stream_block.add_input::<K2ReturnStruct>("command".to_string())?;
        self_instance.stream_block.add_output::<K2ReturnStruct>("response".to_string())?;
        Ok(Box::new(self_instance))
    }
    fn initialize(&mut self ) -> Result<(), K2Error> {
        PARENT_RELATIONSHIP.get_or_init(|| {
            let mut table = HashMap::new();
            table.insert("library".to_string(), "".to_string());
            table.insert("processor".to_string(), "library".to_string());
            table.insert("input".to_string(), "processor".to_string());
            table.insert("output".to_string(), "processor".to_string());
            table.insert("parameter".to_string(), "processor".to_string());
            table.insert("state".to_string(), "processor".to_string());
            table.insert("command".to_string(), "processor".to_string());
            table.insert("application".to_string(), "".to_string());
            table.insert("streaming_control".to_string(), "application".to_string());
            table.insert("block".to_string(), "streaming_control".to_string());
            table.insert("mode".to_string(), "streaming_control".to_string());
            table.insert("chain".to_string(), "streaming_control".to_string());
            table
        });
        ADD_RELATIONSHIP.get_or_init(|| {
            let mut table = HashMap::new();
            table.insert("block".to_string(), "chain".to_string());
            table.insert("chain".to_string(), "mode".to_string());
            table
        });
        CALLBACKS.get_or_init(|| {
            let mut table: HashMap<String, fn(&mut SyntaxTreeProcessor, &K2ReturnStruct) -> Result<K2ReturnStruct, String>> = HashMap::new();
            table.insert("new".to_string(), Self::parse_new);
            table.insert("add".to_string(), Self::parse_add);
            table.insert("delete".to_string(), Self::parse_delete);
            table.insert("set".to_string(), Self::parse_set);
            table.insert("connect".to_string(), Self::parse_connect);
            table.insert("disconnect".to_string(), Self::parse_disconnect);
            table.insert("exec".to_string(), Self::parse_exec);
            table
        });
        Ok(())
    }
    fn process(&mut self) -> Result<(), K2Error> {
        *self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to update status"))? = StreamState::Running;
        let command_input = self.stream_block.get_input::<K2ReturnStruct>(&"command".to_string())?;
        let k2_parse_struct = command_input.receive()?;
        let response = self.parse_command(&k2_parse_struct).map_err(|_| k2err!(K2ErrorCode::InvalidOperation,""))?;
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

impl SyntaxTreeProcessor {
    fn valid_object(&self, object_name: String) -> Result<bool, String> {
        if !object_name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.') {
            return Err("Invalid object name".to_string());
        }
        let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
        for string in split_name.clone() {
            if string.is_empty() {
                return Err("Invalid object name".to_string());
            }
        }
        if self.objects.contains_key(&object_name) {
            Ok(true)
        } else {
            Ok(false)
        }
    }
    fn valid_parent(&self, object_name: String, object_type: String) -> Result<String, String>{
        if object_type == "library" || object_type == "application" {
            return Ok("".to_string());
        }
        if let Some((parent_name, _)) = object_name.rsplit_once('.') {
            let parent_name = parent_name.to_string();
            self.valid_object(parent_name.clone())?;
            let parent_object = self.objects.get(&parent_name.clone()).ok_or(format!("Object {} not found", parent_name.clone()))?;
            let relationship_table = PARENT_RELATIONSHIP.get().ok_or("SyntaxTree not initialized".to_string())?;
            if let Some(parent_type) = relationship_table.get(&object_type) {
                if parent_type == &parent_object.object_type {
                    Ok(parent_name)
                } else {
                    Err(format!("{} type cannot be created inside {}", object_type, parent_object.object_type))
                }
            } else {
                Err(format!("Unkonw type {}", object_type))
            }
        } else {
            Err("Invalid parent name".to_string())
        }
        
    }
    fn add_children(&mut self, parent_name: String, children_name: String) -> Result<(),String>{
        let mut parent_object = self.objects.get(&parent_name).ok_or(format!("Object {} not found", parent_name))?.clone();
        parent_object.children.push(children_name.clone());
        self.objects.insert(parent_name.clone(), parent_object);
        let mut children_object = self.objects.get(&children_name).ok_or(format!("Object {} not found", children_name))?.clone();
        children_object.parent.push(parent_name);
        self.objects.insert(children_name.clone(), children_object);
        Ok(())
    }
    fn get_children(&self, object_name: String) -> Result<Vec<String>, String> {
        let children = self.objects
            .get(&object_name.clone())
            .ok_or(format!("Object {} not found", object_name.clone()))?
            .children.clone();
        Ok(children)
    }
    fn parse_new(&mut self, k2_parse_struct: &K2ReturnStruct) -> SyntaxTreeReturn {
        let object_type = k2_parse_struct.tokens[1].clone();
        let object_name = k2_parse_struct.tokens[2].clone();
        let mut processing_object = K2Object {
            name: object_name.to_string(),
            object_type: object_type.to_string(),
            parent: Vec::new(),
            children: Vec::new(),
            properties: HashMap::new(),
        };
        match self.valid_object(object_name.clone()) {
            Ok(present) => {
                if present {
                    return Err("Object with same name already exists!".to_string());
                }
            }
            Err(e) => {return Err(e);}
        }
        match self.valid_parent(object_name.clone(), object_type.clone()) {
            Ok(parent_name) => {
                let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
                match object_type.as_str() {
                    "input" | "output" => {
                        if split_name.len() != 3 {
                            return Err("Invalid object name format for input/output".to_string());
                        }
                        if k2_parse_struct.tokens.len() != 4 || k2_parse_struct.tokens[3].is_empty() {
                            return Err("Data type cannot be empty".to_string());
                        }
                        processing_object.properties.insert("type".to_string(), k2_parse_struct.tokens[3].clone());
                    },
                    "state" => {
                        if split_name.len() != 3 {
                            return Err("Invalid object name format for state".to_string());
                        }
                        if k2_parse_struct.tokens.len() != 5 {
                            return Err("Data type and data value cannot be empty".to_string());
                        }
                        processing_object.properties.insert("type".to_string(), k2_parse_struct.tokens[3].clone());
                        processing_object.properties.insert("value".to_string(), k2_parse_struct.tokens[4].clone());
                    },
                    "parameter" => {
                        if split_name.len() != 3 {
                            return Err("Invalid object name format for parameter".to_string());
                        }
                        if k2_parse_struct.tokens.len() != 6 {
                            return Err("Invalid argument".to_string());
                        }
                        if k2_parse_struct.tokens[5] != "static" && k2_parse_struct.tokens[5] != "dynamic" {
                            return Err("Parameter can be static or dynamic".to_string());
                        }
                        processing_object.properties.insert("type".to_string(), k2_parse_struct.tokens[3].clone());
                        processing_object.properties.insert("value".to_string(), k2_parse_struct.tokens[4].clone());
                        processing_object.properties.insert("kind".to_string(), k2_parse_struct.tokens[5].clone());
                    },
                    "command" => {
                        if split_name.len() != 3 {
                            return Err("Invalid object name format for command".to_string());
                        }
                        if k2_parse_struct.tokens.len() != 4 {
                            return Err("Invalid argument".to_string());
                        }
                        processing_object.properties.insert("callback".to_string(), k2_parse_struct.tokens[3].clone());
                    },
                    "processor" => {
                        if split_name.len() != 2 {
                            return Err("Invalid object name format for processor".to_string());
                        }
                        if k2_parse_struct.tokens.len() != 3 {
                            return Err("Invalid argument".to_string());
                        }
                    },
                    "block" => {
                        if split_name.len() != 3 {
                            return Err("Invalid object name format for block".to_string());
                        }
                        if k2_parse_struct.tokens.len() != 4 {
                            return Err("Invalid argument".to_string());
                        }
                        let found = self.valid_object(k2_parse_struct.tokens[3].clone())?;
                        if !found {
                            return Err(format!("Processor {} not found", k2_parse_struct.tokens[3]));
                        }
                        let processor_children = self.get_children(k2_parse_struct.tokens[3].clone())?;
                        for child in processor_children {
                            let mut children_object = self.objects.get(&child).ok_or(format!("Object {} not found", child))?.clone();
                            let child = child.split('.').last().ok_or("err")?.to_string();
                            children_object.name = format!("{}.{}", object_name.clone(), child);
                            children_object.parent = Vec::new();
                            if self.objects.contains_key(&children_object.clone().name) {
                                return Err(format!("Object {} already exists", children_object.clone().name));
                            }
                            processing_object.children.push(children_object.clone().name);
                            children_object.parent.push(object_name.clone());
                            self.objects.insert(children_object.clone().name, children_object);
                        }
                        processing_object.properties.insert("type".to_string(), k2_parse_struct.tokens[3].clone());
                        processing_object.properties.insert("connection".to_string(), "0".to_string());
                    },
                    "chain" | "mode" => {
                        if split_name.len() != 3 {
                            return Err("Invalid object name format for chain/mode".to_string());
                        }
                        if k2_parse_struct.tokens.len() != 3 {
                            return Err("Invalid argument".to_string());
                        }
                    },
                    "streaming_control" => {
                        if split_name.len() != 2 {
                            return Err("Invalid object name format for streaming_control".to_string());
                        }
                        if k2_parse_struct.tokens.len() != 3 {
                            return Err("Invalid argument".to_string());
                        }
                    },
                    "application" | "library" => {
                        if split_name.len() != 1 {
                            return Err("Invalid object name format for library/application".to_string());
                        }
                        if k2_parse_struct.tokens.len() != 4 {
                            return Err("Invalid argument".to_string());
                        }
                        processing_object.properties.insert("path".to_string(), k2_parse_struct.tokens[3].clone());
                    },
                    _ => {return  Err(format!("Unknow type {}", object_type));}
                    
                }
                self.objects.insert(object_name.clone(), processing_object.clone());
                if object_type.clone() != "library" && object_type != "application" {
                    self.add_children(parent_name, object_name)?;
                }
            }
            Err(e) => {return Err(e);}
        }
        Ok(
            K2ReturnStruct {
                success: true,
                command: k2_parse_struct.command.clone(),
                tokens: k2_parse_struct.tokens.clone(),
                message: "New".to_string(),
                data: vec![processing_object],
        })
    }
    fn parse_add(&mut self, k2_parse_struct: &K2ReturnStruct) -> SyntaxTreeReturn {
        if k2_parse_struct.tokens.len() != 3 {
            return Err("Invalid argument".to_string());
        }
        let object_name = k2_parse_struct.tokens[1].clone();
        let relation_name = k2_parse_struct.tokens[2].clone();
        if !self.valid_object(object_name.clone())? {
            return Err(format!("Object {} not present", object_name));
        }
        if !self.valid_object(relation_name.clone())? {
            return Err(format!("Object {} not present", relation_name));
        }
        let object = self.objects.get(&object_name).unwrap().clone();
        let relation_object = self.objects.get(&relation_name).unwrap().clone();
        let relationship_table = ADD_RELATIONSHIP.get().ok_or("SyntaxTree not initialized".to_string())?;
        if let Some(parent_type) = relationship_table.get(&object.object_type) {
            if &relation_object.object_type == parent_type {
                self.add_children(relation_name.clone(), object_name.clone())?;
            } else {
                return Err(format!("{} cannot be parent of {}", relation_object.object_type, object.object_type));
            }
        } else {
            return Err(format!("{} cannot be had to anything", object.object_type));
        }
        Ok(
            K2ReturnStruct {
                success: true,
                command: k2_parse_struct.command.clone(),
                tokens: k2_parse_struct.tokens.clone(),
                message: "Add".to_string(),
                data: vec![relation_object.clone(), object.clone()],
        })
    }
    fn parse_delete(&mut self, k2_parse_struct: &K2ReturnStruct) -> SyntaxTreeReturn {
        if k2_parse_struct.tokens.len() < 2 {
            return Err("Invalid k2_parse_struct.tokens length".to_string());
        }
        let object_name = k2_parse_struct.tokens[1].clone();
        if !self.valid_object(object_name.clone())? {
            return Err("Object does not exist".to_string());
        }
        let object = self.objects.get_mut(&object_name.clone()).unwrap().clone();
        for parent_name in &object.parent {
            if let Some(parent_object) = self.objects.get_mut(parent_name) {
                parent_object.children.retain(|child| child != &object_name);
                if parent_object.object_type == "mode".to_string() {
                    parent_object.properties.retain(|k, _| k != &object_name);
                }
            }
        }
        let mut deleted_objects = vec![object.clone()];
        deleted_objects.push(object.clone());
        self.objects.remove(&object_name);
        for child_name in &object.children {
            if let Some(child_object) = self.objects.get_mut(child_name) {
                child_object.parent.retain(|parent| parent != &object_name);
                if child_object.parent.is_empty() {
                    deleted_objects.push(child_object.clone());
                    self.objects.remove(child_name);
                }
            }
        }
        Ok(
            K2ReturnStruct {
                success: true,
                command: k2_parse_struct.command.clone(),
                tokens: k2_parse_struct.tokens.clone(),
                message: "delete".to_string(),
                data: deleted_objects,
        })
    }
    fn parse_set(&mut self, k2_parse_struct: &K2ReturnStruct) -> SyntaxTreeReturn {
        if k2_parse_struct.tokens.len() < 3{
            return Err("".to_string());
        }
        let object_name = k2_parse_struct.tokens[1].clone();
        let mut object = self.objects.get(&object_name).ok_or("Object not found".to_string())?.clone();
        match object.object_type.as_str() {
            "parameter" | "state" => {
                match k2_parse_struct.tokens.len() {
                    3 => { // Global set
                        object.properties.insert("value".to_string(), k2_parse_struct.tokens[2].clone());
                    },
                    4 => { // Mode set
                        let mode_selected = k2_parse_struct.tokens[2].clone();
                        if !self.valid_object(mode_selected.clone())? {
                            return Err(format!("Mode {} not exist", mode_selected));
                        }
                        let mode_obj = self.objects.get_mut(&mode_selected).ok_or("Object not found".to_string())?;
                        if mode_obj.object_type != "mode" {
                            return Err(format!("{} is not a mode", mode_selected));
                        }
                        mode_obj.properties.insert(object_name, k2_parse_struct.tokens[3].clone());
                        object.parent.push(mode_selected);
                    },
                    _ => {return Err("Wrong command length".to_string());},
                }
            },
             "processor" => {
                if k2_parse_struct.tokens.len() != 4 {
                    return Err("Wrong command length".to_string());
                }
                let code_part = k2_parse_struct.tokens[2].clone();
                object.properties.insert(code_part, k2_parse_struct.tokens[3].clone());
            }
            _ => {return Err(format!("Type {} is not settable", object.object_type));}
        }
        Ok(
            K2ReturnStruct {
                success: true,
                command: k2_parse_struct.command.clone(),
                tokens: k2_parse_struct.tokens.clone(),
                message: "set".to_string(),
                data: vec![object.clone()],
        })
    }
    fn parse_connect(&mut self, k2_parse_struct: &K2ReturnStruct) -> SyntaxTreeReturn {
        if k2_parse_struct.tokens.len() != 3 {
            return Err("Invalid command size".to_string());
        }
        if !self.valid_object(k2_parse_struct.tokens[1].clone())? {
            return Err(format!("Connector {} does not exists", k2_parse_struct.tokens[1]));
        }
        if !self.valid_object(k2_parse_struct.tokens[2].clone())? {
            return Err(format!("Connector {} does not exists", k2_parse_struct.tokens[2]));
        }
        
        let source_name = k2_parse_struct.tokens[1].clone();
        let target_name = k2_parse_struct.tokens[2].clone();
        let source_obj = self.objects.get(&source_name).unwrap().clone();
        let target_obj = self.objects.get(&target_name).unwrap().clone();
        if source_obj.object_type != "output" && target_obj.object_type != "input" {
            return Err("Connection invalid type.".to_string());
        }
        let (source_parent_name, _) = source_name.rsplit_once('.').ok_or("Split err")?;
        let source_parent_name = source_parent_name.to_string();
        let mut source_parent_obj = self.objects.get_mut(&source_parent_name.clone()).unwrap().clone();
        if source_parent_obj.object_type != "block" {
            return Err("Invalid parent type for connection".to_string());
        }
        let (target_parent_name, _) = target_name.rsplit_once('.').ok_or("Split err")?;
        let target_parent_name = target_parent_name.to_string();
        let target_parent_obj = self.objects.get(&target_parent_name.clone()).unwrap().clone();
        if target_parent_obj.object_type != "block" {
            return Err("Invalid parent type for connection".to_string());
        }
        
        let connection_number = source_parent_obj.properties.get("connection").unwrap_or(&"0".to_string()).parse::<u32>().unwrap_or(0) + 1;
        source_parent_obj.properties.insert(format!("connection_{}", connection_number), format!("{}-{}", source_name, target_name));
        source_parent_obj.properties.insert("connection".to_string(), connection_number.to_string());
        self.objects.insert(source_parent_name, source_parent_obj.clone());
        Ok(
            K2ReturnStruct {
                success: true,
                command: k2_parse_struct.command.clone(),
                tokens: k2_parse_struct.tokens.clone(),
                message: "connect".to_string(),
                data: vec![source_parent_obj.clone()],
        })
    }
    fn parse_disconnect(&mut self, k2_parse_struct: &K2ReturnStruct) -> SyntaxTreeReturn {
        if k2_parse_struct.tokens.len() != 3 {
            return Err("Invalid command size".to_string());
        }
        if !self.valid_object(k2_parse_struct.tokens[1].clone())? {
            return Err(format!("Connector {} does not exists", k2_parse_struct.tokens[1]));
        }
        if !self.valid_object(k2_parse_struct.tokens[2].clone())? {
            return Err(format!("Connector {} does not exists", k2_parse_struct.tokens[2]));
        }
        
        let source_name = k2_parse_struct.tokens[1].clone();
        let target_name = k2_parse_struct.tokens[2].clone();
        let source_obj = self.objects.get(&source_name).unwrap().clone();
        let target_obj = self.objects.get(&target_name).unwrap().clone();
        if source_obj.object_type != "output" && target_obj.object_type != "input" {
            return Err("Connection invalid type.".to_string());
        }
        let (source_parent_name, _) = source_name.rsplit_once('.').ok_or("Split err")?;
        let source_parent_name = source_parent_name.to_string();
        let mut source_parent_obj = self.objects.get_mut(&source_parent_name.clone()).unwrap().clone();
        if source_parent_obj.object_type != "block" {
            return Err("Invalid parent type for connection".to_string());
        }
        let (target_parent_name, _) = target_name.rsplit_once('.').ok_or("Split err")?;
        let target_parent_name = target_parent_name.to_string();
        let target_parent_obj = self.objects.get(&target_parent_name.clone()).unwrap().clone();
        if target_parent_obj.object_type != "block" {
            return Err("Invalid parent type for connection".to_string());
        }
        let properties = source_parent_obj.properties.clone();
        if let Some(connection) = properties.iter().find(|(_, v)| v == &&format!("{}-{}", source_name, target_name)) {
            source_parent_obj.properties.remove(connection.0);
            let connection_number = source_parent_obj.properties.get("connection").unwrap_or(&"0".to_string()).parse::<i32>().unwrap_or(0) - 1;
            if connection_number >= 0 {
                source_parent_obj.properties.insert("connection".to_string(), connection_number.to_string());
            }
            return Ok(
                K2ReturnStruct {
                    success: true,
                    command: k2_parse_struct.command.clone(),
                    tokens: k2_parse_struct.tokens.clone(),
                    message: "Disconnect".to_string(),
                    data: vec![source_parent_obj.clone()],
            })
        } else {
            return Err("Connection does not exist".to_string());
        }
    }
    fn parse_exec(&mut self, k2_parse_struct: &K2ReturnStruct) -> SyntaxTreeReturn {
        if k2_parse_struct.tokens.len() < 2 {
            return Err("Invalid k2_parse_struct.tokens length".to_string());
        }
        if !self.valid_object(k2_parse_struct.tokens[1].clone())? {
            return Err("Command does not exist".to_string());
        }
        Ok(k2_parse_struct.clone())
    }
    pub fn parse_command(&mut self, k2_parse_struct: &K2ReturnStruct) -> SyntaxTreeReturn {
        let mut response = k2_parse_struct.clone();
        if k2_parse_struct.success == false {
            return Ok(response);
        }
        let callbacks = CALLBACKS.get().ok_or("Uninitialized map".to_string())?;
        if let Some(callback) = callbacks.get(k2_parse_struct.tokens[0].clone().as_str()) {
            match callback(self, &k2_parse_struct) {
                Ok(res) => {
                    response = res;
                }
                Err(e) => {
                    response = K2ReturnStruct {
                        success: false,
                        command: k2_parse_struct.command.clone(),
                        tokens: k2_parse_struct.tokens.clone(),
                        message: e,
                        data: Vec::new(),
                    };
                }
            }
        } else {
        }
        Ok(response)
    }
}


#[cfg(test)]
mod test
{
    use std::{fs::File, io::{BufRead, BufReader}, sync::mpsc};

    use super::*;
    #[test]
    fn syntax_tree_new_test() {
        let syntax_tree_proc = SyntaxTreeProcessor::new("test_ast".to_string());
        assert!(syntax_tree_proc.is_ok());
        let mut syntax_tree_proc = syntax_tree_proc.unwrap();
        let syntax_tree_proc = syntax_tree_proc.as_any_mut().downcast_mut::<SyntaxTreeProcessor>().unwrap();

        let ( sender,  receiver) = mpsc::sync_channel::<K2ReturnStruct>(10);
        let response_port = syntax_tree_proc.get_stream_block_mut().get_output_mut::<K2ReturnStruct>(&"response".to_string());
        assert!(response_port.is_ok());
        let response_port = response_port.unwrap();
        response_port.connect(sender);

        let command_port = syntax_tree_proc.get_stream_block_mut().get_input::<K2ReturnStruct>(&"command".to_string());
        assert!(command_port.is_ok());
        let command_port = command_port.clone().unwrap().get_sender().clone();

        assert!(syntax_tree_proc.initialize().is_ok());
        let file = File::open("test/ok_command_sequence").map_err(|_| k2err!(K2ErrorCode::NotFound,""));
        let reader = BufReader::new(file.unwrap());
        for line in reader.lines() {
            assert!(line.is_ok());
            let line = line.unwrap();
            dbg!(line.clone());
            if line.is_empty() {
                continue;
            }
            let tokens: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
            let message = K2ReturnStruct {
                success: true,
                command: line.clone(),
                message: "Ok".to_string(),
                tokens: tokens,
                data: Vec::new(),
            };
            assert!(command_port.send(message).is_ok());
            assert!(syntax_tree_proc.process().is_ok());
            let response = receiver.recv();
            assert!(response.is_ok());
            let response = response.unwrap();
            dbg!(response.clone());
            assert!(response.success);
        }
    }
    #[test]
    fn ast_add_test() {

    }
    #[test]
    fn ast_delete_test() {

    }
    #[test]
    fn ast_set_test() {

    }
    #[test]
    fn ast_connect_disconnect_test() {

    }
    #[test]
    fn ast_exec_test() {

    }
}