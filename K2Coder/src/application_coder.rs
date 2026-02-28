use std::collections::HashMap;

use k2_lang::{K2Object, K2ReturnStruct};

use crate::coder::{CARGO_IF, CoderTrait};

pub struct ApplicationCoder {
    name: String,
    application_path: String,
    path: String,
    streaming: HashMap<String, HashMap<String, K2Object>>,
    count_connection: usize,
}

impl CoderTrait for ApplicationCoder {
    fn new(name: String, object: &K2Object) -> Result<Box<dyn CoderTrait>, String> where Self: Sized {
        let application_path = object.properties.get(&"path".to_string()).ok_or("Path not present".to_string())?;
        let mut instance = Self {
            name,
            application_path: application_path.clone(),
            path: "".to_string(),
            streaming: HashMap::new(),
            count_connection: 0,
        };
        instance.set_parent_directory(application_path.clone());
        // Create project with CARGO_IF
        let cargo_if = CARGO_IF.get().ok_or("Cargo interface not setted".to_string())?;
        cargo_if.cargo_new_application(application_path.clone())?;
        Ok(Box::new(instance))
    }
    fn get_name(&self) -> String {
        self.name.clone()
    }
    fn get_path(&self) -> String {
        self.path.clone()
    }
    fn set_parent_directory(&mut self, path: String) {
        self.path = format!("{}/src/main.rs", path);
    }
    fn proc_new(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String> {
        let object = k2_struct.data.get(0).ok_or("Missing data")?;
        let object_name = object.name.clone();
        let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
        match split_name.len() {
            2 => {
                // New stream
                if self.streaming.contains_key(&object_name) {
                    return Err(format!("Object {} already exists", object_name));
                }
                self.streaming.insert(object_name, HashMap::new());
            }
            3 => {
                let stream_name = format!("{}.{}", split_name[0], split_name[1]);
                if !self.streaming.contains_key(&stream_name) {
                    return Err(format!("Stream {} does not exists", stream_name));
                }
                let stream_table = self.streaming.get_mut(&stream_name).unwrap();
                if stream_table.contains_key(&object_name) {
                    return Err(format!("Object {} already exists", object_name));
                }
                stream_table.insert(object_name, object.clone());
            }
            _ => {
                return Err("Not valid name".to_string())
            }
        }
        Ok(format!("Object {} created", object.name.clone()))
    }
    fn proc_add(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String> {
        Ok(k2_struct.message.clone())
    }
    fn proc_delete(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String> {
        let object = k2_struct.data.get(0).ok_or("Missing data")?;
        let object_name = object.name.clone();
        let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
        match split_name.len() {
            1 => {
                if object_name != self.name {
                    return Err(format!("{} can not delete {}", self.name, object_name));
                }
                std::fs::remove_file(self.path.clone()).map_err(|_| "Error in deleting object file")?;
            }
            2 => {
                let stream_name = format!("{}.{}", split_name[0], split_name[1]);
                self.streaming.remove(&stream_name);
            }
            3 => {
                let stream_name = format!("{}.{}", split_name[0], split_name[1]);
                if !self.streaming.contains_key(&stream_name) {
                    return Err(format!("Stream {} does not exists", stream_name));
                }
                self.streaming.get_mut(&stream_name).unwrap().remove(&object_name);
            }
            _ => {
            return Err(format!("Invalid name format"));
            }
        }
        Ok(format!("Object {} created", object.name.clone()))
    }
    fn proc_set(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String> {
        let object = k2_struct.data.get(0).ok_or("Missing data")?;
        let object_name = object.name.clone();
        let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
        if split_name.len() != 4 {
            return Err(format!("Invalid name format"));
        }
        if object.object_type != "parameter" || object.object_type != "state"  {
            return Err(format!("Object {} not settable", object_name));
        }
        let stream_name = format!("{}.{}", split_name[0], split_name[1]);
        if !self.streaming.contains_key(&stream_name) {
            return Err(format!("Stream {} does not exists", stream_name));
        }
        let stream_table = self.streaming.get_mut(&stream_name).unwrap();
        if !stream_table.contains_key(&object_name) {
            return Err(format!("Object {} does not exists", object_name));
        }
        let processor_name = format!("{}.{}", stream_name, split_name[2]);
        match k2_struct.tokens.len() {
            3 => {
                stream_table.get_mut(&processor_name)
                    .ok_or(format!("Processor {} not found", processor_name))?
                    .properties.insert(object_name, k2_struct.tokens[2].clone());
            }
            4 => {
                let mode_name = k2_struct.tokens[2].clone();
                stream_table.get_mut(&mode_name)
                    .ok_or(format!("Mode {} not found", mode_name))?
                    .properties.insert(object_name, k2_struct.tokens[3].clone());
            }
            _ => {
                return Err("Wrong parameter number".to_string());
            }
        }
        Ok(format!("Object {} set", object.name.clone()))
    }
    fn proc_connect(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String> {
        if k2_struct.tokens.len() != 3 {
            return Err("Wrong parameter number".to_string());
        }
        let connector_from = k2_struct.tokens[1].clone();
        let object_from = k2_struct.data.get(0).ok_or("Missing source")?;
        if object_from.object_type != "output" {
            return Err(format!("{} is not an output", connector_from));
        }
        let connector_to = k2_struct.tokens[2].clone();
        let object_to = k2_struct.data.get(1).ok_or("Missing destination")?;
        if object_to.object_type != "input" {
            return Err(format!("{} is not an input", connector_to));
        }
        let (processor_from, _) = connector_from.rsplit_once('.').ok_or("Split err")?;
        let (processor_to, _) = connector_to.rsplit_once('.').ok_or("Split err")?;
        let (streaming_from, _) = processor_from.rsplit_once('.').ok_or("Split err")?;
        let (streaming_to, _) = processor_to.rsplit_once('.').ok_or("Split err")?;
        if streaming_from != streaming_to {
            return Err("Connection are allowed only inside same stream".to_string());
        }
        let streaming_table = self.streaming.get_mut(&streaming_from.to_string()).ok_or(format!("Stream {} not found", streaming_from))?;
        if !streaming_table.contains_key(processor_from) {
            return Err(format!("Processor {} not found", processor_from));
        }
        if !streaming_table.contains_key(processor_to) {
            return Err(format!("Processor {} not found", processor_to));
        }
        streaming_table.get_mut(processor_from).unwrap().properties.insert(
            format!("connection_{}", self.count_connection),format!("{}-{}",connector_from.clone(), connector_to.clone()));
        self.count_connection = self.count_connection + 1;
        Ok(format!("{} connected to {}", connector_from, connector_to))
    }
    fn proc_disconnect(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String> {
        if k2_struct.tokens.len() != 3 {
            return Err("Wrong parameter number".to_string());
        }
        let connector_from = k2_struct.tokens[1].clone();
        let connector_to = k2_struct.tokens[2].clone();
        let (processor_from, _) = connector_from.rsplit_once('.').ok_or("Split err")?;
        let (streaming_from, _) = processor_from.rsplit_once('.').ok_or("Split err")?;
        let streaming_table = self.streaming.get_mut(&streaming_from.to_string()).ok_or(format!("Stream {} not found", streaming_from))?;
        if !streaming_table.contains_key(processor_from) {
            return Err(format!("Processor {} not found", processor_from));
        }
        let connection = format!("{}-{}",connector_from.clone(), connector_to.clone());
        streaming_table.get_mut(processor_from).unwrap().properties.retain(|_, v| v == &connection);
        Ok(format!("Connection removed"))
    }
    fn proc_exec(&mut self, _k2_struct: &K2ReturnStruct) -> Result<String, String> {
        Err("Exec not supported by K2Coder".to_string())
    }
    fn generate(&self) -> Result<String, String> {
        Ok(format!("Application {} code generate with success", self.name.clone()))
    }
}