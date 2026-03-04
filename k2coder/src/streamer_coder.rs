use std::collections::HashMap;

use k2lang::K2LangStruct;
use k2stream::{errors::{K2ErrorCode, K2Error}, k2err};

use crate::coder::{CoderTrait, K2CoderReturn};

#[derive(Clone)]
pub struct StreamChild {
    pub name: String,
    pub k2_type: String,
    pub block_type: Option<String>,
    pub connection: Vec<(String, String)>,
    pub children: Vec<String>,
    pub settings: HashMap<String, String>,
}
pub struct StreamerCoder {
    object_map: HashMap<String, StreamChild>,
}

impl StreamerCoder {
    pub fn new() -> Self {
        Self {
            object_map: HashMap::new(),
        }
    }
}

impl CoderTrait for StreamerCoder {
    fn execute(&mut self, input: &K2LangStruct) -> K2CoderReturn {
        match input.tokens[0].as_str() {
            "new" => {
                if self.object_map.contains_key(&input.data[0].name) {
                    return Err(k2err!(K2ErrorCode::AlreadyExists, format!("Object {} already exist", input.data[0].name)));
                }
                let mut new_object = StreamChild {
                    name: input.data[0].name.clone(),
                    k2_type: input.data[0].object_type.clone(),
                    block_type: None,
                    connection: Vec::new(),
                    settings: HashMap::new(),
                    children: Vec::new(),
                };
                match new_object.k2_type.as_str() {
                    "block" => {
                        new_object.block_type = Some(input.data[0].properties.get("type").ok_or(k2err!(K2ErrorCode::NotFound, "Type field not found"))?.clone());
                    }
                    "chain" | "mode" => {}
                    _ => {return Err(k2err!(K2ErrorCode::BadFormat, format!("Type {} not exists", new_object.k2_type)));}
                }
                self.object_map.insert(new_object.name.clone(), new_object);
            }
            "add" => {
                let child = self.object_map.get(&input.data[1].name)
                    .ok_or(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", input.data[1].name)))?
                    .clone();
                let parent = self.object_map.get_mut(&input.data[0].name)
                    .ok_or(k2err!(K2ErrorCode::NotFound, format!("Object {} not found", input.data[0].name)))?;
                if (parent.k2_type == "mode" && child.k2_type == "chain") 
                    || (parent.k2_type == "chain" && child.k2_type == "block") {
                    parent.children.push(child.name.clone());
                } else {
                    return Err(k2err!(K2ErrorCode::InvalidOperation, format!("Type {} can not be added to type {}", parent.k2_type, child.k2_type)));
                }
            }
            "delete" => {
                self.object_map.remove(&input.data[0].name);
            }
            "set" => {
                if let Some((block_name, _ ))  = input.data[0].name.rsplit_once('.') {
                    let object = self.object_map.get_mut(block_name)
                        .ok_or(k2err!(K2ErrorCode::NotFound, format!("Block {} not found", block_name)))?;
                    let value = input.data[0].properties.get("value").ok_or(k2err!(K2ErrorCode::NotFound, "Value field not found"))?;
                    object.settings.insert(input.data[0].name.clone(), value.clone());
                } else {
                    return Err(k2err!(K2ErrorCode::BadFormat, format!("Invalid name {}", input.data[0].name)));
                }
            }
            "connect" => {
                if let Some((block_name, _ ))  = input.data[0].name.rsplit_once('.') {
                    let object = self.object_map.get_mut(block_name)
                        .ok_or(k2err!(K2ErrorCode::NotFound, format!("Block {} not found", block_name)))?;
                    object.connection.push((input.data[0].name.clone(), input.data[1].name.clone()));
                } else {
                    return Err(k2err!(K2ErrorCode::BadFormat, format!("Invalid name {}", input.data[0].name)));
                }
            }
            "disconnect" => {
                if let Some((block_name, _ ))  = input.data[0].name.rsplit_once('.') {
                    let object = self.object_map.get_mut(block_name)
                        .ok_or(k2err!(K2ErrorCode::NotFound, format!("Block {} not found", block_name)))?;
                    object.connection.retain(|v| *v != (input.data[0].name.clone(), input.data[1].name.clone()));
                } else {
                    return Err(k2err!(K2ErrorCode::BadFormat, format!("Invalid name {}", input.data[0].name)));
                }
            }
             _ => {
                return Err(k2err!(K2ErrorCode::InvalidOperation, format!("Command not supported for library")));
            }
        }
        Ok("Ok".to_string())
    }
    fn generate(&mut self) -> K2CoderReturn {
        Ok("Ok".to_string())
    }
    fn build(&self) -> K2CoderReturn {   
        Ok("Ok".to_string())
    }
}