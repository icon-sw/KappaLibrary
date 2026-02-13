use std::collections::HashMap;

pub mod parser;
pub mod coder;

#[derive(Clone)]
pub struct K2Object {
    pub name: String,
    pub object_type: String,
    pub parent: Vec<String>,
    pub children: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Clone)]
pub struct K2ReturnStruct {
    pub success: bool,
    pub command: String,
    pub message: String,
    pub data: Option<Vec<K2Object>>,
}