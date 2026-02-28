use std::collections::HashMap;

pub mod parser;
pub mod syntax_tree;

pub type Token = Vec<String>;

#[derive(Debug, Clone)]
pub struct K2Object {
    pub name: String,
    pub object_type: String,
    pub parent: Vec<String>,
    pub children: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct K2ReturnStruct {
    pub success: bool,
    pub command: String,
    pub tokens: Token,
    pub message: String,
    pub data: Vec<K2Object>,
}

impl K2ReturnStruct {
    pub fn new() -> Self {
        Self {
            success: true,
            command: "".to_string(),
            tokens: Vec::new(),
            message: "".to_string(),
            data: Vec::new(),
        }
    }
}