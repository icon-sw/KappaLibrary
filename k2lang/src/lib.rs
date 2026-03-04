use std::collections::HashMap;

use k2stream::errors::K2Error;

pub mod parser;
pub mod ast;

pub type K2LangReturn = Result<K2LangStruct, K2Error>;

#[derive(Debug, Clone)]
pub struct K2LangObject {
    pub name: String,
    pub object_type: String,
    pub parent: Vec<String>,
    pub children: Vec<String>,
    pub properties: HashMap<String, String>
}
#[derive(Debug, Clone)]
pub struct K2LangStruct {
    pub command: String,
    pub message: String,
    pub tokens: Vec<String>,
    pub data: Vec<K2LangObject>
}