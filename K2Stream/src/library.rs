use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::processors::ProcessorTrait;

#[derive(Clone, Serialize, Deserialize)]
pub struct LibraryStruct {
    pub name: String,
    pub description: String,
    pub version: String,
    pub author: String,
    pub email: String,
    pub license: String,
    pub repository: String,
    pub processors: HashMap<String, Box<dyn ProcessorTrait>>,
}

impl LibraryStruct {
    pub fn get_processor(&self, name: &String) -> Option<&Box<dyn ProcessorTrait>> {
        self.processors.get(name)
    }
}