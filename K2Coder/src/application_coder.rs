use k2_lang::K2ReturnStruct;

use crate::coder::CoderTrait;

pub struct ApplicationCoder {
    name: String,
    path: Option<String>
}

impl CoderTrait for ApplicationCoder {
    fn new(name: String) -> Result<Box<dyn CoderTrait>, String> where Self: Sized {
        let instance = Self {
            name,
            path: None
        };
        Ok(Box::new(instance))
    }
    fn get_name(&self) -> String {
        self.name.clone()
    }
    fn get_path(&self) -> String {
        self.path.clone().unwrap_or_default()
    }
    fn set_parent_directory(&mut self, path: String) {
        self.path = Some(format!("{}/{}.rs", path, self.name));
    }
    fn proc_new(&mut self, object: &K2ReturnStruct) -> Result<String, String> {
        Ok(object.message.clone())
    }
    fn proc_add(&mut self, object: &K2ReturnStruct) -> Result<String, String> {
        Ok(object.message.clone())
    }
    fn proc_delete(&mut self, object: &K2ReturnStruct) -> Result<String, String> {
        Ok(object.message.clone())
    }
    fn proc_set(&mut self, object: &K2ReturnStruct) -> Result<String, String> {
        Ok(object.message.clone())
    }
    fn proc_connect(&mut self, object: &K2ReturnStruct) -> Result<String, String> {
        Ok(object.message.clone())
    }
    fn proc_disconnect(&mut self, object: &K2ReturnStruct) -> Result<String, String> {
        Ok(object.message.clone())
    }
    fn proc_exec(&mut self, object: &K2ReturnStruct) -> Result<String, String> {
        Ok(object.message.clone())
    }
}