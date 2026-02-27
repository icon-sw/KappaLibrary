use k2_lang::K2ReturnStruct;

use crate::coder::CoderTrait;

pub struct LibraryCoder {
    name: String,
    path: Option<String>
}

impl CoderTrait for LibraryCoder {
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
    fn proc_add(&mut self, _object: &K2ReturnStruct) -> Result<String, String> {
        Err("Add not applicable to library".to_string())
    }
    fn proc_delete(&mut self, object: &K2ReturnStruct) -> Result<String, String> {
        Ok(object.message.clone())
    }
    fn proc_set(&mut self, _object: &K2ReturnStruct) -> Result<String, String> {
        Err("Set not applicable to library".to_string())
    }
    fn proc_connect(&mut self, _object: &K2ReturnStruct) -> Result<String, String> {
        Err("Connect not applicable to library".to_string())
    }
    fn proc_disconnect(&mut self, _object: &K2ReturnStruct) -> Result<String, String> {
        Err("Disconnect not applicable to library".to_string())
    }
    fn proc_exec(&mut self, _object: &K2ReturnStruct) -> Result<String, String> {
        Err("Exec not applicable to library".to_string())
    }
}