use std::collections::HashMap;

use k2_lang::{K2Object, K2ReturnStruct};

use crate::{coder::{CARGO_IF, Coder, CoderTrait}, processor_coder::ProcessCoder};

pub struct LibraryCoder {
    name: String,
    library_path: String,
    path: String,
    processors: HashMap<String, Box<dyn CoderTrait>>,
}

impl CoderTrait for LibraryCoder {
    fn new(name: String, object: &K2Object) -> Result<Box<dyn CoderTrait>, String> where Self: Sized {
        let path = object.properties.get(&"path".to_string()).ok_or("Path not present".to_string())?;
        let mut instance = Self {
            name,
            library_path: path.to_string(),
            path: path.to_string(),
            processors: HashMap::new(),
        };
        instance.set_parent_directory(path.clone());
        // Create project with CARGO_IF
        let cargo_if = CARGO_IF.get().ok_or("Cargo interface not setted".to_string())?;
        cargo_if.cargo_new_library(path.clone())?;
        Ok(Box::new(instance))
    }
    fn get_name(&self) -> String {
        self.name.clone()
    }
    fn get_path(&self) -> String {
        self.path.clone()
    }
    fn set_parent_directory(&mut self, path: String) {
        self.path = format!("{}/src/lib.rs", path);
    }
    fn proc_new(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String> {
        let object = k2_struct.data.get(0).ok_or("Missing data")?;
        let object_name = object.name.clone();
        let object_type = object.object_type.clone();
        if object_type == "processor".to_string() {
            if self.processors.contains_key(&object_name) {
                return Err("Object already exist".to_string());
            }
            let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
            if split_name.len() != 2 {
                return Err("Invalid object name".to_string());
            }
            self.processors.insert(object_name.clone(), ProcessCoder::new(object_name.clone(), object)?);
            self.processors.get_mut(&object_name.clone()).unwrap().set_parent_directory(self.library_path.clone());
            return Ok(format!("Processor {} successfull created", object_name));
        } else {
            let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
            if split_name.len() != 3 {
                return Err("Invalid object name".to_string());
            }
            let procesor_name = format!("{}.{}", split_name[0], split_name[1]);
            if self.processors.contains_key(&procesor_name) {
                return Ok(self.processors.get_mut(&procesor_name).unwrap().proc_new(k2_struct)?);
            } else {
                return Err(format!("Processor {} not present", procesor_name));
            }
        }
       
    }
    fn proc_add(&mut self, _k2_struct: &K2ReturnStruct) -> Result<String, String> {
        Err("Add not applicable to library".to_string())
    }
    fn proc_delete(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String> {
        let object = k2_struct.data.get(0).ok_or("Missing data")?;
        let object_name = object.name.clone();
        let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
        if split_name.len() == 1 && split_name[0] == self.name {
            for proc in self.processors.values_mut() {
                proc.proc_delete(k2_struct)?;
            }
            let cargo_if = CARGO_IF.get().ok_or("Cargo interface not setted".to_string())?;
            cargo_if.delete_project(self.library_path.clone())?;
        } else {
            let proc_name = format!("{}.{}", split_name[0], split_name[1]);
            if let Some(proc) = self.processors.get_mut(&proc_name) {
                proc.proc_delete(k2_struct)?;
                self.processors.remove(&object_name);
            } else {
                return Err(format!("Processor {} not found", proc_name));
            }
        }
        Ok(format!("Object {} deleted with success", object_name))
    }
    fn proc_set(&mut self, k2_struct: &K2ReturnStruct) -> Result<String, String> {
        let object = k2_struct.data.get(0).ok_or("Missing data")?;
        let object_name = object.name.clone();
        let split_name: Vec<String> = object_name.split(".").map(|s| s.to_string()).collect();
        let proc_name = format!("{}.{}", split_name[0], split_name[1]);
        if let Some(proc) = self.processors.get_mut(&proc_name) {
            proc.proc_set(k2_struct)
        } else {
            Err(format!("Processor {} not found", proc_name))
        }
        
    }
    fn proc_connect(&mut self, _k2_struct: &K2ReturnStruct) -> Result<String, String> {
        Err("Connect not applicable to library".to_string())
    }
    fn proc_disconnect(&mut self, _k2_struct: &K2ReturnStruct) -> Result<String, String> {
        Err("Disconnect not applicable to library".to_string())
    }
    fn proc_exec(&mut self, _k2_struct: &K2ReturnStruct) -> Result<String, String> {
        Err("Exec not applicable to library".to_string())
    }
    fn generate(&mut self) -> Result<String, String> {
        let code_file = Coder::get_tmp_file();
        let mut lib_code: Vec<String> = Vec::new();
        for proc in self.processors.values_mut() {
            lib_code.push(format!("pub mod {}", proc.get_name()));
            proc.generate()?;
        }
        let full_code = lib_code.join("\n");
        Coder::file_write(code_file.clone(), full_code)?;
        Coder::file_move(&code_file, &self.path)?;
        Ok(format!("Library {} code generate with success", self.name.clone()))
    }
    fn build(&self) -> Result<String, String> {
        let cargo_if = CARGO_IF.get().ok_or("Cargo interface not setted".to_string())?;
        cargo_if.cargo_build(self.library_path.clone(), "debug".to_string())?;
        Ok("Build completed".to_string())
    }
}