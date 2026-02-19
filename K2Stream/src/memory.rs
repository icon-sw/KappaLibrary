use std::{any::Any, collections::HashMap};

use crate::errors::{K2Error, K2ErrorCode};

pub type DataHeader = String;

pub trait MemoryTrait : Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub trait DataTrait: MemoryTrait + Send + Sync {
    fn initialize(&mut self) -> ();
    fn clone_box(self) -> Box<dyn DataTrait>;
    fn is_setted(&self) -> bool;
}

pub struct Memory {
    data: HashMap<DataHeader, Box<dyn DataTrait>>,
}

impl Memory {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }
    pub fn insert(&mut self, header: DataHeader, data: Box<dyn DataTrait>) -> Result<(), K2Error> {
        if self.data.contains_key(&header) {
            return Err(K2Error { code: K2ErrorCode::AlreadyExists, message: "Data with this header already exists".into() });
        }
        self.data.insert(header, data);
        Ok(())
    }
    pub fn update(&mut self, header: DataHeader, data: Box<dyn DataTrait>) -> Result<(), K2Error> {
        if self.data.contains_key(&header) {
            self.data.insert(header, data);
            Ok(())
        } else {
            Err(K2Error { code: K2ErrorCode::NotFound, message: "Data with this header does not exist".into() })
        }
    }
    pub fn remove(&mut self, header: &DataHeader) -> Result<(), K2Error> {
        if self.data.remove(header).is_some() {
            Ok(())
        } else {
            Err(K2Error { code: K2ErrorCode::NotFound, message: "Data with this header does not exist".into() })
        }
    }
    pub fn get(&self, header: &DataHeader) -> Option<&Box<dyn DataTrait>> {
        self.data.get(header)
    }
    pub fn get_mut(&mut self, header: &DataHeader) -> Option<&mut Box<dyn DataTrait>> {
        self.data.get_mut(header)
    }
    pub fn values(&self) -> impl Iterator<Item = &Box<dyn DataTrait>> {
        self.data.values()
    }
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut Box<dyn DataTrait>> {
        self.data.values_mut()
    }
    pub fn is_initialized(&self) -> bool {
        for data in self.data.values() {
            if !data.is_setted() {
                return false;
            }
        }
        true
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn void() -> () {
        let _a = Memory::new();
    }
}