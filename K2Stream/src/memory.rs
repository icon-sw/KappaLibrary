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

impl Default for Memory {
    fn default() -> Self {
        Self::new()
    }
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
        if let std::collections::hash_map::Entry::Occupied(mut e) = self.data.entry(header) {
            e.insert(data);
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
    use crate::parameters::{Parameter, ParameterType};

    use super::*;
    
    #[test]
    fn test_memory() -> () {
        let mut test_memory = Memory::new();
        let param = Parameter::<f64>::float("test".to_string(), 0.0, ParameterType::DYNAMIC).unwrap();
        assert!(test_memory.insert("test".to_string(), Box::new(param)).is_ok());
        let param = Parameter::<i64>::int("test".to_string(), 0, ParameterType::DYNAMIC).unwrap();
        assert!(test_memory.insert("test".to_string(), Box::new(param)).is_err());
        assert!(!test_memory.is_initialized());
        let param = test_memory.get_mut(&"test".to_string());
        assert!(param.is_some());
        let param = param.unwrap();
        let param = param.as_any_mut().downcast_mut::<Parameter<f64>>();
        assert!(param.is_some());
        assert!(param.unwrap().set(10.0).is_ok());
        assert!(test_memory.is_initialized());
        let param = test_memory.get(&"test".to_string());
        let param = param.unwrap();
        let param = param.as_any().downcast_ref::<Parameter<f64>>();
        assert_eq!(param.unwrap().get(), &10.0);
        let param = Parameter::<i64>::int("test".to_string(), 0, ParameterType::DYNAMIC).unwrap();
        assert!(test_memory.update("test".to_string(), Box::new(param)).is_ok());
        assert!(!test_memory.is_initialized());
        assert!(test_memory.remove(&"test".to_string()).is_ok());
        let param = Parameter::<i64>::int("test".to_string(), 0, ParameterType::DYNAMIC).unwrap();
        assert!(test_memory.update("test".to_string(), Box::new(param)).is_err());
        assert!(test_memory.remove(&"test".to_string()).is_err());
        let param = Parameter::<i64>::int("test".to_string(), 0, ParameterType::DYNAMIC).unwrap();
        assert!(test_memory.insert("test".to_string(), Box::new(param)).is_ok());
        assert!(test_memory.remove(&"test".to_string()).is_ok());
        for i in 0..5 {
            let param_name = format!("test_{}",i);
            let param = Parameter::<i64>::int(param_name.clone(), i, ParameterType::DYNAMIC).unwrap();
            assert!(test_memory.insert(param_name.clone(), Box::new(param)).is_ok());
        }
        assert_eq!(test_memory.values().count(), 5);
        let values = test_memory.values_mut();
        for val in values.into_iter() {
            let param = val.as_any_mut().downcast_mut::<Parameter<i64>>();
            assert!(param.is_some());
            let param = param.unwrap();
            assert!(param.set(10).is_ok());
        }
        let values = test_memory.values();
        for val in values.into_iter() {
            let param = val.as_any().downcast_ref::<Parameter<i64>>();
            assert!(param.is_some());
            let param = param.unwrap();
            assert_eq!(param.get(),&10);
        }
    }
}