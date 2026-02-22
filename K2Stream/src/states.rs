use memory_macro::K2Memory;

use crate::{errors::{K2Error, K2ErrorCode}, k2err, memory::{DataHeader, DataTrait, MemoryTrait}};

#[derive(Clone, K2Memory)]
pub struct State<T: 'static + Sync + Send> {
    pub name: DataHeader,
    value: T,
    init: T,
    initialized: bool,
}

impl<T: 'static + Clone + Sync + Send + Default> State<T> {
    pub fn new(name: DataHeader) -> Result<Self, K2Error> {
        let param = Self {
            name: name.clone(),
            value: T::default(),
            init: T::default(),
            initialized: false,
        };
        Ok(param)
    }
    pub fn set(&mut self, value: T) -> Result<(), K2Error> {
        if self.initialized {
            self.value = value;
            Ok(())
        }
        else {
            Err(k2err!(K2ErrorCode::InvalidOperation, format!("State {} not initialized", self.name)))
        }
    }
    pub fn get(&self) -> &T {
        &self.value
    }
    pub fn set_init(&mut self, value: T) -> Result<(), K2Error> {
        self.value = value.clone();
        self.init = value;
        self.initialized = true;
        Ok(())
    }
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }
}

impl<T: 'static + Clone+ Send + Sync> DataTrait for State<T> {
    fn clone_box(self) -> Box<dyn DataTrait> {
        Box::new(self.clone())
    }
    fn is_setted(&self) -> bool {
        self.initialized
    }
    fn initialize(&mut self) {
        self.value = self.init.clone();
        self.initialized = true;   
    }
}

unsafe impl<T: 'static + Send + Sync> Send for State<T> {}
unsafe impl<T: 'static + Send + Sync> Sync for State<T> {}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_states() {
        let state = State::<f64>::new("test".to_string());
        assert!(state.is_ok());
        let mut state = state.unwrap();
        assert!(!state.is_initialized());
        assert!(!state.is_setted());
        assert!(state.set(1.0).is_err());
        assert!(state.set_init(1.0).is_ok());
        assert!(state.is_setted());
        assert!(state.set(2.0).is_ok());
        assert_eq!(state.get(), &2.0);
        state.initialize();
        assert_eq!(state.get(), &1.0);
    }
}