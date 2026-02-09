use memory_macro::K2Memory;

use crate::memory::{DataHeader, DataTrait, MemoryTrait};

#[derive(Clone, K2Memory)]
pub struct State<T: 'static + Sync + Send> {
    pub name: DataHeader,
    value: T,
    init: T,
    initialized: bool,
}

impl<T: 'static + Clone + Sync + Send + Default> State<T> {
    pub fn new(name: DataHeader) -> Result<Self, ()> {
        let param = Self {
            name: name.clone(),
            value: T::default(),
            init: T::default(),
            initialized: false,
        };
        Ok(param)
    }
    pub fn set(&mut self, value: T) -> Result<(), ()> {
        self.value = value;
        Ok(())
    }
    pub fn get(&self) -> &T {
        &self.value
    }
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }
    pub fn initialize(&mut self, value: T) -> Result<(), ()> {
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
    fn initialize(&mut self) -> () {
        self.value = self.init.clone();
        self.initialized = true;   
    }
}

unsafe impl<T: 'static + Send + Sync> Send for State<T> {}
unsafe impl<T: 'static + Send + Sync> Sync for State<T> {}