use memory_macro::K2Memory;
use num_traits::{Float, PrimInt};

use crate::memory::{DataHeader, DataTrait, MemoryTrait};

pub enum ParameterRangeType {
    Range,
    RangeStart,
    RangeEnd,
}
#[derive(PartialEq, Clone)]
pub enum ParameterType {
    STATIC,
    DYNAMIC,
}
#[derive(PartialEq, Clone)]
pub enum ParameterValueType {
    INTEGER,
    FLOAT,
}
#[derive(Clone, K2Memory)]
pub struct Parameter<T: 'static + Send + Sync> {
    pub name: DataHeader,
    value: T,
    default: T,
    min_value: T,
    max_value: T,
    values: Vec<T>,
    param_type: ParameterType,
    setted: bool,
}

impl<T: 'static + PrimInt + Sync + Send> Parameter<T> {
    pub fn int(name: DataHeader, default: T, param_type: ParameterType) -> Result<Self, ()> {
        let param = Self {
            name: name.clone(),
            value: default.clone(),
            default,
            min_value: T::min_value(),
            max_value: T::max_value(),
            values: Vec::new(),
            param_type,
            setted: false,
        };
        Ok(param)
    }
}

impl<T: 'static + Float + Sync + Send> Parameter<T> {
    pub fn float(name: DataHeader, default: T, param_type: ParameterType) -> Result<Self, ()> {
        let param = Self {
            name: name.clone(),
            value: default.clone(),
            default,
            min_value: T::neg_infinity(),
            max_value: T::infinity(),
            values: Vec::new(),
            param_type,
            setted: false,
        };
        Ok(param)
    }
}

impl<T: 'static + Clone + PartialOrd + Send + Sync> Parameter<T> {
    pub fn set_range(&mut self, value: T, range_type: ParameterRangeType) -> Result<(), ()> {
        match range_type {
            ParameterRangeType::Range => {
                self.values.push(value);
            }
            ParameterRangeType::RangeStart => {
                if value <= self.max_value {
                    self.min_value = value;
                } else {
                    return Err(())
                }
            }
            ParameterRangeType::RangeEnd => {
                if value >= self.min_value {
                    self.max_value = value;
                } else {
                    return Err(())
                }
            }
        }
        Ok(())
    }
    pub fn set(&mut self, value: T) -> Result<(), ()> {
        if self.param_type == ParameterType::STATIC && self.setted {
            return Err(())
        }
        if value >= self.min_value && value <= self.max_value {
            if self.values.len() > 0 {
                if !self.values.contains(&value) {
                    return Err(())
                }
            }
            self.value = value;
            self.setted = true;
            Ok(())
        } else {
            Err(())
        }
    }
    pub fn get(&self) -> &T {
        &self.value
    }
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }
    pub fn default(&mut self) -> () {
        self.value = self.default.clone();
        self.setted = true;
    }
    pub fn is_default(&self) -> bool {
        self.value == self.default
    }
}

impl<T: 'static + Clone+ Send + Sync> DataTrait for Parameter<T> {
    fn clone_box(self) -> Box<dyn DataTrait> {
        Box::new(self.clone())
    }
    fn is_setted(&self) -> bool {
        self.setted
    }
    fn initialize(&mut self) -> () {
        self.value = self.default.clone();
        self.setted = true;
    }
}

unsafe impl<T: 'static + Send + Sync> Send for Parameter<T> {}
unsafe impl<T: 'static + Send + Sync> Sync for Parameter<T> {}