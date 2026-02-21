use std::fmt;

use memory_macro::K2Memory;
use num_traits::{Float, PrimInt};

use crate::{errors::{K2Error, K2ErrorCode}, k2err, memory::{DataHeader, DataTrait, MemoryTrait}};

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
impl fmt::Display for ParameterType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParameterType::STATIC => write!(f, "STATIC"),
            ParameterType::DYNAMIC => write!(f, "DYNAMIC"),
        }
    }
}
#[derive(PartialEq, Clone)]
pub enum ParameterValueType {
    INTEGER,
    FLOAT,
    OTHERS,
}
impl fmt::Display for ParameterValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParameterValueType::INTEGER => write!(f, "INTEGER"),
            ParameterValueType::FLOAT => write!(f, "FLOAT"),
            ParameterValueType::OTHERS => write!(f, "OTHERS"),
        }
    }
}

#[derive(Clone, K2Memory)]
pub struct Parameter<T: 'static + Send + Sync> {
    pub name: DataHeader,
    value: T,
    default: T,
    boundable: bool,
    min_value: Option<T>,
    max_value: Option<T>,
    values: Vec<T>,
    param_type: ParameterType,
    setted: bool,
}

impl<T: 'static + PrimInt + Sync + Send> Parameter<T> {
    pub fn int(name: DataHeader, default: T, param_type: ParameterType) -> Result<Self, K2Error> {
        let param = Self {
            name: name.clone(),
            value: default.clone(),
            default,
            boundable: true,
            min_value: Some(T::min_value()),
            max_value: Some(T::max_value()),
            values: Vec::new(),
            param_type,
            setted: false,
        };
        Ok(param)
    }
}

impl<T: 'static + Float + Sync + Send> Parameter<T> {
    pub fn float(name: DataHeader, default: T, param_type: ParameterType) -> Result<Self, K2Error> {
        let param = Self {
            name: name.clone(),
            value: default.clone(),
            default,
            boundable: true,
            min_value: Some(T::neg_infinity()),
            max_value: Some(T::infinity()),
            values: Vec::new(),
            param_type,
            setted: false,
        };
        Ok(param)
    }
}

impl<T: 'static + Clone + PartialOrd + Send + Sync> Parameter<T> 
{
    pub fn new(name: DataHeader, default: T, param_type: ParameterType) -> Result<Self, K2Error> {
        let param = Self {
            name: name.clone(),
            value: default.clone(),
            default,
            boundable: false,
            min_value: None,
            max_value: None,
            values: Vec::new(),
            param_type,
            setted: false,
        };
        Ok(param)
    }
    pub fn set_range(&mut self, value: T, range_type: ParameterRangeType) -> Result<(), K2Error> {
        match range_type {
            ParameterRangeType::Range => {
                self.values.push(value);
            }
            ParameterRangeType::RangeStart => {
                if !self.boundable {
                    return Err(k2err!( K2ErrorCode::ErrorRange, "Parameter can't have RangeStart"));
                }
                if let Some(max_value) = &self.max_value {
                    if &value <= max_value {
                        self.min_value = Some(value);
                    } else {
                        return Err(k2err!( K2ErrorCode::OutOfRange, "Miminum range shall be less or equal to current max range"))
                    }
                }
                else {
                    self.min_value = Some(value);
                }
            }
            ParameterRangeType::RangeEnd => {
                if !self.boundable {
                    return Err(k2err!( K2ErrorCode::ErrorRange, "Parameter can't have RangeEnd"));
                }
                if let Some(min_value) = &self.min_value {
                    if &value >= min_value {
                        self.max_value = Some(value);
                    } else {
                        return Err(k2err!( K2ErrorCode::OutOfRange, "Miminum range shall be less or equal to current max range"))
                    }
                }
                else {
                    self.max_value = Some(value);
                }
            }
        }
        Ok(())
    }
    pub fn set(&mut self, value: T) -> Result<(), K2Error> {
        if self.param_type == ParameterType::STATIC && self.setted {
            return Err(k2err!( K2ErrorCode::InvalidOperation, "Cannot set static parameter"))
        }
        if self.values.len() > 0 {
            if !self.values.contains(&value) {
                return Err(k2err!( K2ErrorCode::OutOfRange, "Value is not in range"))
            }
        }
        if let Some(min_value) = &self.min_value {
            if &value < min_value {
                return Err(k2err!( K2ErrorCode::OutOfRange, "Value is out of range"))
            }
        }
        if let Some(max_value) = &self.max_value {
            if &value > max_value {
                return Err(k2err!( K2ErrorCode::OutOfRange, "Value is out of range"))
            }
        }
        self.value = value;
        self.setted = true;
        Ok(())
    }
    pub fn get(&self) -> &T {
        &self.value
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

#[cfg(test)]
mod test {
    use super::*;
    impl Parameter<f64> {
        pub fn create(name: DataHeader, default: f64, param_type: ParameterType) -> Result<Self, K2Error> {
            let param = Self {
                name: name.clone(),
                value: default.clone(),
                default,
                boundable: true,
                min_value: None,
                max_value: None,
                values: Vec::new(),
                param_type,
                setted: false,
            };
            Ok(param)
        }
    }
    #[test]
    fn create() {
        assert!(Parameter::<i64>::int("test".to_string(), 0, ParameterType::STATIC).is_ok());
        assert!(Parameter::<f64>::float("test".to_string(), 0.0, ParameterType::STATIC).is_ok());
        assert!(Parameter::<String>::new("test".to_string(), "hello".to_string(), ParameterType::STATIC).is_ok());
    }
    #[test]
    fn set_range() {
        let mut param = Parameter::<i64>::int("test".to_string(), 0, ParameterType::DYNAMIC).unwrap();
        assert!(param.set_range(-100, ParameterRangeType::RangeStart).is_ok());
        assert!(param.set_range(100, ParameterRangeType::RangeEnd).is_ok());
        assert!(param.set(10).is_ok());
        assert!(param.set(20).is_ok());
        assert!(param.set(110).is_err());
        assert!(param.set(-110).is_err());
        assert!(param.set_range(200, ParameterRangeType::RangeStart).is_err());
        assert!(param.set_range(50, ParameterRangeType::RangeStart).is_ok());
        assert!(param.set_range(60, ParameterRangeType::RangeEnd).is_ok());
        assert!(param.set_range(10, ParameterRangeType::RangeEnd).is_err());
        let mut param = Parameter::<String>::new("test".to_string(), "default".to_string(), ParameterType::DYNAMIC).unwrap();
        assert!(param.set_range("a".to_string(),ParameterRangeType::RangeStart).is_err());
        assert!(param.set_range("d".to_string(),ParameterRangeType::RangeEnd).is_err());
        assert!(param.set_range("a".to_string(),ParameterRangeType::Range).is_ok());
        assert!(param.set_range("b".to_string(),ParameterRangeType::Range).is_ok());
        assert!(param.set_range("c".to_string(),ParameterRangeType::Range).is_ok());
        assert!(param.set("a".to_string()).is_ok());
        assert!(param.set("d".to_string()).is_err());
        let param = Parameter::<f64>::create("test".to_string(), 0.0, ParameterType::DYNAMIC);
        assert!(param.is_ok());
        let mut param = param.unwrap();
        assert!(param.set_range(0.0, ParameterRangeType::RangeEnd).is_ok());
        assert!(param.set(f64::neg_infinity()).is_ok());
        assert!(param.set(f64::epsilon()).is_err());
        let param = Parameter::<f64>::create("test".to_string(), 0.0, ParameterType::DYNAMIC);
        assert!(param.is_ok());
        let mut param = param.unwrap();
        assert!(param.set_range(0.0, ParameterRangeType::RangeStart).is_ok());
        assert!(param.set(f64::infinity()).is_ok());
        assert!(param.set(-f64::epsilon()).is_err());
    }
    #[test]
    fn usage_test() {
        let parameter = Parameter::<f64>::float("test".to_string(), 1.0, ParameterType::DYNAMIC);
        assert!(parameter.is_ok());
        let mut parameter = parameter.unwrap();
        assert!(!parameter.is_setted());
        assert!(parameter.set(10.0).is_ok());
        assert!(parameter.is_setted());
        assert_eq!(parameter.get(), &10.0);
        assert!(parameter.set(3.0).is_ok());
        assert_eq!(parameter.get(), &3.0);


        let parameter = Parameter::<f64>::float("test".to_string(), 1.0, ParameterType::STATIC);
        assert!(parameter.is_ok());
        let mut parameter = parameter.unwrap();
        assert!(!parameter.is_setted());
        assert!(parameter.is_default());
        parameter.default();
        assert!(parameter.is_default());
        assert!(parameter.is_setted());
        assert!(parameter.set(3.0).is_err());
        assert!(parameter.is_default());
        
        let parameter = Parameter::<f64>::float("test".to_string(), 1.0, ParameterType::STATIC);
        assert!(parameter.is_ok());
        let mut parameter = parameter.unwrap();
        assert!(!parameter.is_setted());
        assert!(parameter.set(3.0).is_ok());
        assert_eq!(parameter.get(), &3.0);
        assert!(!parameter.is_default());
        parameter.default();
        assert!(parameter.is_default());

        let parameter = Parameter::<f64>::float("test".to_string(), 1.0, ParameterType::DYNAMIC);
        assert!(parameter.is_ok());
        let mut parameter = parameter.unwrap();
        assert!(!parameter.is_setted());
        parameter.initialize();
        assert!(parameter.is_default());
        assert!(parameter.is_setted());
    }
}