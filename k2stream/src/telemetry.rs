use std::any::Any;
use std::collections::HashMap;
use std::net::UdpSocket;
use std::sync::{Condvar, OnceLock};
use std::sync::mpsc::Receiver;
use std::{collections::VecDeque, iter::Sum};

use chrono::Utc;
use libc::{clockid_t, pthread_t};

#[derive(Clone)]
pub struct Statistic {
    pub timestamp: f64,
    pub average: f64,
    pub std_dev: f64,
    pub max: f64,
    pub min: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
}

impl Statistic {
    pub fn average<T>(data: &Vec<T>) -> f64 
    where T: Clone + Sum + Into<f64> {
        let sum: T = data.iter().cloned().sum();
        let average: f64 = sum.into() / (data.len() as f64);
        average
    }
    pub fn variance<T>(data: &Vec<T>, average: f64) -> f64
    where T: Clone + Sum + Into<f64> {
        let var_sum: f64 = data.iter().map(|x| {
            let diff: f64 = x.clone().into() - average;
            diff * diff
        }).sum();
        var_sum / (data.len() as f64)
    }
    pub fn std_dev<T>(data: &Vec<T>, average: f64) -> f64
    where T: Clone + Sum + Into<f64> {
        Statistic::variance(data, average).sqrt()
    }
    pub fn percentile<T>(data: &mut Vec<T>, percentile: f64) -> f64 
    where T: Clone + Sum + Into<f64> + PartialOrd{
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let k = (percentile / 100.0) * ((data.len() - 1) as f64);
        let f = k.floor() as usize;
        let c = k.ceil() as usize;
        if f == c {
            data[f].clone().into()
        } else {
            let d0 = data[f].clone().into() * ((c as f64 - k));
            let d1 = data[c].clone().into() * ((k - f as f64));
            d0 + d1
        }
    }
}

impl<T> From<Vec<T>> for Statistic 
where T: Into<f64> + Clone + PartialOrd + Sum
{
    fn from(vector: Vec<T>) -> Self {
        let timestamp = Utc::now();
        let mut sort_vec = vector.clone();
        sort_vec.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let minimum: f64 = match sort_vec.first() {
            Some(min) => {min.clone().into()}
            None => {0.0}
        };
        let maximum: f64 = match sort_vec.last() {
            Some(max) => {max.clone().into()}
            None => {0.0}
        };
        let average = Statistic::average(&sort_vec);
        Self {
            timestamp:  timestamp.timestamp_millis() as f64 * 1e-3,
            average: average.clone(),
            std_dev: Statistic::std_dev(&sort_vec, average),
            max: maximum,
            min: minimum,
            p50: Statistic::percentile(&mut sort_vec, 50.0),
            p90: Statistic::percentile(&mut sort_vec, 90.0),
            p99: Statistic::percentile(&mut sort_vec, 99.0),
        }
    }
}

impl<T> From<VecDeque<T>> for Statistic 
where T: Into<f64> + Clone + PartialOrd + Sum
{
    fn from(vector: VecDeque<T>) -> Self {
        let vector: Vec<T> = vector.iter().cloned().collect();
        Statistic::from(vector)
    }
}

#[cfg(feature =  "telemetry")]
use std::sync::{Arc, Mutex, MutexGuard};

use crate::{k2err, k2log};
use crate::log::K2LogLevel;
use crate::processor::parameters::ParameterType;
use crate::processor::processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState};
use crate::streamer::task_monitor::TaskMonitor;
use crate::{errors::{K2ErrorCode, K2Error}, processor::memory::{DataHeader, MemoryTrait}};
use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;
// END_K2_IMPORT

static K2TELEMETRY: OnceLock<Arc<Mutex<Box<dyn ProcessorTrait>>>> = OnceLock::new();
#[derive(K2Memory, K2ProcessorBlock)]
pub struct K2Telemetry {
    name: String,
    header: ProcessorHeader,
    stream_block: StreamBlock,
    state: Arc<Mutex<StreamState>>,
    socket: Option<UdpSocket>,
}


impl ProcessorTrait for K2Telemetry {
    fn new(name: String) -> ProcessorNewReturn {
        let state = Mutex::new(StreamState::Uninitialized);
        let mut self_instance = Self {
            // START_K2_INIT
            name: name.clone(),
            header: ProcessorHeader {
                proc_name: "K2Telemetry".to_string(),
                description: "A fake processor with no functionality".to_string(),
                version: "0.1.0".to_string(),
                author: "Sofia".to_string(),
                email: "ms.sofia.silvestri@gmail.com".to_string(),
                license: "LGPLv2.0".to_string(),
                repository: "".to_string(),
            },
            stream_block: StreamBlock::new(),
            state: Arc::new(Mutex::new(StreamState::Uninitialized)),
            socket: None,
            
            // END_K2_INIT
            // END_USER_INIT
        };
        self_instance.get_stream_block_mut().add_parameter::<String>("host_destination".to_string(), ParameterType::STATIC)?;
        // END_K2_MEMBER_CREATION
        // END_USER_MEMBER_CREATION
        Ok(Box::new(self_instance))
    }
    fn initialize(&mut self ) -> Result<(), K2Error> {
        let socket = UdpSocket::bind("0.0.0.0:0")
            .map_err(|err| k2err!(
                K2ErrorCode::ProcessError,
                format!("{}", err)
            ))?;
        let host_destination = self
            .get_stream_block()
            .get_param_value::<String>(
                &"host_address".to_string())?;
        socket.connect(host_destination)
            .map_err(|err| k2err!(
                K2ErrorCode::ProcessError,
                format!("{}", err)
            ))?;
            self.socket = Some(socket);
        *self.state.lock().map_err(|err|
            k2err!(K2ErrorCode::LockError, format!("{}", err)))? = StreamState::Initialized;
        Ok(()) // INITIALIZE_CODE
    }
    fn process(&mut self) -> Result<(), K2Error> {
        Ok(()) // PROCESS_CODE
    }
    fn finalize(&mut self) -> Result<(), K2Error> {
        *self.state.lock().map_err(|err|
            k2err!(K2ErrorCode::LockError, format!("{}", err)))? = StreamState::Waiting;
        Ok(()) // FINALIZE_CODE
    }
}

impl K2Telemetry {
    
    pub fn add_monitor<T>(name: String, receiver: Arc<Mutex<Receiver<T>>>) 
    where T: 'static + Send + Sync{
        let _ = TaskMonitor::create_task(format!("metric_{}", name), move ||
        {
            let k2_telemetry = K2TELEMETRY.get_or_init(|| Arc::new(
                Mutex::new(
                    K2Telemetry::new("k2_telemetry".to_string()).unwrap()
                )
            ));
            let mut k2_telemetry = k2_telemetry.lock().unwrap();
            let k2_telemetry = k2_telemetry.as_any_mut().downcast_mut::<K2Telemetry>().unwrap();
            k2_telemetry.monitor(receiver);
        });
    }
    pub fn monitor<T>(&mut self, receiver: Arc<Mutex<Receiver<T>>>) {
        *self.state.lock().map_err(|err|
            k2err!(K2ErrorCode::LockError, format!("{}", err))).unwrap() = StreamState::Running;
        loop {
            let receiver = receiver.lock().unwrap();
            let _data = receiver.recv().unwrap();
            if let Some(_socket) = &self.socket {
                todo!()
            }
        }
    }
}