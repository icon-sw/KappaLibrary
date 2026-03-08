use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread::{JoinHandle, sleep};
use std::{fmt, thread};

use crate::log::{K2LogLevel};
use crate::telemetry::Statistic;
use crate::{k2err, k2log};
use crate::processor::parameters::{ParameterRangeType, ParameterType};
use crate::processor::processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState};
use crate::{errors::{K2ErrorCode, K2Error}, processor::memory::{DataHeader, MemoryTrait}};
use chrono::{DateTime, TimeDelta, Utc};
use libc::{clock_gettime, clockid_t, pthread_getcpuclockid, pthread_self, pthread_t, timespec};
use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;

pub fn timespec_to_f64(ts: &timespec) -> f64 {
    ts.tv_sec as f64 + (ts.tv_nsec as f64) * 1e-9
}
struct Task {
    pub name: String,
    pub occupacy: VecDeque<f64>,
    thread_id: pthread_t,
    cpu_clock_id: clockid_t,
    last_cpu_time: f64,
    last_update: DateTime<Utc>
}

impl Task {
    fn new(name: String, thread_id: pthread_t) -> Self {
        let mut cpu_clock_id: clockid_t = 0;
        unsafe {
            pthread_getcpuclockid(thread_id, &mut cpu_clock_id);
        }
        Self {
            name: name.to_string(),
            occupacy: VecDeque::new(),
            thread_id,
            cpu_clock_id,
            last_cpu_time: 0.0,
            last_update: Utc::now(),
        }
    }
    fn update(&mut self) -> Result<(), K2Error> {
        unsafe {
            let mut ts: timespec = timespec { tv_sec: 0, tv_nsec: 0 };
            let ts_ptr: *mut timespec = &mut ts as *mut timespec;
            let res = clock_gettime(self.cpu_clock_id, ts_ptr);
            if  res != 0 {
                return Err(k2err!(K2ErrorCode::ProcessError, format!("clock_gettime return {}", res)));
            }
            if self.last_cpu_time == 0.0 {
                self.last_cpu_time = timespec_to_f64(&ts);
                self.last_update = Utc::now();
            } else {
                let current_cpu_time = timespec_to_f64(&ts);
                let current_time = Utc::now();
                let cpu_time_diff = current_cpu_time - self.last_cpu_time;
                let wall_time_diff = (current_time - self.last_update).num_nanoseconds().unwrap() as f64 * 1e-9;
                if wall_time_diff > 0.0 {
                    let occupacy = cpu_time_diff / wall_time_diff;
                    self.occupacy.push_back(occupacy);
                    if self.occupacy.len() > 100 {
                        self.occupacy.pop_front();
                    }
                }
                self.last_cpu_time = current_cpu_time;
                self.last_update = current_time;
            }
            Ok(())
        }
    }
    fn get_stats(&self) -> Statistic {
       self.occupacy.clone().into()
    }
}
unsafe impl Send for Task {}
unsafe impl Sync for Task {}

static TASK_TABLE: OnceLock<Arc<Mutex<HashMap<String, Task>>>> = OnceLock::new();

#[derive(K2Memory, K2ProcessorBlock)]
pub struct TaskMonitor {
    name: String,
    header: ProcessorHeader,
    stream_block: StreamBlock,
    state: Arc<Mutex<StreamState>>,
    time_send: DateTime<Utc>,
}

impl ProcessorTrait for TaskMonitor {
    fn new(name: String) -> ProcessorNewReturn {
        let mut self_instance = Self {
            name: name.clone(),
            header: ProcessorHeader {
                proc_name: "TaskMonitor".to_string(),
                description: "The task monitoring job".to_string(),
                version: "0.1.0".to_string(),
                author: "Sofia".to_string(),
                email: "ms.sofia.silvestri@gmail.com".to_string(),
                license: "LGPLv2.0".to_string(),
                repository: "".to_string(),
            },
            stream_block: StreamBlock::new(),
            state: Arc::new(Mutex::new(StreamState::Uninitialized)),
            time_send: Utc::now(),
        };
        self_instance.get_stream_block_mut().add_output::<f64>("statistics".to_string())?;
        self_instance.get_stream_block_mut().add_parameter::<f64>(
            "time_refresh".to_string(), 
            ParameterType::DYNAMIC)?;
        self_instance.get_stream_block_mut().add_parameter::<f64>(
            "time_send".to_string(), 
            ParameterType::DYNAMIC)?;
        self_instance.get_stream_block_mut().add_parameter::<bool>(
            "enable".to_string(), 
            ParameterType::DYNAMIC)?;
        self_instance.get_stream_block_mut().get_parameter_mut(&"time_refresh".to_string())
            .unwrap().set_range(10e-3, ParameterRangeType::RangeStart)?;
        self_instance.get_stream_block_mut().get_parameter_mut(&"time_refresh".to_string())
            .unwrap().set_range(1000e-3, ParameterRangeType::RangeEnd)?;
        self_instance.get_stream_block_mut().get_parameter_mut(&"time_send".to_string())
            .unwrap().set_range(100e-3, ParameterRangeType::RangeStart)?;
        self_instance.get_stream_block_mut().get_parameter_mut(&"time_send".to_string())
            .unwrap().set_range(1000e-3, ParameterRangeType::RangeEnd)?;
        self_instance.get_stream_block_mut().set_param_value(&"enable".to_string(), false)?;
        Ok(Box::new(self_instance))
    }
    fn initialize(&mut self ) -> Result<(), K2Error> {
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

impl TaskMonitor {
    pub fn create_task<F, T, S: Clone>(name: S, f: F) -> Result<JoinHandle<T>, K2Error>
    where
        F: FnOnce() -> T + Send + 'static, 
        T: Send + 'static, 
        S: Into<String> + fmt::Display,
    {
        let builder = thread::Builder::new().name(name.clone().into());
        let task_table = TASK_TABLE.get_or_init(|| Arc::new(Mutex::new(HashMap::new())));
        let mut task_table = task_table.lock().map_err(|err| 
            k2err!(K2ErrorCode::LockError, format!("{}", err)))?;
        let thread_id = unsafe { pthread_self() };
        
        let task = Task::new(name.to_string(), thread_id);
        task_table.insert(name.to_string(), task); 
        builder.spawn(f).map_err(|err| k2err!(K2ErrorCode::ProcessError, format!("{}", err)))
    }

    pub fn run(&mut self) -> Result<(), K2Error> {
        *self.state.lock().map_err(|err| k2err!(K2ErrorCode::LockError, format!("{}",err)))? = StreamState::Running;
        while self.get_proc_state()? == StreamState::Running {
            let start = Utc::now();
            let enable = self.get_stream_block().get_param_value::<bool>(&"enable".to_string()).ok().unwrap_or(&false);
            if *enable {
                let task_table_arc = TASK_TABLE.get_or_init(|| Arc::new(Mutex::new(HashMap::new())));
                
                let mut task_table;
                match task_table_arc.lock() {
                    Ok(table) =>{task_table = table;},
                    Err(err) => {
                        let _ = k2log!(self.name.clone(), K2LogLevel::Error, "{}", format!("{}", err));
                        continue;
                    },
                }
                let time_send = self.get_stream_block().get_param_value::<f64>(&"time_send".to_string()).ok().unwrap_or(&1000.0e-3);
                let send_data = self.time_send + TimeDelta::milliseconds((time_send*1e3) as i64) > start;
                let mut remove_task = Vec::new();
                let mut stats: Vec<Statistic> = Vec::new();
                for (name, task) in task_table.iter_mut() {
                    match task.update() {
                        Ok(_) => {},
                        Err(err) => {
                            let _ = k2log!(self.name.clone(), K2LogLevel::Error, "{}", format!("{}", err.message));
                            remove_task.push(name.clone());
                        }
                    }
                    if send_data {
                        stats.push(task.get_stats());
                    }
                }
                for name in remove_task {
                    task_table.remove(&name);
                }
                if send_data {
                    match self.get_stream_block_mut().send_output(&"statistics".to_string(), stats) {
                        Ok(_) => {},
                        Err(err) => {
                            let _ = k2log!(self.name.clone(), K2LogLevel::Error, "{}", format!("{}", err.message));
                        }
                    }
                    self.time_send = start;
                }
            }
            let time_refresh = self.get_stream_block().get_param_value::<f64>(&"time_refresh".to_string()).ok().unwrap_or(&100.0e-3);
            let sleep_until = start + TimeDelta::milliseconds((time_refresh*1e3) as i64);
            let now = Utc::now();
            match (now - sleep_until).to_std() {
                Ok(delta) => {sleep(delta);}
                Err(err) => {
                    let _ = k2log!(self.name.clone(), K2LogLevel::Error, "{}", format!("{}",err));
                    continue;
                }
            }

        }
        Ok(())
    }
}