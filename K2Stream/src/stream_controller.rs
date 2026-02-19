use core::str;
use std::{collections::HashMap, sync::{Arc, Mutex, MutexGuard, OnceLock}, thread::JoinHandle}; 

use processor_macro::K2ProcessorBlock;
use memory_macro::K2Memory;

use crate::{memory::{DataHeader, MemoryTrait}, modes::OperativeMode, processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState}};

pub type Callback = fn (&mut dyn ProcessorTrait) -> Result<(), ()>;
pub type StreamProcessorHandle = Arc<Mutex<Option<JoinHandle<Result<(),()>>>>>;
type StreamTable = Mutex<Vec<Arc<Mutex<StreamController>>>>;
static STREAM_TABLE: OnceLock<StreamTable> = OnceLock::new();
static STREAM_ID_COUNTER: OnceLock<Mutex<isize>> = OnceLock::new();

#[derive(K2Memory, K2ProcessorBlock)]
pub struct StreamController {
    pub name: String,
    pub header: ProcessorHeader,
    stream_id: isize,
    stream_block: StreamBlock,
    modes: HashMap<usize, OperativeMode>,
    current_mode_id: usize,
    command_map: HashMap<String, String>,
    processors: HashMap<String, Box<dyn ProcessorTrait>>,
    commands_callback: HashMap<String, Callback>,
    state: Arc<Mutex<StreamState>>,
    stream_handle: StreamProcessorHandle,
}

impl StreamController {
    pub fn create(name: String) -> Result<isize, ()> {
        let mode = OperativeMode::new("default".to_string(), 0);
        let mut modes = HashMap::new();
        modes.insert(0, mode);

        let mut self_instance = Self {
            name: name.clone(),
            stream_id: -1 as isize,
            header: ProcessorHeader {
                proc_name: "StreamController".to_string(),
                description: "A processor that controls the stream blocks and the execution of the modes".to_string(),
                version: "0.1.0".to_string(),
                author: "Sofia Silvestri".to_string(),
                email: "ms.sofia.silvestri@gmail.com".to_string(),
                license: "LGPLv2.0".to_string(),
                repository: "".to_string(),
            },
            stream_block: StreamBlock::new(),
            modes,
            current_mode_id: 0,
            command_map: HashMap::new(),
            state: Arc::new(Mutex::new(StreamState::Uninitialized)),
            processors: HashMap::new(),
            commands_callback: HashMap::new(),
            stream_handle: Arc::new(Mutex::new(None)),
        };
        self_instance.stream_block.add_input::<String>("command".to_string())?;
        self_instance.stream_block.add_output::<Result<(), ()>>("response".to_string())?;
        dbg!("Creating stream controller with name: {}", name.clone());
        let stream_id: isize;
        {
            let mut stream_id_counter = STREAM_ID_COUNTER.get_or_init(|| Mutex::new(0)).lock().map_err(|_| ())?;
            *stream_id_counter += 1;
            dbg!(*stream_id_counter);
            stream_id = *stream_id_counter;
        }
        self_instance.stream_id = stream_id;
        dbg!("Set stream id: {}", stream_id);
        self_instance.stream_block.set_stream_id(stream_id);
        dbg!("Registering commands...");
        self_instance.register_commands()?;
        dbg!("Registering stream controller in table...");
        let mut stream_table = STREAM_TABLE.get_or_init(|| Mutex::new(Vec::new())).lock().map_err(|_| ())?;
        stream_table.push(Arc::new(Mutex::new(self_instance)));
        dbg!("Stream controller created with id: {}", stream_id);
        Ok(stream_id)
    }
    pub fn get_stream_by_id(id: isize) -> Result<Arc<Mutex<Self>>, ()> {
        let stream_table = STREAM_TABLE.get_or_init(|| Mutex::new(Vec::new())).lock().map_err(|_| ())?;
        let stream_lock = stream_table.get((id - 1) as usize).ok_or(())?;
        Ok(Arc::clone(stream_lock))
    }
    pub fn get_stream_by_name(_name: String) -> Result<Arc<Mutex<Self>>, ()> {
        unimplemented!()
    }
    pub fn register_commands(&mut self) -> Result<(), ()> {
        self.stream_block.add_command("init".to_string(), |proc| {
            let stream_controller = proc.as_any_mut().downcast_mut::<Self>().ok_or(())?;
            stream_controller.initialize()
        })?;
        self.stream_block.add_command("run".to_string(), |proc| {
            let stream_controller = proc.as_any_mut().downcast_mut::<Self>().ok_or(())?;
            Self::run(stream_controller.stream_id)
        })?;
        self.stream_block.add_command("stand-by".to_string(), |proc| {
            let stream_controller = proc.as_any_mut().downcast_mut::<Self>().ok_or(())?;
            stream_controller.finalize()
        })?;
        Ok(())
    }
    pub fn stream_handle(&self) -> StreamProcessorHandle {
        self.stream_handle.clone()
    }
    pub fn set_stream_handle(&mut self, handle: StreamProcessorHandle) {
        self.stream_handle = handle;
    }
    pub fn run(stream_id: isize) -> Result<(), ()> {
        let stream = Self::get_stream_by_id(stream_id)?;
        let handle: JoinHandle<Result<(), ()>> = std::thread::spawn(move || {
            let mut stream = stream.lock().map_err(|_| ())?;
            {
                let mut state = stream.state.lock().map_err(|_| ())?;
                if *state == StreamState::Uninitialized {
                    return Err(());
                }
                *state = StreamState::Running;
            }
            loop {
                if let Err(e) = stream.process() {
                    let mut state = stream.state.lock().map_err(|_| ())?;
                    *state = StreamState::Waiting;
                    return Err(e);
                }
                let state = stream.state.lock().map_err(|_| ())?;
                if *state == StreamState::Waiting {
                    break;
                }
            }
            Ok(())
        });
        let stream = Self::get_stream_by_id(stream_id)?;
        stream.lock().map_err(|_|())?.set_stream_handle(Arc::new(Mutex::new(Some(handle))));
        Ok(())
    }
    pub fn connect<T: 'static + Send + Sync + Clone> (&mut self, from: String, to: String) -> Result<(), ()> {
        let mut from_block = None;
        let mut to_block = None;
        let from_split: Vec<&str> = from.split(".").collect();
        let to_split: Vec<&str> = to.split(".").collect();
        let from_proc: String = from_split.get(0).ok_or(())?.to_string();
        let to_proc: String = to_split.get(0).ok_or(())?.to_string();
        let from_connector = from_split.get(1).ok_or(())?.to_string();
        let to_connector = from_split.get(1).ok_or(())?.to_string();
        for (proc_name, proc) in self.processors.iter_mut(){
            if *proc_name == from_proc {
                from_block = Some(proc.get_stream_block_mut());
            } else if *proc_name == to_proc {
                to_block = Some(proc.get_stream_block_mut());
            }
            if let (Some(from_block), Some(to_block)) = (from_block.as_mut(), to_block.as_mut()) {
                from_block.connect::<T>(&from_connector, &to_connector, to_block)?;
                return Ok(())
            }
        }
        Err(())
        
    }
    pub fn add_mode(&mut self, id: usize, mode: OperativeMode) -> Result<(), ()> {
        if self.modes.contains_key(&id) {
            Err(())
        } else {
            self.modes.insert(id, mode);
            Ok(())
        }
    }
    pub fn get_mode(&self, id: &usize) ->  Result<&OperativeMode, ()> {
        self.modes.get(id).ok_or(())
    }
    pub fn get_mode_mut(&mut self, id: &usize) ->  Result<&mut OperativeMode, ()> {
        self.modes.get_mut(id).ok_or(())
    }
    pub fn set_current_mode(&mut self, id: usize) -> Result<(), ()> {
        if self.modes.contains_key(&id) {
            let mode = self.modes.get_mut(&self.current_mode_id).ok_or(())?;
            mode.finalize()?;
            let mode = self.modes.get_mut(&id).ok_or(())?;
            mode.initialize()?;
            mode.process()?;
            self.current_mode_id = id;
            Ok(())
        } else {
            Err(())
        }
    }
    pub fn add_command(id: isize, command: String, block_name: String, callback: Callback) -> Result<(), ()> {
        if id < 0 || id > *STREAM_ID_COUNTER.get_or_init(|| Mutex::new(0)).lock().map_err(|_| ())? {
            return Err(());
        }
        let mut stream_table = STREAM_TABLE.get_or_init(|| Mutex::new(Vec::new())).lock().map_err(|_| ())?;
        let stream_lock = stream_table.get_mut(id as usize).ok_or(())?;
        let mut binding = stream_lock.lock().map_err(|_| ())?;
        let stream = binding.as_any_mut().downcast_mut::<Self>().ok_or(())?;
        if stream.command_map.contains_key(&command) {
            return Err(())
        }
        if !stream.processors.contains_key(&block_name) {
            return Err(())
        }
        stream.command_map.insert(command.clone(), block_name);
        stream.commands_callback.insert(command, callback);
        Ok(())
    }
    pub fn execute_command(&mut self, command: String) -> Result<(), ()> {
        let block_name = self.command_map.get(&command).ok_or(())?;
        let block = self.processors.get_mut(block_name).ok_or(())?;
        let callback = self.commands_callback.get(&command).ok_or(())?;
        (callback)(block.as_mut())
    }
    pub fn add_processor(&mut self, name: String, processor: Box<dyn ProcessorTrait>) -> Result<(), ()> {
        if self.processors.contains_key(&name.clone()) {
            return Err(());
        }
        self.processors.insert( name, processor);
        Ok(())
    }
    pub fn get_processors(&self, processor_list: String) -> Result<&Box<dyn ProcessorTrait>, ()> {
        self.processors.get(&processor_list).ok_or(())
    }
    pub fn get_processors_mut(&mut self, processor_list: String) -> Result<&mut Box<dyn ProcessorTrait>, ()> {
        self.processors.get_mut(&processor_list).ok_or(())
    }
}

impl ProcessorTrait for StreamController {
    fn new(_name: String) -> ProcessorNewReturn {
        Err(())
    }

    fn initialize(&mut self) -> Result<(), ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        if self.stream_id == -1 {
            return Err(());
        }
        if *state == StreamState::Running {
            return Err(());
        }
        for mode in self.modes.values_mut() {
            mode.initialize()?;
        }
        *state = StreamState::Initialized;
        Ok(())
    }
    fn process(&mut self) -> Result<(), ()> {
        if self.stream_block.get_input::<String>(&"command".to_string())?.receive().is_ok() {
            let command = self.stream_block.get_input::<String>(&"command".to_string())?.receive()?;
            self.execute_command(command)?;
            if self.stream_block.get_output::<Result<(),()>>(&"response".to_string())?.send(Ok(())).is_ok() {
                return Ok(());
            }
        }
        Err(())
    }
    fn finalize(&mut self) -> Result<(), ()> {
        let handle = self.stream_handle().lock().map_err(|_|())?.take();
        self.set_stream_handle(Arc::new(Mutex::new(None)));
        if let Some(handle) = handle {
            match handle.join() {
                Ok(result) => {

                    return result;
                }
                Err(_) => return Err(())
            }
        }
        Ok(())
    }
}