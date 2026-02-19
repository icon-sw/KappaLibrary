use core::str;
use std::{collections::HashMap, sync::{Arc, Mutex, MutexGuard, OnceLock}, thread::JoinHandle}; 

use processor_macro::K2ProcessorBlock;
use memory_macro::K2Memory;

use crate::{errors::{K2Error, K2ErrorCode}, memory::{DataHeader, MemoryTrait}, modes::OperativeMode, processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState}};

pub type Callback = fn (&mut dyn ProcessorTrait) -> Result<(), K2Error>;
pub type StreamProcessorHandle = Arc<Mutex<Option<JoinHandle<Result<(),K2Error>>>>>;
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
    pub fn create(name: String) -> Result<isize, K2Error> {
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
        self_instance.stream_block.add_output::<Result<(), K2Error>>("response".to_string())?;
        dbg!("Creating stream controller with name: {}", name.clone());
        let stream_id: isize;
        {
            let mut stream_id_counter = STREAM_ID_COUNTER.get_or_init(|| Mutex::new(0)).lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream id counter".into() })?;
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
        let mut stream_table = STREAM_TABLE.get_or_init(|| Mutex::new(Vec::new())).lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream table".into() })?;
        stream_table.push(Arc::new(Mutex::new(self_instance)));
        dbg!("Stream controller created with id: {}", stream_id);
        Ok(stream_id)
    }
    pub fn get_stream_by_id(id: isize) -> Result<Arc<Mutex<Self>>, K2Error> {
        let stream_table = STREAM_TABLE.get_or_init(|| Mutex::new(Vec::new())).lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream table".into() })?;
        let stream_lock = stream_table.get((id) as usize).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "Stream not found".into() })?;
        Ok(Arc::clone(stream_lock))
    }
    pub fn get_stream_by_name(_name: String) -> Result<Arc<Mutex<Self>>, K2Error> {
        unimplemented!()
    }
    pub fn register_commands(&mut self) -> Result<(), K2Error> {
        self.stream_block.add_command("init".to_string(), |proc| {
            let stream_controller = proc.as_any_mut().downcast_mut::<Self>().ok_or(K2Error { code: K2ErrorCode::InvalidValue, message: "Invalid processor type".into() })?;
            stream_controller.initialize()
        })?;
        self.stream_block.add_command("run".to_string(), |proc| {
            let stream_controller = proc.as_any_mut().downcast_mut::<Self>().ok_or(K2Error { code: K2ErrorCode::InvalidValue, message: "Invalid processor type".into() })?;
            Self::run(stream_controller.stream_id)
        })?;
        self.stream_block.add_command("stand-by".to_string(), |proc| {
            let stream_controller = proc.as_any_mut().downcast_mut::<Self>().ok_or(K2Error { code: K2ErrorCode::InvalidValue, message: "Invalid processor type".into() })?;
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
    pub fn run(stream_id: isize) -> Result<(), K2Error> {
        let stream = Self::get_stream_by_id(stream_id)?;
        let handle: JoinHandle<Result<(), K2Error>> = std::thread::spawn(move || {
            let mut stream = stream.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream".into() })?;
            {
                let mut state = stream.state.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream state".into() })?;
                if *state == StreamState::Uninitialized {
                    return Err(K2Error { code: K2ErrorCode::Uninitialized, message: "Stream is not initialized".into()});
                }
                *state = StreamState::Running;
            }
            loop {
                if let Err(e) = stream.process() {
                    let mut state = stream.state.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream state".into() })?;
                    *state = StreamState::Waiting;
                    return Err(e);
                }
                let state = stream.state.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream state".into() })?;
                if *state == StreamState::Waiting {
                    break;
                }
            }
            Ok(())
        });
        let stream = Self::get_stream_by_id(stream_id)?;
        stream.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream".into() })?.set_stream_handle(Arc::new(Mutex::new(Some(handle))));
        Ok(())
    }
    pub fn connect<T: 'static + Send + Sync + Clone> (&mut self, from: String, to: String) -> Result<(), K2Error> {
        let mut from_block = None;
        let mut to_block = None;
        let from_split: Vec<&str> = from.split(".").collect();
        let to_split: Vec<&str> = to.split(".").collect();
        let from_proc: String = from_split.get(0).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "From processor not found".into() })?.to_string();
        let to_proc: String = to_split.get(0).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "To processor not found".into() })?.to_string();
        let from_connector = from_split.get(1).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "From connector not found".into() })?.to_string();
        let to_connector = to_split.get(1).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "To connector not found".into() })?.to_string();
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
        Err(K2Error { code: K2ErrorCode::NotFound, message: "Failed to connect blocks".into() })
        
    }
    pub fn add_mode(&mut self, id: usize, mode: OperativeMode) -> Result<(), K2Error> {
        if self.modes.contains_key(&id) {
            Err(K2Error { code: K2ErrorCode::AlreadyExists, message: "Mode with this ID already exists".into() })
        } else {
            self.modes.insert(id, mode);
            Ok(())
        }
    }
    pub fn get_mode(&self, id: &usize) ->  Result<&OperativeMode, K2Error> {
        self.modes.get(id).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "Mode not found".into() })
    }
    pub fn get_mode_mut(&mut self, id: &usize) ->  Result<&mut OperativeMode, K2Error> {
        self.modes.get_mut(id).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "Mode not found".into() })
    }
    pub fn set_current_mode(&mut self, id: usize) -> Result<(), K2Error> {
        if self.modes.contains_key(&id) {
            let mode = self.modes.get_mut(&self.current_mode_id).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "Current mode not found".into() })?;
            mode.finalize()?;
            let mode = self.modes.get_mut(&id).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "Mode not found".into() })?;
            mode.initialize()?;
            mode.process()?;
            self.current_mode_id = id;
            Ok(())
        } else {
            Err(K2Error { code: K2ErrorCode::NotFound, message: "Mode not found".into() })
        }
    }
    pub fn add_command(id: isize, command: String, block_name: String, callback: Callback) -> Result<(), K2Error> {
        if id < 0 || id > *STREAM_ID_COUNTER.get_or_init(|| Mutex::new(0)).lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream ID counter".into() })? {
            return Err(K2Error { code: K2ErrorCode::InvalidValue, message: "Invalid stream ID".into() });
        }
        let mut stream_table = STREAM_TABLE.get_or_init(|| Mutex::new(Vec::new())).lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream table".into() })?;
        let stream_lock = stream_table.get_mut(id as usize).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "Stream not found".into() })?;
        let mut binding = stream_lock.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream".into() })?;
        let stream = binding.as_any_mut().downcast_mut::<Self>().ok_or(K2Error { code: K2ErrorCode::BadFormat, message: "Failed to downcast stream".into() })?;
        if stream.command_map.contains_key(&command) {
            return Err(K2Error { code: K2ErrorCode::AlreadyExists, message: "Command already exists".into() });
        }
        if !stream.processors.contains_key(&block_name) {
            return Err(K2Error { code: K2ErrorCode::NotFound, message: "Block does not exist".into() });
        }
        stream.command_map.insert(command.clone(), block_name);
        stream.commands_callback.insert(command, callback);
        Ok(())
    }
    pub fn execute_command(&mut self, command: String) -> Result<(), K2Error> {
        let block_name = self.command_map.get(&command).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "Command not found".into() })?;
        let block = self.processors.get_mut(block_name).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "Block not found".into() })?;
        let callback = self.commands_callback.get(&command).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "Callback not found".into() })?;
        (callback)(block.as_mut())
    }
    pub fn add_processor(&mut self, name: String, processor: Box<dyn ProcessorTrait>) -> Result<(), K2Error> {
        if self.processors.contains_key(&name.clone()) {
            return Err(K2Error { code: K2ErrorCode::AlreadyExists, message: "Processor with this name already exists".into() });
        }
        self.processors.insert( name, processor);
        Ok(())
    }
    pub fn get_processors(&self, processor_list: String) -> Result<&Box<dyn ProcessorTrait>, K2Error> {
        self.processors.get(&processor_list).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "Processor not found".into() })
    }
    pub fn get_processors_mut(&mut self, processor_list: String) -> Result<&mut Box<dyn ProcessorTrait>, K2Error> {
        self.processors.get_mut(&processor_list).ok_or(K2Error { code: K2ErrorCode::NotFound, message: "Processor not found".into() })
    }
}

impl ProcessorTrait for StreamController {
    fn new(_name: String) -> ProcessorNewReturn {
        Err(K2Error { code: K2ErrorCode::InvalidOperation, message: "Use the method StreamController::create(\"name\") instead".into() })
    }

    fn initialize(&mut self) -> Result<(), K2Error> {
        let mut state = self.state.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream state".into() })?;
        if self.stream_id == -1 {
            return Err(K2Error { code: K2ErrorCode::Uninitialized, message: "Stream ID is not set".into() });
        }
        if *state == StreamState::Running {
            return Err(K2Error { code: K2ErrorCode::NotAllowed, message: "Stream is already running".into() });
        }
        for mode in self.modes.values_mut() {
            mode.initialize()?;
        }
        *state = StreamState::Initialized;
        Ok(())
    }
    fn process(&mut self) -> Result<(), K2Error> {
        if self.stream_block.get_input::<String>(&"command".to_string())?.receive().is_ok() {
            let command = self.stream_block.get_input::<String>(&"command".to_string())?.receive()?;
            self.execute_command(command)?;
            if self.stream_block.get_output::<Result<(),K2Error>>(&"response".to_string())?.send(Ok(())).is_ok() {
                return Ok(());
            }
        }
        Err(K2Error { code: K2ErrorCode::ProcessError, message: "Failed to process command".into() })
    }
    fn finalize(&mut self) -> Result<(), K2Error> {
        let handle = self.stream_handle().lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock stream handle".into() })?.take();
        self.set_stream_handle(Arc::new(Mutex::new(None)));
        if let Some(handle) = handle {
            match handle.join() {
                Ok(result) => {

                    return result;
                }
                Err(_) => return Err(K2Error { code: K2ErrorCode::ProcessError, message: "Failed to join stream thread".into() }),
            }
        }
        Ok(())
    }
}