use std::{collections::HashMap, sync::{Arc, Mutex, MutexGuard, OnceLock}, thread::JoinHandle}; 


use crate::{connections::{Input, Output}, memory::{DataHeader, MemoryTrait}, modes::OperativeMode, processors::{ProcessorHeader, ProcessorTrait, StreamBlock}};

pub type Callback = fn (&mut dyn ProcessorTrait) -> Result<(), ()>;
pub type StreamProcessorHandle = Arc<Mutex<Option<JoinHandle<Result<(),()>>>>>;
type StreamTable = Mutex<Vec<Arc<Mutex<StreamController>>>>;
static STREAM_TABLE: OnceLock<StreamTable> = OnceLock::new();
static STREAM_ID_COUNTER: OnceLock<Mutex<isize>> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamState {
    Uninitialized,
    Initialized,
    Running,
    Waiting,
}
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

impl MemoryTrait for Input<String> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
impl MemoryTrait for Output<Result<(), ()>> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl StreamController {
    pub fn new() -> Result<Self, ()> {
        let mode = OperativeMode::new("default".to_string(), 0);
        let mut modes = HashMap::new();
        modes.insert(0, mode);
        
        let mut self_instance = Self {
            name: "StreamController".to_string(),
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
        
        Ok(self_instance)
    }
    pub fn register_stream(mut stream: Self) -> Result<(), ()> {
        let mut stream_table = STREAM_TABLE.get_or_init(|| Mutex::new(Vec::new())).lock().map_err(|_| ())?;
        let mut stream_id_counter = STREAM_ID_COUNTER.get_or_init(|| Mutex::new(0)).lock().map_err(|_| ())?;
        *stream_id_counter += 1;
        stream.stream_id = *stream_id_counter;
        stream.stream_block.set_stream_id(*stream_id_counter);
        stream.register_commands()?;
        stream_table.push(Arc::new(Mutex::new(stream)));
        Ok(())
    }
    pub fn get_stream(id: isize) -> Result<Arc<Mutex<Self>>, ()> {
        let stream_table = STREAM_TABLE.get_or_init(|| Mutex::new(Vec::new())).lock().map_err(|_| ())?;
        let stream_lock = stream_table.get((id - 1) as usize).ok_or(())?;
        Ok(Arc::clone(stream_lock))
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
        let stream = Self::get_stream(stream_id)?;
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
                if let Err(_) = stream.process() {
                    let mut state = stream.state.lock().map_err(|_| ())?;
                    *state = StreamState::Waiting;
                    return Err(());
                }
                let state = stream.state.lock().map_err(|_| ())?;
                if *state == StreamState::Waiting {
                    break;
                }
            }
            Ok(())
        });
        let stream = Self::get_stream(stream_id)?;
        stream.lock().map_err(|_|())?.set_stream_handle(Arc::new(Mutex::new(Some(handle))));
        Ok(())
    }
    pub fn connect<T: 'static + Clone> (&mut self, from: String, to: String) -> Result<(), ()> {
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
}

impl ProcessorTrait for StreamController {
    fn as_any(&self) -> &dyn std::any::Any {self}
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {self}
    fn name(&self) -> &DataHeader { &self.name}
    fn proc_name(&self) -> &String { &self.header().proc_name }
    fn header(&self) -> &ProcessorHeader { &self.header }
    fn lock() -> Result<MutexGuard<'static, Self>, ()> where Self: Sized {Err(())}
    fn get_proc_state(&self) -> Result<(), ()> {
        Ok(())
    }
    fn get_stream_block(&self) -> &StreamBlock {
        &self.stream_block
    }
    fn get_stream_block_mut(&mut self) -> &mut StreamBlock {
        &mut self.stream_block
    }
    fn initialize(&mut self) -> Result<(), ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        if self.stream_id == -1 {
            return Err(());
        }
        if *state == StreamState::Running {
            return Err(());
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