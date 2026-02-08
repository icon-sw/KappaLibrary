use std::{collections::HashMap, sync::{Mutex, MutexGuard, OnceLock}}; 

use crate::{connections::{Input, Output}, memory::{DataHeader, MemoryTrait}, modes::OperativeMode, processors::{ProcessorHeader, ProcessorTrait, StreamBlock}};

pub type Callback = fn (&mut StreamBlock) -> Result<(), ()>;

static STREAM_TABLE: OnceLock<Mutex<Vec<StreamController>>> = OnceLock::new();
static STREAM_ID_COUNTER: OnceLock<Mutex<isize>> = OnceLock::new();

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
        self.stream_block.add_input::<String>("command".to_string())?;
        self.stream_block.add_output::<Result<(), ()>>("response".to_string())?;
        Ok(())
    }
    fn process(&mut self) -> Result<(), ()> {
        let mut return_value: Result<(), ()> = Err(());
        if self.stream_block.get_input::<String>(&"command".to_string())?.receive().is_ok() {
            let command = self.stream_block.get_input::<String>(&"command".to_string())?.receive()?;
            self.execute_command(command)?;
            if self.stream_block.get_output::<Result<(),()>>(&"response".to_string())?.send(Ok(())).is_ok() {
                return_value = Ok(());
            }
        }
        return_value
    }
    fn finalize(&mut self) -> Result<(), ()> {
         Ok(())
    }
}
impl StreamController {
    pub fn new() -> Self {
        let mode = OperativeMode::new("default".to_string(), 0);
        let mut modes = HashMap::new();
        modes.insert(0, mode);
        Self {
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
            processors: HashMap::new(),
            commands_callback: HashMap::new(),
        }
    }
    pub fn register_stream(mut stream: Self) -> Result<(), ()> {
        let mut stream_table = STREAM_TABLE.get_or_init(|| Mutex::new(Vec::new())).lock().map_err(|_| ())?;
        let mut stream_id_counter = STREAM_ID_COUNTER.get_or_init(|| Mutex::new(0)).lock().map_err(|_| ())?;
        *stream_id_counter += 1;
        stream.stream_id = *stream_id_counter;
        stream_table.push(stream);
        Ok(())
    }
    pub fn run(&mut self) -> Result<(), ()> {
        if self.stream_id == -1 {
            return Err(());
        }
        self.initialize()?;
        self.process()?;
        self.finalize()?;
         Ok(())
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
        let stream = stream_lock.as_any_mut().downcast_mut::<Self>().ok_or(())?;
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
        (callback)(block.as_mut().get_stream_block_mut())
    }
}