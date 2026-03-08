use std::{collections::HashMap, sync::{Arc, Mutex}};

use k2stream::{errors::{K2Error, K2ErrorCode}, k2err, processor::processors::ProcessorTrait, streamer::{modes::{Chain, OperativeMode}, stream_controller::{StreamConfigurationTrait, StreamController}}};

pub type Callback = fn (&mut StreamConfiguration) -> Result<(), K2Error>;

pub struct StreamConfiguration {
    chains_modes_table: HashMap<String, String>,
    blocks_chains_table: HashMap<String, Arc<Mutex<Chain>>>,
    blocks: HashMap<String, Box<dyn ProcessorTrait>>,
    mode_callback: HashMap<String, Callback>,
    stream_id: isize,
}

impl StreamConfiguration {
    pub fn new() -> Self {
        Self {
            modes: HashMap::new(),
            chains: HashMap::new(),
            blocks: HashMap::new(),
            mode_callback: HashMap::new(),
            stream_id: -1,
        }
    }
    pub fn configure(&mut self, id: isize) -> Result<(), K2Error> {
        self.stream_id = id;
        self.create_blocks();
        self.create_chains();
        self.create_modes();
        self.add_chain_to_mode();
        self.add_block_to_chain();
        Ok(())
    }
}
impl StreamConfigurationTrait for StreamConfiguration {
    fn set_mode_configuration(&mut self, mode: String) -> Result<(), K2Error> {
        let callback = self.mode_callback.get_mut(&mode).ok_or(k2err!(K2ErrorCode::NotFound, format!("Mode {} not found", mode)))?;
        callback(self)
    }
}
impl StreamConfiguration {
    fn create_blocks(&mut self) {

    }
    fn create_chains(&mut self) {

    }
    fn create_modes(&mut self) {
        
    }
    fn add_block_to_chain(&mut self) {

    }
    fn add_chain_to_mode(&mut self) {
        let stream = StreamController::get_stream_by_id(self.stream_id)?;
        let mut stream = stream.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "".to_string()))?;
        let stream = stream.as_any_mut().downcast_mut::<StreamController>().unwrap();
        stream.get_mode_mut();
    }
    fn add_modes_to_stream(&mut self) -> Result<(), K2Error>{
        let stream = StreamController::get_stream_by_id(self.stream_id)?;
        let mut stream = stream.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "".to_string()))?;
        let stream = stream.as_any_mut().downcast_mut::<StreamController>().unwrap();
        for mode in self.modes.values() {
            stream.add_mode(mode.clone());
        }
        Ok(())
    }
}
