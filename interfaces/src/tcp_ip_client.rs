use std::{collections::HashMap, fmt, sync::{Arc, Mutex, MutexGuard}};
use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;

use k2stream::{memory::{DataHeader, MemoryTrait}, processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorTrait, StreamBlock, StreamState}};

#[derive(K2Memory, K2ProcessorBlock)]
pub struct TcpIpClient {
    pub name: String,
    pub header: ProcessorHeader,
    stream_block: StreamBlock,
    state: Arc<Mutex<StreamState>>,
}
impl TcpIpClient {
    pub fn new(name: String) -> Result<Self, ()> {
        let mut ret = Self {
            name: name.clone(),
            header: ProcessorHeader {
                proc_name: "".to_string(),
                description: "".to_string(),
                version: "0.1.0".to_string(),
                author: "".to_string(),
                email: "".to_string(),
                license: "".to_string(),
                repository: "".to_string(),
            },
            stream_block: StreamBlock::new(),
            state: Arc::new(Mutex::new(StreamState::Initialized)),
        }
        Ok(ret)
    }
}
        