use std::sync::{Arc, Mutex, MutexGuard};

use k2stream::processor::processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState};
use k2stream::{errors::{K2ErrorCode, K2Error}, processor::memory::{DataHeader, MemoryTrait}};
use memory_macro::K2Memory;
use processor_macro::K2ProcessorBlock;
// END_K2_IMPORT

// END_USER_IMPORT
#[derive(K2Memory, K2ProcessorBlock)]
pub struct FakeProcessor {
    name: String,
    header: ProcessorHeader,
    stream_block: StreamBlock,
    state: Arc<Mutex<StreamState>>,
// USER_STRUCT
}


impl ProcessorTrait for FakeProcessor {
    fn new(name: String) -> ProcessorNewReturn {
        let self_instance = Self {
            // START_K2_INIT
            name: name.clone(),
            header: ProcessorHeader {
                proc_name: "FakeProcessor".to_string(),
                description: "A fake processor with no functionality".to_string(),
                version: "0.1.0".to_string(),
                author: "Sofia".to_string(),
                email: "ms.sofia.silvestri@gmail.com".to_string(),
                license: "LGPLv2.0".to_string(),
                repository: "".to_string(),
            },
            stream_block: StreamBlock::new(),
            state: Arc::new(Mutex::new(StreamState::Uninitialized)),
            // END_K2_INIT
            // END_USER_INIT
        };
        // END_K2_MEMBER_CREATION
        // END_USER_MEMBER_CREATION
        Ok(Box::new(self_instance))
    }
    fn initialize(&mut self ) -> Result<(), K2Error> {
        Ok(()) // INITIALIZE_CODE
    }
    fn process(&mut self) -> Result<(), K2Error> {
        Ok(()) // PROCESS_CODE
    }
    fn finalize(&mut self) -> Result<(), K2Error> {
        Ok(()) // FINALIZE_CODE
    }
}
