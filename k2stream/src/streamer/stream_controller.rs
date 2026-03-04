use std::{collections::HashMap, sync::{Arc, Mutex, MutexGuard, OnceLock}, thread::JoinHandle, time::Duration}; 

use processor_macro::K2ProcessorBlock;
use memory_macro::K2Memory;

use crate::{errors::{K2Error, K2ErrorCode}, k2err, processor::memory::{DataHeader, MemoryTrait}, streamer::modes::{ChainType, OperativeMode}, processor::processors::{ProcessorBlockTrait, ProcessorHeader, ProcessorNewReturn, ProcessorTrait, StreamBlock, StreamState}};

pub type Callback = fn (&mut dyn ProcessorTrait) -> Result<(), K2Error>;
pub type StreamProcessorHandle = Arc<Mutex<Option<JoinHandle<Result<(),K2Error>>>>>;

type ProcessorType = Arc<Mutex<Box<dyn ProcessorTrait>>>;
type StreamTable = Mutex<Vec<ProcessorType>>;
type MemoryTable = Mutex<HashMap<String, ProcessorType>>;

static STREAM_TABLE: OnceLock<StreamTable> = OnceLock::new();
static STREAM_ID_COUNTER: OnceLock<Mutex<isize>> = OnceLock::new();

static PROCESSOR_TABLE: OnceLock<MemoryTable> = OnceLock::new();

pub trait StreamConfigurationTrait: Send + Sync {
    fn set_mode_configuration(&mut self, mode: String) -> Result<(), K2Error>;
}
#[derive(K2Memory, K2ProcessorBlock)]
pub struct StreamController {
    pub name: String,
    pub header: ProcessorHeader,
    stream_id: isize,
    stream_block: StreamBlock,
    stream_configuration: Box<dyn StreamConfigurationTrait>,
    modes: HashMap<String, OperativeMode>,
    current_mode: String,
    command_map: HashMap<String, String>,
    commands_callback: HashMap<String, Callback>,
    state: Arc<Mutex<StreamState>>,
    stream_handle: StreamProcessorHandle,
}

impl StreamController {
    pub fn create(name: String, configuration: Box<dyn StreamConfigurationTrait>) -> Result<isize, K2Error> {
        let mut self_instance = Self {
            name: name.clone(),
            stream_id: -1_isize,
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
            stream_configuration: configuration,
            modes: HashMap::new(),
            current_mode: "default".to_string(),
            command_map: HashMap::new(),
            state: Arc::new(Mutex::new(StreamState::Uninitialized)),
            commands_callback: HashMap::new(),
            stream_handle: Arc::new(Mutex::new(None)),
        };
        self_instance.stream_block.add_input::<String>("command".to_string())?;
        self_instance.stream_block.add_output::<Result<(), K2Error>>("response".to_string())?;
        dbg!("Creating stream controller with name: {}", name.clone());
        let stream_id: isize;
        {
            let mut stream_id_counter = STREAM_ID_COUNTER.get_or_init(|| Mutex::new(0)).lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Failed to lock stream id counter"))?;
            *stream_id_counter += 1;
            dbg!(*stream_id_counter);
            stream_id = *stream_id_counter;
        }
        self_instance.stream_id = stream_id;
        dbg!("Set stream id: {}", stream_id);
        self_instance.stream_block.set_stream_id(stream_id);
        dbg!("Registering commands...");
        self_instance.register_commands()?;
        let mode = OperativeMode::new("default".to_string());
        self_instance.add_mode(mode)?;
        {
            let mut processor_table = StreamController::get_processor_table().lock().map_err(|_| k2err!(K2ErrorCode::LockError,""))?;
            processor_table.insert(name.clone(), Arc::new(Mutex::new(Box::new(self_instance))));
        }
        dbg!("Registering stream controller in table...");
        {
            let mut stream_table = STREAM_TABLE.get_or_init(|| Mutex::new(Vec::new())).lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Failed to lock stream table"))?;
            let self_instance = StreamController::get_processor(&name)?;
            stream_table.push(self_instance.clone());
        }
        
        dbg!("Stream controller created with id: {}", stream_id);
        dbg!("Ending StreamController create...");
        Ok(stream_id)
    }
    fn get_stream_id(&self) -> isize {
        self.stream_id
    }
    pub fn get_stream_by_id(id: isize) -> Result<ProcessorType, K2Error> {
        let stream_table = STREAM_TABLE.get_or_init(|| Mutex::new(Vec::new())).lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Failed to lock stream table"))?;
        let stream_lock = stream_table.get((id-1) as usize)
            .ok_or(k2err!(K2ErrorCode::NotFound, format!("Stream {} not found", id)))?;
        Ok(Arc::clone(stream_lock))
    }
    pub fn get_stream_by_name(name: String) -> Result<ProcessorType, K2Error> {
        Ok(StreamController::get_processor(&name.clone())?)
    }
    pub fn register_commands(&mut self) -> Result<(), K2Error> {
        
        self.command_map.insert("init".to_string(), self.name().clone());
        self.commands_callback.insert("init".to_string(), |proc| proc.initialize());
        self.command_map.insert("run".to_string(), self.name().clone());
        self.commands_callback.insert("run".to_string(), |proc | {
            let stream_cntr = proc.as_any().downcast_ref::<Self>().ok_or(
                k2err!(K2ErrorCode::InvalidOperation, "Mismatched type"))?;
            Self::run(stream_cntr.get_stream_id())
        });
        self.command_map.insert("stand-by".to_string(), self.name().clone());
        self.commands_callback.insert("stand-by".to_string(), |proc| proc.finalize());
        Ok(())
    }
    pub fn stream_handle(&self) -> StreamProcessorHandle {
        self.stream_handle.clone()
    }
    pub fn set_stream_handle(&mut self, handle: StreamProcessorHandle) {
        self.stream_handle = handle;
    }
    pub fn run(stream_id: isize) -> Result<(), K2Error> {
        let stream = Self::get_stream_by_id(stream_id)?.clone();
        let handle: JoinHandle<Result<(), K2Error>> = std::thread::spawn(move || {
            let mut stream = stream.lock().map_err(|_| k2err!(K2ErrorCode::LockError, ""))?;
            let stream = stream.as_any_mut().downcast_mut::<Self>().ok_or(k2err!(K2ErrorCode::BadFormat, ""))?;
            {
                let mut state = stream.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Failed to lock stream state"))?;
                if *state == StreamState::Uninitialized {
                    return Err(k2err!(K2ErrorCode::Uninitialized, "Stream is not initialized"));
                }
                *state = StreamState::Running;
            }
            loop {
                if let Err(e) = stream.process() {
                    let mut state = stream.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Failed to lock stream state"))?;
                    *state = StreamState::Waiting;
                    return Err(e);
                }
                let state = stream.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Failed to lock stream state"))?;
                if *state == StreamState::Waiting {
                    break;
                }
            }
            Ok(())
        });
        
        let stream = Self::get_stream_by_id(stream_id)?.clone();
        let mut stream = stream.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Failed to lock stream"))?;
        let stream = stream.as_any_mut().downcast_mut::<Self>().ok_or(k2err!(K2ErrorCode::LockError, ""))?;
        stream.set_stream_handle(Arc::new(Mutex::new(Some(handle))));
        for modes in stream.modes.values_mut() {
            modes.process()?;
        }
        Ok(())
    }
    pub fn add_mode(&mut self, mut mode: OperativeMode) -> Result<(), K2Error> {
        let mode_name = mode.name.clone();
        if self.modes.contains_key(&mode_name) {
            Err(k2err!(K2ErrorCode::AlreadyExists, "Mode with this ID already exists"))
        } else {
            mode.set_stream_id(self.get_stream_id())?;
            self.modes.insert(mode_name.clone(), mode);
            let mode = self.modes.get(&mode_name).unwrap();
            dbg!(mode.get_stream_id());
            Ok(())
        }
    }
    pub fn get_mode(&self, name: &String) ->  Result<&OperativeMode, K2Error> {
        self.modes.get(name).ok_or(k2err!(K2ErrorCode::NotFound, "Mode not found"))
    }
    pub fn get_mode_mut(&mut self, name: &String) ->  Result<&mut OperativeMode, K2Error> {
        self.modes.get_mut(name).ok_or(k2err!(K2ErrorCode::NotFound, "Mode not found"))
    }
    pub fn set_current_mode(&mut self, name: &String) -> Result<(), K2Error> {
        if self.modes.contains_key(name) {
            let mode = self.modes.get_mut(&self.current_mode).ok_or(k2err!(K2ErrorCode::NotFound, "Current mode not found"))?;
            mode.finalize()?;
            let mode = self.modes.get_mut(name).ok_or(k2err!(K2ErrorCode::NotFound, "Mode not found"))?;
            self.stream_configuration.set_mode_configuration(mode.name.clone())?;
            mode.initialize()?;
            mode.process()?;
            self.current_mode = name.clone();
            Ok(())
        } else {
            Err(k2err!(K2ErrorCode::NotFound, "Mode not found"))
        }
    }
    pub fn get_processor_table() -> &'static MemoryTable {
        PROCESSOR_TABLE.get_or_init( || Mutex::new(HashMap::new()))
    }
    pub fn get_processor(name: &String) -> Result<Arc<Mutex<Box<dyn ProcessorTrait>>>, K2Error>
    {
        let proc_table = StreamController::get_processor_table().lock()
            .map_err(|_| k2err!(K2ErrorCode::LockError, "Error on PROCESSOR_TABLE locking"))?;
        if let Some(proc) = proc_table.get(&name.clone()) {
            Ok(proc.clone())
        } else {
            Err(k2err!(K2ErrorCode::NotFound, format!("Processor {} not found", name)))
        }
    }
    pub fn get_processor_list() -> Result<Vec<Arc<Mutex<Box<dyn ProcessorTrait>>>>, K2Error>
    {
        let proc_table = StreamController::get_processor_table();
        let mut proc_list: Vec<Arc<Mutex<Box<dyn ProcessorTrait>>>> = Vec::new();
        let proc_lock = proc_table.lock().map_err(|_|
            k2err!(K2ErrorCode::LockError, "Error in lock of processor table"))?;
        for proc in proc_lock.values() {
            proc_list.push(proc.clone())
        }
        Ok(proc_list)
    }
    pub fn add_processor(&mut self, chain: &ChainType, name: String, mut processor: Box<dyn ProcessorTrait>) -> Result<(), K2Error> {
        let mut proc_table = PROCESSOR_TABLE.get_or_init(|| Mutex::new(HashMap::new())).lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Failed to lock stream table"))?;
        if proc_table.contains_key(&name.clone()) {
            return Err(k2err!(K2ErrorCode::AlreadyExists, "Processor with this name already exists"));
        }
        processor.get_stream_block_mut().set_stream_id(self.get_stream_id());
        match chain.lock() {
            Ok(mut chain) => {
                processor.get_stream_block_mut().set_task_id(chain.get_task_id());
                (*chain).add_block(name.clone(), processor.get_stream_block())?;
            }
            Err(_) => {
                return Err(k2err!(K2ErrorCode::LockError, "".to_string()));
            }
        }
        proc_table.insert( name, Arc::new(Mutex::new(processor)));
        Ok(())
    }
    pub fn connect<T: 'static + Send + Sync + Clone> (&mut self, from: String, to: String) -> Result<(), K2Error> {
        let from_split: Vec<&str> = from.split(".").collect();
        let to_split: Vec<&str> = to.split(".").collect();
        let from_proc: String = from_split.first().ok_or(k2err!(K2ErrorCode::NotFound, "From processor not found"))?.to_string();
        let to_proc: String = to_split.first().ok_or(k2err!(K2ErrorCode::NotFound, "To processor not found"))?.to_string();
        let from_connector = from_split.get(1).ok_or(k2err!(K2ErrorCode::NotFound, "From connector not found"))?.to_string();
        let to_connector = to_split.get(1).ok_or(k2err!(K2ErrorCode::NotFound, "To connector not found"))?.to_string();
        let binding = StreamController::get_processor(&from_proc.clone())?;
        let mut from_processor = binding.lock().map_err(|_|
            k2err!(K2ErrorCode::LockError, format!("Can't get lock on processor {}", from_proc)))?;
        let binding = StreamController::get_processor(&to_proc.clone())?;
        let mut to_processor = binding.lock().map_err(|_|
            k2err!(K2ErrorCode::LockError, format!("Can't get lock on processor {}", to_proc)))?;
        
        let from_block = from_processor.get_stream_block_mut();
        let to_block = to_processor.get_stream_block_mut();
        from_block.connect::<T>(&from_connector, &to_connector, to_block)?;
        Ok(())
    }
    pub fn add_command(&mut self, command: String, block_name: String, callback: Callback) -> Result<(), K2Error> {
        if self.command_map.contains_key(&command) {
            return Err(k2err!(K2ErrorCode::AlreadyExists, "Command already exists"));
        }
        if !StreamController::get_processor_table()
            .lock()
            .map_err(|_|k2err!(K2ErrorCode::LockError,"Fail to lock processor table".to_string()))?
            .contains_key(&block_name) {
            return Err(k2err!(K2ErrorCode::NotFound, "Block does not exist"));
        }
        self.command_map.insert(command.clone(), block_name);
        self.commands_callback.insert(command, callback);
        Ok(())
    }
    pub fn execute_command(&mut self, command: String) -> Result<(), K2Error> {
        let block_name = self.command_map.get(&command).ok_or(k2err!(K2ErrorCode::NotFound, "Command not found"))?;
        let bind = StreamController::get_processor(block_name)?;
        let mut processor = bind.lock()
            .map_err(|_| k2err!(K2ErrorCode::LockError, "Unable to lock processor"))?;
        let callback = self.commands_callback.get(&command).ok_or(k2err!(K2ErrorCode::NotFound, "Callback not found"))?;
        (callback)(processor.as_mut())
    }
}

impl ProcessorTrait for StreamController {
    fn new(_name: String) -> ProcessorNewReturn {
        Err(k2err!(K2ErrorCode::InvalidOperation, "Use the method StreamController::create(\"name\") instead"))
    }

    fn initialize(&mut self) -> Result<(), K2Error> {
        dbg!("Reading state...");
        let mut state = self.state.lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Failed to lock stream state"))?;
        if self.stream_id == -1 {
            return Err(k2err!(K2ErrorCode::Uninitialized, "Stream ID is not set"));
        }
        if *state == StreamState::Running {
            return Err(k2err!(K2ErrorCode::NotAllowed, "Stream is already running"));
        }
        let proc_table = Self::get_processor_table();
        let proc_table = proc_table.lock().map_err(|_| k2err!(K2ErrorCode::LockError,"")).unwrap();
        let proc_list: Vec<&Arc<Mutex<Box<dyn ProcessorTrait + 'static>>>> = proc_table.iter().filter(|&(key,_)| *key != self.name).map(|(_,v)| v).collect();
        for block in proc_list {
            println!("Block: {}", block.lock().unwrap().name());
            match block.lock() {
                Ok(mut block)=> { block.initialize()? },
                Err(_) => {return Err(k2err!(K2ErrorCode::LockError, format!("Unable to lock processor")));}
            };
        }
        dbg!("Initializing modes...");
        for mode in self.modes.values_mut() {
            dbg!(mode.get_stream_id());
            mode.initialize()?;
        }
        dbg!("End initilize...");
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
        Err(k2err!(K2ErrorCode::ProcessError, "Failed to process command"))
    }
    fn finalize(&mut self) -> Result<(), K2Error> {
        let handle = self.stream_handle().lock().map_err(|_| k2err!(K2ErrorCode::LockError, "Failed to lock stream handle"))?.take();
        self.set_stream_handle(Arc::new(Mutex::new(None)));
        if let Some(handle) = handle {
            match handle.join() {
                Ok(result) => {

                    return result;
                }
                Err(_) => return Err(k2err!(K2ErrorCode::ProcessError, "Failed to join stream thread")),
            }
        }
        std::thread::sleep(Duration::from_millis(100));
        dbg!("Finalizing mode");
        for mode in self.modes.values_mut() {
            mode.finalize()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc;

    use crate::streamer::modes::Chain;

    use super::*;
    pub struct TestConfiguration {}

    impl StreamConfigurationTrait for TestConfiguration {
        fn set_mode_configuration(&mut self, _mode: String) -> Result<(), K2Error> {
            Ok(())
        }
    }
    #[derive(K2Memory, K2ProcessorBlock)]
    pub struct TestProcessor {
        name: String,
        header: ProcessorHeader,
        stream_block: StreamBlock,
        state: Arc<Mutex<StreamState>>,
    }
    impl TestProcessor {
        pub fn callback(&self) -> Result<(), K2Error> {
            println!("Name: {}", self.name());
            Ok(())
        }
    }

    impl ProcessorTrait for TestProcessor {
        fn new(name: String) -> ProcessorNewReturn {
            let mut self_instance = Self {
                name: name.clone(),
                header: ProcessorHeader {
                    proc_name: "TestProcessor".to_string(),
                    description: "A fake processor with no functionality".to_string(),
                    version: "0.1.0".to_string(),
                    author: "Sofia".to_string(),
                    email: "ms.sofia.silvestri@gmail.com".to_string(),
                    license: "LGPLv2.0".to_string(),
                    repository: "".to_string(),
                },
                stream_block: StreamBlock::new(),
                state: Arc::new(Mutex::new(StreamState::Uninitialized)),
            };
            self_instance.get_stream_block_mut().add_input::<i64>("input".to_string())?;
            self_instance.get_stream_block_mut().add_output::<i64>("output".to_string())?;

            Ok(Box::new(self_instance))
        }
        fn initialize(&mut self ) -> Result<(), K2Error> {
            Ok(()) // INITIALIZE_CODE
        }
        fn process(&mut self) -> Result<(), K2Error> {
            let a = self.get_stream_block_mut().receive_input::<i64>(&"input".to_string())?;
            self.get_stream_block_mut().send_output::<i64>(&"output".to_string(), a+1)?;
            Ok(())
        }
        fn finalize(&mut self) -> Result<(), K2Error> {
            Ok(()) // FINALIZE_CODE
        }
    }
    static TEST_MUTEX: Mutex<()> = Mutex::new(());
    #[test]
    fn create_stream_controller() {
        let _lock = TEST_MUTEX.lock();
        let stream_id: Result<isize, K2Error> = StreamController::create("test".to_string(), Box::new(TestConfiguration{}));
        assert!(stream_id.is_ok());
        assert!(StreamController::new("test".to_string()).is_err());
        let stream_cntr = StreamController::get_stream_by_id(stream_id.clone().unwrap());
        assert!(stream_cntr.is_ok());
        let stream_cntr = stream_cntr.unwrap();
        let stream_cntr = stream_cntr.lock();
        assert!(stream_cntr.is_ok());
        let mut stream_cntr = stream_cntr.unwrap();
        let stream_cntr = stream_cntr.as_any_mut().downcast_mut::<StreamController>();
        assert!(stream_cntr.is_some());
        assert_eq!(stream_cntr.unwrap().get_stream_id(), stream_id.clone().unwrap());
    }
    #[test]
    fn mode_cntr() {
        let _lock = TEST_MUTEX.lock();
        let stream_id: Result<isize, K2Error> = StreamController::create("test".to_string(), Box::new(TestConfiguration{}));
        assert!(stream_id.is_ok());
        let stream_cntr = StreamController::get_stream_by_id(stream_id.clone().unwrap());
        assert!(stream_cntr.is_ok());
        let stream_cntr = stream_cntr.unwrap();
        let mut stream_cntr = stream_cntr.lock().unwrap();
        let stream_cntr = stream_cntr.as_any_mut().downcast_mut::<StreamController>();
        assert!(stream_cntr.is_some());
        let stream_cntr = stream_cntr.unwrap();
        let mode = OperativeMode::new("modo1".to_string());
        assert!(stream_cntr.add_mode(mode).is_ok());
        let mode = OperativeMode::new("modo1".to_string());
        assert!(stream_cntr.add_mode(mode).is_err());
        let mode = stream_cntr.get_mode_mut(&"modo1".to_string());
        assert!(mode.is_ok());
        let mode = mode.unwrap();
        assert!(mode.add_chain("name".to_string(), Arc::new(Mutex::new(Chain::new("name".to_string())))).is_ok());
        let mode = stream_cntr.get_mode(&"modo1".to_string());
        assert!(mode.is_ok());
        let mode = mode.unwrap();
        assert!(mode.get_chain(&"name".to_string()).is_ok());
        assert!(stream_cntr.set_current_mode(&"modo1".to_string()).is_ok());
        let mode = stream_cntr.get_mode(&"modo2".to_string());
        assert!(mode.is_err());
        assert!(stream_cntr.set_current_mode(&"modo2".to_string()).is_err());
    }
    #[test]
    fn processor_cntr() {
        let _lock = TEST_MUTEX.lock();
        StreamController::get_processor_table().lock().unwrap().clear();
        dbg!("Create proc");
        let proc = TestProcessor::new("test".to_string());
        assert!(proc.is_ok());
        dbg!("Create chain");
        let chain = Arc::new(Mutex::new(Chain::new("test".to_string())));
        dbg!("Create mode");
        let mut mode = OperativeMode::new("test".to_string());
        dbg!("Append chain to mode");
        assert!(mode.add_chain("test".to_string(), chain.clone()).is_ok());
        dbg!("Create stream controller");
        let stream_id: Result<isize, K2Error> = StreamController::create("test".to_string(), Box::new(TestConfiguration{}));
        assert!(stream_id.is_ok());
        dbg!("Get stream controller arc");
        let stream_cntr = StreamController::get_stream_by_id(stream_id.clone().unwrap());
        assert!(stream_cntr.is_ok());
        let stream_cntr_arc = stream_cntr.unwrap();
        {
            dbg!("Add mode to stream controller");
            let mut stream_cntr = stream_cntr_arc.lock().unwrap();
            let stream_cntr = stream_cntr.as_any_mut().downcast_mut::<StreamController>();
            assert!(stream_cntr.is_some());
            assert!(stream_cntr.unwrap().add_mode(mode).is_ok());
        }
        {
            dbg!("Add processor to stream controller");
            let mut stream_cntr = stream_cntr_arc.lock().unwrap();
            let stream_cntr = stream_cntr.as_any_mut().downcast_mut::<StreamController>();
            assert!(stream_cntr.is_some());
            let ret = stream_cntr.unwrap().add_processor(&chain.clone(), "test".to_string(), proc.unwrap());
            match ret {
                Ok(_) => {},
                Err(e) => {eprintln!("{}", e.message);}
            }
        }
        dbg!("Get processor from stream controller");
        let proc = StreamController::get_processor(&"test".to_string());
        assert!(proc.is_ok());
        let proc = proc.unwrap();
        {
            let mut proc = proc.lock().unwrap();
            assert_eq!(proc.name(), &"test".to_string());
            dbg!("Adding input");
            assert!(proc.get_stream_block_mut().add_input::<String>("test".to_string()).is_ok());
        }
        {
        let proc = StreamController::get_processor(&"test".to_string());
        assert!(proc.is_ok());
        let proc = proc.unwrap();
        let proc = proc.lock().unwrap();
        dbg!("Verifying input");
        assert!(proc.get_stream_block().get_input::<String>(&"test".to_string()).is_ok());
        }
        dbg!("Get error processor to stream controller");
        let proc = StreamController::get_processor(&"test_1".to_string());
        assert!(proc.is_err());
        let proc = TestProcessor::new("test".to_string());
        assert!(proc.is_ok());
        {
            dbg!("Add error processor to stream controller");
            let mut stream_cntr = stream_cntr_arc.lock().unwrap();
            let stream_cntr = stream_cntr.as_any_mut().downcast_mut::<StreamController>().unwrap();
            assert!(stream_cntr.add_processor(&chain.clone(), "test".to_string(), proc.unwrap()).is_err());
            dbg!("Add processor to stream controller");
            let proc = TestProcessor::new("test_1".to_string());
            assert!(stream_cntr.add_processor(&chain.clone(), "test_a".to_string(), proc.unwrap()).is_ok());
        }
        let proc_list = StreamController::get_processor_list();
        assert!(proc_list.is_ok());
        assert_eq!(proc_list.unwrap().len(), 2);
    }
    #[test]
    fn operation_mode() {
        let _lock = TEST_MUTEX.lock();
        StreamController::get_processor_table().lock().unwrap().clear();
        let (output_sender, output_receiver) = mpsc::sync_channel::<i64>(10);
        let proc_1 = TestProcessor::new("test_1".to_string()).unwrap();
        let input_sender = proc_1.get_stream_block().get_input::<i64>(&"input".to_string());
        assert!(input_sender.is_ok());
        let input_sender = input_sender.unwrap().get_sender();
        let mut proc_2 = TestProcessor::new("test_2".to_string()).unwrap();
        proc_2.get_stream_block_mut().get_output_mut(&"output".to_string()).unwrap().connect(output_sender);
        let chain = Arc::new(Mutex::new(Chain::new("test_chain".to_string())));
        let mut mode = OperativeMode::new("Mode_1".to_string());
        assert!(mode.add_chain("test_chain".to_string(), chain.clone()).is_ok());
        let stream_id = StreamController::create("test_stream".to_string(), Box::new(TestConfiguration{}));
        let stream_cntr = StreamController::get_stream_by_id(stream_id.clone().unwrap());
        let stream_cntr = stream_cntr.unwrap();
        let mut stream_cntr = stream_cntr.lock().unwrap();
        let stream_cntr = stream_cntr.as_any_mut().downcast_mut::<StreamController>().unwrap();
        assert!(stream_cntr.add_mode(mode).is_ok());
        assert!(stream_cntr.add_processor(&chain.clone(), "test_1".to_string(), proc_1).is_ok());
        assert!(stream_cntr.add_processor(&chain.clone(), "test_2".to_string(), proc_2).is_ok());
        let ret = stream_cntr.connect::<i64>("test_1.output".to_string(), "test_2.input".to_string());
        match ret {
            Ok(_) => {},
            Err(e) => {
                eprintln!("{}", e.message);
                assert!(false);
            }
        }
        //assert!(stream_cntr.connect::<i64>("test_1.output".to_string(), "test_2.input".to_string()).is_ok());
        match stream_cntr.initialize() {
            Ok(_) => {},
            Err(e) => {eprintln!("{}", e.message); assert!(false);}
        }
        assert!(stream_cntr.set_current_mode(&"Mode_1".to_string()).is_ok());
        let _handle = std::thread::spawn( move || {
            let _ = StreamController::run(stream_id.unwrap());      
        });
        std::thread::spawn(move || {
            for i in 0..10000 {
                std::thread::sleep(Duration::from_millis(10));
                println!("Sending {}", i);
                assert!(input_sender.send(i).is_ok());
                let out = output_receiver.recv_timeout(Duration::from_millis(100));
                assert!(out.is_ok());
                assert_eq!(out.unwrap(), i+2);
                println!("Reveing {}", out.unwrap());
            }
        });
        
        assert!(stream_cntr.finalize().is_ok());
    }
    #[test]
    fn command_test() {
        let _lock = TEST_MUTEX.lock();
        let proc_1 = TestProcessor::new("test_1".to_string()).unwrap();
        let chain = Arc::new(Mutex::new(Chain::new("test_chain".to_string())));
        let mut mode = OperativeMode::new("Mode_1".to_string());
        assert!(mode.add_chain("test_chain".to_string(), chain.clone()).is_ok());
        let stream_id = StreamController::create("test_stream".to_string(), Box::new(TestConfiguration{}));
        let stream_cntr = StreamController::get_stream_by_id(stream_id.clone().unwrap());
        let stream_cntr = stream_cntr.unwrap();
        let mut stream_cntr = stream_cntr.lock().unwrap();
        let stream_cntr = stream_cntr.as_any_mut().downcast_mut::<StreamController>().unwrap();
        assert!(stream_cntr.add_mode(mode).is_ok());
        assert!(stream_cntr.add_processor(&chain.clone(), "test_1".to_string(), proc_1).is_ok());
        assert!(stream_cntr.add_command(
            "test.callback".to_string(), 
            "test_1".to_string(), 
            |proc| TestProcessor::callback(
                proc.as_any()
                .downcast_ref::<TestProcessor>()
                .unwrap()))
            .is_ok());
        assert!(stream_cntr.add_command(
            "test.callback".to_string(), 
            "test".to_string(), 
            |proc| TestProcessor::callback(
                proc.as_any()
                .downcast_ref::<TestProcessor>()
                .unwrap()))
            .is_err());
        assert!(stream_cntr.add_command(
            "test.callback".to_string(), 
            "test_1".to_string(), 
            |proc| TestProcessor::callback(
                proc.as_any()
                .downcast_ref::<TestProcessor>()
                .unwrap()))
            .is_err());
        let ret = stream_cntr.execute_command("test.callback".to_string());
        match ret {
            Ok(_) => {},
            Err(e) => {
                eprint!("{}: {}", e.code, e.message);
                assert!(false)
            }
            
        }
        assert!(stream_cntr.execute_command("test_1.callback".to_string()).is_err());
    }
}