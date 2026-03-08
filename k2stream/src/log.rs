use std::{collections::HashMap, fmt, io::Write, net::UdpSocket, process::Command, sync::{Arc, Mutex, OnceLock, mpsc::{Receiver, SyncSender, sync_channel}}};

use chrono::{DateTime, Datelike, Utc};
use regex::Regex;

use crate::{errors::{K2Error, K2ErrorCode}, k2err, streamer::task_monitor::TaskMonitor};

#[derive(Clone, PartialEq, PartialOrd)]
pub enum K2LogLevel {
    Emergency,
    Alert,
    Critical,
    Error,
    Warning,
    Notice,
    Info,
    Debug,
    Verbose,
}

impl TryFrom<u8> for  K2LogLevel {
    type Error = K2Error;
    fn try_from(value: u8) -> Result<Self, <Self as TryFrom<u8>>::Error> {
        match value {
            0 => Ok(K2LogLevel::Emergency),
            1 => Ok(K2LogLevel::Alert),
            2 => Ok(K2LogLevel::Critical),
            3 => Ok(K2LogLevel::Error),
            4 => Ok(K2LogLevel::Warning),
            5 => Ok(K2LogLevel::Notice),
            6 => Ok(K2LogLevel::Info),
            7 => Ok(K2LogLevel::Debug),
            8 => Ok(K2LogLevel::Verbose),
            _ => Err(k2err!(K2ErrorCode::OutOfRange, "Value out of range".to_string())),
        }
    }
}

impl fmt::Display for K2LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (color_code, label) = match self {
            K2LogLevel::Verbose   => ("\x1b[37m", "[VERBOSE]"),  // White
            K2LogLevel::Debug     => ("\x1b[37m", "[DEBUG]"),    // White
            K2LogLevel::Info      => ("\x1b[36m", "[INFO]"),     // Cyan
            K2LogLevel::Notice    => ("\x1b[32m", "[NOTICE]"),   // Blue
            K2LogLevel::Warning   => ("\x1b[33m", "[WARNING]"),  // Yellow
            K2LogLevel::Error     => ("\x1b[31m", "[ERROR]"),    // Red
            K2LogLevel::Critical  => ("\x1b[1;31m", "[CRITICAL]"),// Bold Red
            K2LogLevel::Alert     => ("\x1b[1;31m", "[ALERT]"),// Bold Red
            K2LogLevel::Emergency => ("\x1b[1;31m", "[EMERGENCY]"),// Bold Red
        };
        write!(f, "{}[{}]{}\x1b[0m", color_code, label, color_code)
    }
}
#[derive(Clone)]
pub struct K2LogStruct {
    pub module: String,
    pub level: K2LogLevel,
    pub message: String,
}

impl fmt::Display for K2LogStruct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl From<K2Error> for K2LogStruct {
    fn from(value: K2Error) -> Self {
        let message = format!("ErrorCode: {}. {}", value.code, value.message);
        K2LogStruct {
            module: "".to_string(),
            level: K2LogLevel::Error, 
            message,
        }
    }
}

#[macro_export]
macro_rules! k2log {
    ($module: expr, $level:expr, $($arg:tt)+) => {{ 
        let now = chrono::Utc::now().format("%H:%M:%S%.3f");
        
        $crate::log::Logger::log($crate::log::K2LogStruct {
            module: $module,
            level: $level,
            message: format!("\x1b[90m[{}]\x1b[0m {} {}", 
                now, 
                $level, 
                format!($($arg)+)
            ),
        })
    }};
}

#[macro_export]
macro_rules! k2log_verbose {
    ($module: expr, $($arg:tt)+) => {{ 
        #[cfg(feature = "log_verbose")]
        {
            k2log!($module, crate::log::K2LogLevel::Verbose, $($arg)*)?
        }
        #[cfg(not(feature = "log_verbose"))]
        {
            Ok::<(), K2Error>(())
        }
    }};
}
#[derive(Clone)]
pub struct LogNetwork {
    receiver: Arc<Mutex<Receiver<K2LogStruct>>>,
    destination: String,
}

impl LogNetwork {
    pub fn new(receiver: Arc<Mutex<Receiver<K2LogStruct>>>) -> Self {
        Self {
            receiver,
            destination: "127.0.0.1:0".to_string(),
        }
    }
    pub fn set_destination(&mut self, destination: String) -> Result<(), K2Error>{
        let socket = UdpSocket::bind("0.0.0.0:0")
            .map_err(|err| k2err!(
                K2ErrorCode::ProcessError,
                format!("{}", err)
            ))?;
        // Connection test
        match socket.connect(destination.clone()) {
            Ok(_) => {self.destination = destination;},
            Err(err) => {
                return Err(k2err!(
                    K2ErrorCode::BadFormat,
                    format!("{}", err)
                ));
            }
        }
        Ok(())
    }
    pub fn run(&self) -> Result<(), K2Error>{
        let socket = UdpSocket::bind("0.0.0.0:0")
            .map_err(|err| k2err!(
                K2ErrorCode::ProcessError,
                format!("{}", err)
            ))?;
        socket.connect(self.destination.clone())
            .map_err(|err| k2err!(
                K2ErrorCode::ProcessError,
                format!("{}", err)
            ))?;
        loop {
            let recv = self.receiver.lock().
                map_err(|err|
                k2err!(K2ErrorCode::ProcessError, format!("{}", err)))?;
            let log = recv.recv().map_err(|err|
                k2err!(
                    K2ErrorCode::ProcessError,
                    format!("{}", err)
                ))?;
            if log.message == "exit".to_string() {
                break;
            }
            socket.send(log.message.as_bytes())
                .map_err(|err| 
                k2err!(K2ErrorCode::ProcessError, format!("{}", err)))?;
        }
        Ok(())
    }
}

pub struct LogFile {
    receiver: Arc<Mutex<Receiver<K2LogStruct>>>,
    directory: String,
    rotate_time: DateTime<Utc>,
    compress: bool,
    file_prefix: String,
    file_suffix: String,
    datestamp: String,
    log_path: String,
    file: Option<std::fs::File>
}

impl LogFile {
    pub fn new(receiver: Arc<Mutex<Receiver<K2LogStruct>>>) -> Self {
        Self { 
            directory: "".to_string(),
            receiver, 
            rotate_time: Utc::now(), 
            compress: true,
            file_prefix: "k2_log_file".to_string(), 
            file_suffix: "".to_string(), 
            datestamp: "%Y-%m-%d".to_string(),
            log_path: "".to_string(),
            file: None,
        }
    }
    pub fn set_file_prefix(&mut self, prefix: String) {
        self.file_prefix = prefix;
    }
    pub fn set_file_suffix(&mut self, suffix: String) {
        self.file_suffix = suffix;
    }
    pub fn set_date_stamp(&mut self, date_stamp: String) -> Result<(), K2Error>{
        let date = Utc::now();
        let result = date.format(&date_stamp).to_string();
        match DateTime::parse_from_str(&result, &date_stamp) {
            Ok(_) => {self.datestamp = date_stamp;},
            Err(err) => { return Err(k2err!(K2ErrorCode::BadFormat, format!("{}", err)));}
        }
        Ok(())
    }
    fn init(&mut self) -> Result<(), K2Error>{
        let file_name = format!("{}{}{}.log", 
            self.file_prefix,
            Utc::now().format(&self.datestamp),
            self.file_suffix,
        );
        self.log_path = format!("{}/{}", self.directory, file_name);
        let file = match std::fs::File::create(self.log_path.clone()) {
            Ok(file) => {file}
            Err(_) => {
                return Err(k2err!(
                    K2ErrorCode::AlreadyExists, 
                    "Log with same name already exists.".to_string()));
                }
        };
        self.file = Some(file);
        Ok(())
    }

    fn file_rotate(&mut self) -> Result<(), K2Error>{
        let now = Utc::now();
        if now.num_days_in_month() != self.rotate_time.num_days_in_month() {
            if self.compress {
                let file_name = self.log_path.clone();
                std::thread::spawn( move || {
                    Command::new("gzip").arg(file_name);
                });
            }
            self.init()?;
            self.rotate_time = now;
        } 
        Ok(())
    }
    pub fn run(&mut self) -> Result<(), K2Error>{
        self.init()?;
        loop {
            let log: K2LogStruct;
            {
                let recv = self.receiver.lock()
                    .map_err(|err|
                        k2err!(K2ErrorCode::LockError, 
                        format!("{}", err)))?; 
                log = recv.recv().map_err(|err| 
                    k2err!(K2ErrorCode::ProcessError, format!("{}", err)))?;
            }
            if log.message == "exit".to_string() {
                break;
            }
            self.file_rotate()?;
            let file = self.file.as_mut().unwrap();
            match file.write_all(log.message.as_bytes()) {
                Ok(_) => {},
                Err(err) => {
                    return Err(k2err!(
                        K2ErrorCode::ProcessError,
                        format!("{}", err)
                    ));
                }
            }
        }
        Ok(())
    }
}
static LOGGER: OnceLock<Arc<Mutex<Logger>>> = OnceLock::new();

#[derive(Clone)]
pub struct LogLevel {
    console: K2LogLevel,
    file: K2LogLevel,
    network: K2LogLevel,
}

struct LogEntry {
    console: SyncSender<K2LogStruct>,
    file: SyncSender<K2LogStruct>,
    network: SyncSender<K2LogStruct>,
}
pub struct Logger {
    log_level: LogLevel,
    file_logger: LogFile,
    net_logger: LogNetwork,
    module_level_logger: HashMap<String, LogLevel>,
    log_entry: LogEntry,
    log_receiver_console: Arc<Mutex<Receiver<K2LogStruct>>>,
}

impl Logger {
    fn new() -> Self {
        let (log_entry_console, rx) = sync_channel::<K2LogStruct>(100);
        let log_receiver_console = Arc::new(Mutex::new(rx));
        let (log_entry_file, rx) = sync_channel::<K2LogStruct>(100);
        let log_receiver_file = Arc::new(Mutex::new(rx));
        let (log_entry_network, rx) = sync_channel::<K2LogStruct>(100);
        let log_receiver_network = Arc::new(Mutex::new(rx));
        Self {
            log_level: LogLevel{ 
                console:    K2LogLevel::Debug,
                file:       K2LogLevel::Warning,
                network:    K2LogLevel::Error,
            },
            file_logger: LogFile::new(log_receiver_file.clone()),
            net_logger: LogNetwork::new(log_receiver_network.clone()),
            module_level_logger: HashMap::new(),
            log_receiver_console,
            log_entry: LogEntry { 
                console: log_entry_console,
                file: log_entry_file,
                network: log_entry_network,
            }
        }
    }
    pub fn get() -> &'static Arc<Mutex<Logger>> {
        LOGGER.get_or_init(|| Arc::new(Mutex::new(Logger::new())))
    }
    pub fn run(&mut self) -> Result<(), K2Error> {
        let receiver = self.log_receiver_console.clone();
        let terminal_handle = TaskMonitor::create_task( "terminal_logger".to_string() ,move || {
            Logger::log_server_console(receiver);
        })?;
        let net_logger = self.net_logger.clone();
        let net_handle =TaskMonitor::create_task( "terminal_logger", move || {
            net_logger.run()
        })?;
        self.file_logger.run()?;
        let _ = terminal_handle.join();
        let _ = net_handle.join();
        Ok(())
    }
    pub fn set_mode_log_level(name: String, log_level: LogLevel) -> Result<(), K2Error> {
        let logger = Logger::get();
        let mut logger = logger.lock().map_err(|err| k2err!(K2ErrorCode::LockError, format!("{}", err)))?;
        logger.module_level_logger.insert(name, log_level);
        Ok(())
    }
    pub fn log(log_entry: K2LogStruct) -> Result<(), K2Error> {
        let logger = Logger::get();
        let logger = logger.lock().map_err(|err| k2err!(K2ErrorCode::LockError, format!("{}", err)))?;
        let log_level: LogLevel;
        match logger.module_level_logger.get(&log_entry.module) {
            Some(log_lev) => {log_level = log_lev.clone();}
            None => { log_level = logger.log_level.clone();}
        }
        if log_entry.level >= log_level.console {
            let _ = logger.log_entry.console.send(log_entry.clone());
        }
        let re_clean = Regex::new(r"\x1b\[[0-9;]*[a-zA-Z]").unwrap();
        let mut clean_log = log_entry.clone();
        clean_log.message = re_clean.replace_all(&log_entry.message, "").to_string();
        if log_entry.level >= log_level.file {
            let _ = logger.log_entry.file.send(clean_log.clone());
        }
        if log_entry.level >= log_level.network {
            let _ = logger.log_entry.network.send(clean_log.clone());
        }
        Ok(())
    }

    pub fn log_server_console(receiver: Arc<Mutex<Receiver<K2LogStruct>>>) {
        loop {
            match receiver.lock().unwrap().recv() {
                Ok(log_entry) => {
                    if log_entry.message == "exit".to_string() {
                        break;
                    }
                    println!("{}", log_entry.message);
                }
                Err(_) => {

                }        
            }
        }
    }
    pub fn get_file_logger(&self) -> &LogFile {
        &self.file_logger
    }
    pub fn get_file_logger_mut(&mut self) -> &mut LogFile {
        &mut self.file_logger
    }
    pub fn get_net_logger(&self) -> &LogNetwork {
        &self.net_logger
    }
    pub fn get_net_logger_mut(&mut self) -> &mut LogNetwork {
        &mut self.net_logger
    }
}