use std::{env, process::Command, sync::{Arc, Mutex, OnceLock}};

use k2lang::{ast::AbstractSyntaxTree, parser::Parser};
use k2stream::{errors::{K2Error, K2ErrorCode}, k2err, processor::processors::ProcessorTrait, streamer::{modes::{Chain, OperativeMode}, stream_controller::StreamController}};

use crate::{cargo_interface::CargoInterface, coder::Coder};

pub mod coder;
pub mod library_coder;
pub mod application_coder;
pub mod processor_coder;
pub mod streamer_coder;
pub mod cargo_interface;

pub static CARGO_IF: OnceLock<Mutex<CargoInterface>> = OnceLock::new();

pub fn run(stream_id: isize) -> Result<(), K2Error>{
    let _handle = std::thread::spawn( move || {
        let _ = StreamController::run(stream_id);     
    });
    // Loop da terminale
    Ok(())
}

fn init() -> Result<isize, K2Error>{
    let stream_id = StreamController::create("k2code".to_string())?;
    let stream = StreamController::get_stream_by_id(stream_id)?;
    let parser = Parser::new("parser".to_string())?;
    let ast = AbstractSyntaxTree::new("ast".to_string())?;
    let coder = Coder::new("coder".to_string())?;
    let chain = Arc::new(Mutex::new(Chain::new("k2_code_chain".to_string())));
    let mut mode = OperativeMode::new("k2_code_mode".to_string(), 1);
    mode.add_chain("k2_code_chain".to_string(), chain.clone())?;
    let mut stream = stream.lock().map_err(|_|k2err!(K2ErrorCode::LockError, "".to_string()))?;
    let stream = stream.as_any_mut().downcast_mut::<StreamController>().unwrap();
    stream.add_processor(&chain.clone(), parser.name().clone(), parser)?;
    stream.add_processor(&chain.clone(), ast.name().clone(), ast)?;
    stream.add_processor(&chain.clone(), coder.name().clone(), coder)?;
    stream.add_mode(1, mode)?;
    stream.initialize()?;
    Ok(stream_id)
}
fn main() {
    let args: Vec<String> = env::args().collect();
    //let application_path = env::current_exe().unwrap();
    let mut next_is_k2 = false;
    let mut k2_library: String = "../".to_string();
    for arg in args.into_iter().skip(1) {
        match arg.as_str() {
            "--k2_library" => {
                next_is_k2 = true;
            }
            _ => {
                if next_is_k2 {
                    k2_library = arg;
                }
            },
        }
    }
    match Command::new("which").arg("cargo").output() {
        Ok(output) => {
            if output.status.success() {
                let cargo_path = String::from_utf8_lossy(&output.stdout).to_string();
                CARGO_IF.get_or_init(|| Mutex::new(CargoInterface {
                    cargo_path,
                    library_path: k2_library,
                }));
            } else {
                eprintln!("{}", String::from_utf8_lossy(&output.stderr));
                return;
            }
        }
        Err(_) => {
            eprintln!("Unable to locate cargo!");
            return;
        }
    }
    let stream_id: isize;
    match init() {
        Ok(id) => {stream_id = id;},
        Err(err) => {
            eprintln!("{}", err.message);
            return;
        }
    }
    match run(stream_id) {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{}", err.message);
        }
    }
}
