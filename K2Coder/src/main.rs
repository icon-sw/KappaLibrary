use std::{sync::mpsc, env};

use k2_lang::{syntax_tree::SyntaxTreeProcessor, parser::Parser};
use k2_stream::{processor::connections::Output, streamer::modes::{Chain, OperativeMode}, processor::processors::ProcessorTrait, streamer::stream_controller::StreamController};

pub mod coder;
mod processor_coder;
mod library_coder;
mod application_coder;

pub fn main() -> Result<(), ()>{
    let args: Vec<String> = env::args().collect();
    let application_path = env::current_exe().unwrap();
    
    Ok(())
}
