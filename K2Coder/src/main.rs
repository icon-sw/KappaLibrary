use std::env;

use k2_lang::{ast::AstProcessor, parser::Parser, coder::Coder};
use k2_stream::{modes::{Chain, OperativeMode}, processors::ProcessorTrait, stream_controller::StreamController};

fn init() -> Result<(), ()>{
    let stream_cntr_code = StreamController::create("coder_controller".to_string())?;
    let stream_cntr_arc = StreamController::get_stream_by_id(stream_cntr_code)?;
    let mut streaming_control = stream_cntr_arc.lock().map_err(|_| ())?;
    streaming_control.add_mode(1, 
        OperativeMode::new("coder_chain".to_string(), 1)
    )?;
    let mode = streaming_control.get_mode_mut(&1)?;
    mode.add_chain("coder_chain".to_string(), Chain::new("coder_chain".to_string()))?;
    let mut chain = mode.get_chain_mut(&"coder_chain".to_string())?.lock().map_err(|_|())?;
    (*chain).add_block(Parser::new("parser".to_string())?)?;
    (*chain).add_block(AstProcessor::new("ast".to_string())?)?;
    (*chain).add_block(Coder::new("coder".to_string())?)?;

    //streaming_control.add_processor("parser", parser)?;
    let mut streaming_control = stream_cntr_arc.lock().map_err(|_| ())?;
    streaming_control.initialize()?;
    Ok(())

}

pub fn main() -> Result<(), ()>{
    let args: Vec<String> = env::args().collect();
    let application_path = env::current_exe().unwrap();
    
    init()?;

    Ok(())
}
