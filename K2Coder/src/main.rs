use std::{sync::mpsc, env};

use k2_lang::{ast::AstProcessor, parser::Parser, coder::Coder};
use k2_stream::{connections::Output, modes::{Chain, OperativeMode}, processors::ProcessorTrait, stream_controller::StreamController};

fn run() -> Result<(), ()>{
    let stream_cntr_code = StreamController::create("coder_controller".to_string())?;
    let stream_cntr_arc = StreamController::get_stream_by_id(stream_cntr_code)?;
    let streaming_control = stream_cntr_arc.lock().map_err(|_| ())?;
    let input_parser = streaming_control
        .get_processors("parser".to_string())?
        .get_stream_block()
        .get_input::<String>(&"command".to_string())?
        .get_sender();
    let (sender, receiver) = mpsc::sync_channel::<String>(100);
    let mut streaming_control = stream_cntr_arc.lock().map_err(|_| ())?;
    streaming_control
        .get_processors_mut("coder".to_string())?
        .get_stream_block_mut()
        .get_output_mut::<String>(&"response".to_string())?
        .connect(sender);
    loop {
        let mut command = String::new();
        print!("K2Coder: ");
        std::io::stdin().read_line(&mut command).map_err(|_| ())?;
        if command.trim() == "exit" {
            break;
        }
        input_parser.send(command).map_err(|_| ())?;
        let response = receiver.recv().map_err(|_| ())?;
        println!("Response: {}", response);
    }
    Ok(())
}
fn init() -> Result<(), ()>{
    //dbg!("Initializing K2Coder...");
    let stream_cntr_code = StreamController::create("coder_controller".to_string())?;
    dbg!(stream_cntr_code);
    let stream_cntr_arc = StreamController::get_stream_by_id(stream_cntr_code)?;
    let mut streaming_control = stream_cntr_arc.lock().map_err(|_| ())?;
    dbg!("Adding default mode...");
    streaming_control.add_mode(1, 
        OperativeMode::new("coder_chain".to_string(), 1)
    )?;
    let mode = streaming_control.get_mode_mut(&1)?;
    dbg!("Adding default chain...");
    mode.add_chain("coder_chain".to_string(), Chain::new("coder_chain".to_string()))?;
    let mut chain = mode.get_chain_mut(&"coder_chain".to_string())?.lock().map_err(|_|())?;
    dbg!("Adding processing blocks ...");
    (*chain).add_block(Parser::new("parser".to_string())?)?;
    (*chain).add_block(AstProcessor::new("ast".to_string())?)?;
    (*chain).add_block(Coder::new("coder".to_string())?)?;
    dbg!("Connection blocks...");
    let mut streaming_control = stream_cntr_arc.lock().map_err(|_| ())?;
    streaming_control.connect::<String>("parser.response".to_string(), "ast.input".to_string())?;
    streaming_control.connect::<String>("ast.response".to_string(), "coder.input".to_string())?;
    //streaming_control.add_processor("parser", parser)?;
    let mut streaming_control = stream_cntr_arc.lock().map_err(|_| ())?;
    dbg!("Initializing streaming control...");
    streaming_control.initialize()?;
    Ok(())
}

pub fn main() -> Result<(), ()>{
    let args: Vec<String> = env::args().collect();
    let application_path = env::current_exe().unwrap();
    
    init()?;
    run()?;
    Ok(())
}
