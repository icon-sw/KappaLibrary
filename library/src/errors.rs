pub enum ProcessorError {
    Ok,
    RangeValueError,
    VariableAlreadyExists,
    VariableNotFound,
    MemoryAlreadyExists,
    MemoryNotFound,
    ReceivingError,
    SendingError,
    LockError,
}

pub fn processor_error_to_str(err: &ProcessorError) -> &str {
    match err {
        ProcessorError::Ok => "No error",
        ProcessorError::RangeValueError => "Value out of range",
        ProcessorError::VariableAlreadyExists => "Variable already exists",
        ProcessorError::VariableNotFound => "Variable not found",
        ProcessorError::MemoryAlreadyExists => "Memory already exists",
        ProcessorError::MemoryNotFound => "Memory not found",
        ProcessorError::ReceivingError => "Error receiving data",
        ProcessorError::SendingError => "Error sending data",
        ProcessorError::LockError => "Error locking resource",
    }
}