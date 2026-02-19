#[derive(Debug, Clone)]
pub enum K2ErrorCode {
    NotAllowed,
    NotFound,
    ErrorRange,
    AlreadyExists,
    InvalidValue,
    BadFormat,
    Uninitialized,
    LockError,
    ProcessError,
    OutOfRange,
    InvalidOperation,
}

#[derive(Debug, Clone)]
pub struct K2Error {
    pub code: K2ErrorCode,
    pub message: String,
}

impl From<()> for K2Error {
    fn from(_) -> Self {
        K2Error { code: K2ErrorCode::ErrorRange, message: "IO Error".into() }
    }
}