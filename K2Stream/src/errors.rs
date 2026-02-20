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
    GenericError,
}

impl std::fmt::Display for K2ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            &K2ErrorCode::NotAllowed => write!(f, "NotAllowed"),
            &K2ErrorCode::NotFound => write!(f, "NotFound"),
            &K2ErrorCode::ErrorRange => write!(f, "ErrorRange"),
            &K2ErrorCode::AlreadyExists => write!(f, "AlreadyExists"),
            &K2ErrorCode::InvalidValue => write!(f, "InvalidValue"),
            &K2ErrorCode::BadFormat => write!(f, "BadFormat"),
            &K2ErrorCode::Uninitialized => write!(f, "Uninitialized"),
            &K2ErrorCode::LockError => write!(f, "LockError"),
            &K2ErrorCode::ProcessError => write!(f, "ProcessError"),
            &K2ErrorCode::OutOfRange => write!(f, "OutOfRange"),
            &K2ErrorCode::InvalidOperation => write!(f, "InvalidOperation"),
            &K2ErrorCode::GenericError => write!(f, "GenericError"),
        }
    }
}
#[derive(Debug, Clone)]
pub struct K2Error {
    pub code: K2ErrorCode,
    pub message: String,
}

impl From<()> for K2Error {
    fn from(_: ()) -> Self {
        K2Error { code: K2ErrorCode::GenericError, message: "Error".into() }
    }
}

impl From<K2Error> for () {
    fn from(_: K2Error) -> () {
        ()
    }
}

#[macro_export]
macro_rules! k2err {
    ($code:expr, $msg:expr) => {
        K2Error {
            code: $code,
            message: format!(
                "\x1b[90m[{}:{}]\x1b[0m \x1b[31mCode: {}\x1b[0m | \x1b[38;5;208m{}\x1b[0m",
                file!(),
                line!(),
                $code,
                $msg
            ),
        }
    };
}