pub enum K2LogLevel {
    Emergency,
    Alert,
    Critical,
    Error,
    Warning,
    Notice,
    Informational,
    Debug,
}

pub struct K2LogStruct {
    pub level: K2LogLevel,
    pub proc: String,
    pub message: String,
}
