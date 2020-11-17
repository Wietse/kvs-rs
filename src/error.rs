use std::{
    self,
    io,
    fmt,
    error::Error,
};
use serde_json;


pub type Result<T> = std::result::Result<T, KvsError>;


#[derive(Debug)]
pub enum KvsError {
    Io(::std::io::Error),
    Serde(serde_json::Error),
    KeyNotFound,
    InvalidLogFileHandle,
}


impl KvsError {
    pub fn is_key_not_found(&self) -> bool {
        matches!(*self, KvsError::KeyNotFound)
    }
}


impl fmt::Display for KvsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KvsError::Io(ref err) => err.fmt(f),
            KvsError::Serde(ref err) => err.fmt(f),
            KvsError::KeyNotFound => write!(f, "Key not found"),
            KvsError::InvalidLogFileHandle => write!(f, "The Log file handle is not valid"),
        }
    }
}


impl Error for KvsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            KvsError::Io(ref err) => Some(err),
            KvsError::Serde(ref err) => Some(err),
            _ => None,
        }
    }
}


impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}


impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}
