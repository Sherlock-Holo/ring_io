use std::io::{Error as IoError, ErrorKind};

use nix::Error;

pub trait NixErrorExt {
    fn into_io_error(self) -> IoError;
}

impl NixErrorExt for Error {
    fn into_io_error(self) -> IoError {
        match self {
            Error::Sys(errno) => IoError::from_raw_os_error(errno as _),
            Error::InvalidPath | Error::InvalidUtf8 => IoError::from(ErrorKind::InvalidInput),
            Error::UnsupportedOperation => IoError::from(ErrorKind::Unsupported),
        }
    }
}
