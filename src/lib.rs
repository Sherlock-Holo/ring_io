use std::io::Error;
use std::io::ErrorKind;

pub mod drive;
pub mod fs;
pub mod io;
pub mod net;
mod ring;

fn from_nix_err(err: nix::Error) -> Error {
    match err {
        nix::Error::InvalidPath | nix::Error::InvalidUtf8 => Error::from(ErrorKind::InvalidInput),
        nix::Error::UnsupportedOperation => Error::from_raw_os_error(libc::ENOTSUP),
        nix::Error::Sys(errno) => errno.into(),
    }
}
