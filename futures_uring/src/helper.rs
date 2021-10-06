use std::convert;
use std::ffi::CString;
use std::io::{Error, ErrorKind};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

// when https://github.com/rust-lang/rust/issues/70142 is stable, remove it and use the native
// method
pub trait ResultExt<T, E> {
    fn flatten_result(self) -> Result<T, E>;
}

impl<T, E> ResultExt<T, E> for Result<Result<T, E>, E> {
    fn flatten_result(self) -> Result<T, E> {
        self.and_then(convert::identity)
    }
}

pub fn path_to_c_string(path: &Path) -> Result<CString, Error> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err))
}
