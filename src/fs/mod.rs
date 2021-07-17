use std::ffi::CString;
use std::io::{Error, ErrorKind, Result};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

pub use file::{read, write, File, OpenOptions, Sync};
pub use metadata::{metadata, symlink_metadata, FileType, Metadata, Permissions};
pub use remove::{remove_dir, remove_file};
pub use rename::rename;

mod file;
mod metadata;
mod remove;
mod rename;

fn path_to_cstring<P: AsRef<Path>>(path: P) -> Result<CString> {
    CString::new(path.as_ref().as_os_str().as_bytes())
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err))
}
