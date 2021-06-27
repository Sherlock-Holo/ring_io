use std::io::{Error, Result};

use io_uring::cqueue::Entry;

pub trait EntryExt: Sized {
    fn ok(self) -> Result<Self>;
}

impl EntryExt for Entry {
    fn ok(self) -> Result<Self> {
        let result = self.result();
        if result < 0 {
            Err(Error::from_raw_os_error(-result))
        } else {
            Ok(self)
        }
    }
}
