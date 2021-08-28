use std::io::{Error, Result};

use io_uring::cqueue::Entry;

pub trait EntryExt: Sized {
    fn ok(self) -> Result<Self>;

    fn is_err(&self) -> bool;
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

    fn is_err(&self) -> bool {
        self.result() < 0
    }
}
