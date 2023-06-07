use std::ffi::CString;
use std::io;
use std::os::fd::RawFd;

use io_uring::opcode;
use io_uring::types::Fd;

use crate::fs::OpenOptions;
use crate::op::{Completable, Op};
use crate::operation::{Droppable, Operation, OperationResult};
use crate::per_thread::runtime::with_driver;

pub struct Open {
    path: CString,
}

impl Open {
    pub(crate) fn new(path: CString, options: &OpenOptions) -> io::Result<Op<Self>> {
        let flags = libc::O_CLOEXEC
            | options.access_mode()?
            | options.creation_mode()?
            | (options.custom_flags & !libc::O_ACCMODE);

        let entry = opcode::OpenAt::new(Fd(libc::AT_FDCWD), path.as_ptr())
            .flags(flags)
            .mode(options.mode)
            .build();
        let (operation, receiver, data_drop) = Operation::new();

        with_driver(|driver| driver.push_sqe(entry, operation)).unwrap();

        Ok(Op::new(Self { path }, receiver, data_drop))
    }
}

impl Completable for Open {
    type Output = io::Result<RawFd>;

    fn complete(self, result: OperationResult) -> Self::Output {
        result.result
    }

    fn data_drop(self) -> Option<Box<dyn Droppable>> {
        Some(Box::new(self.path))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::File;
    use std::io::{Read, Write};
    use std::os::fd::OwnedFd;

    use tempfile::NamedTempFile;

    use super::*;
    use crate::block_on;

    #[test]
    fn test_open_read() {
        block_on(async move {
            let file = OpenOptions::new()
                .read(true)
                .open("testdata/book.txt")
                .await
                .unwrap();
            let mut std_file = File::from(OwnedFd::from(file));

            let mut buf = vec![];
            std_file.read_to_end(&mut buf).unwrap();

            let data = fs::read("testdata/book.txt").unwrap();

            assert_eq!(data, buf);
        })
    }

    #[test]
    fn test_open_write() {
        block_on(async move {
            let mut temp_file = NamedTempFile::new().unwrap();
            let file = OpenOptions::new()
                .write(true)
                .open(temp_file.path())
                .await
                .unwrap();
            let mut std_file = File::from(OwnedFd::from(file));

            std_file.write_all(b"test").unwrap();

            let mut buf = vec![];
            temp_file.read_to_end(&mut buf).unwrap();

            assert_eq!(buf, b"test");
        })
    }
}
