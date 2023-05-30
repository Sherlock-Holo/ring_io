use std::os::fd::RawFd;

use io_uring::opcode;
use io_uring::types::Fd;

use crate::buf::IoBuf;
use crate::op::{Completable, Op};
use crate::operation::{Droppable, Operation, OperationResult};
use crate::runtime::with_runtime_context;
use crate::BufResult;

pub struct Write<T: IoBuf> {
    buffer: T,
}

impl<T: IoBuf> Write<T> {
    pub(crate) fn new(fd: RawFd, buf: T, offset: u64) -> Op<Self> {
        let entry = opcode::Write::new(Fd(fd), buf.stable_ptr(), buf.bytes_init() as _)
            .offset(offset)
            .build();
        let (operation, receiver, data_drop) = Operation::new();

        with_runtime_context(|runtime| runtime.submit(entry, operation)).unwrap();

        Op::new(Self { buffer: buf }, receiver, data_drop)
    }
}

impl<T: IoBuf> Completable for Write<T> {
    type Output = BufResult<usize, T>;

    fn complete(self, result: OperationResult) -> Self::Output {
        let result = result.result;

        (result.map(|n| n as _), self.buffer)
    }

    fn data_drop(self) -> Option<Box<dyn Droppable>> {
        Some(Box::new(self.buffer))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::AsRawFd;
    use std::thread;

    use tempfile::NamedTempFile;

    use super::*;
    use crate::runtime::block_on;

    #[test]
    fn test_file_write() {
        let mut file = NamedTempFile::new().unwrap();
        let fd = file.as_raw_fd();

        block_on(async move {
            let (result, _) = Write::new(fd, b"hello".as_slice(), 0).await;
            assert_eq!(result.unwrap(), 5);

            let (result, _) = Write::new(fd, b"world".as_slice(), 5).await;
            assert_eq!(result.unwrap(), 5);
        });

        let mut buf = vec![];
        file.read_to_end(&mut buf).unwrap();

        assert_eq!(buf, b"helloworld");
    }

    #[test]
    fn test_socket_write() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let join_handle = thread::spawn(move || listener.accept().unwrap().0);

        let tcp1 = TcpStream::connect(addr).unwrap();
        let mut tcp2 = join_handle.join().unwrap();

        block_on(async move {
            let fd = tcp1.as_raw_fd();

            let (result, _) = Write::new(fd, b"test".as_slice(), 0).await;
            assert_eq!(result.unwrap(), 4);

            let mut buf = [0; 4];
            tcp2.read_exact(&mut buf).unwrap();

            assert_eq!(&buf, b"test");
        })
    }
}
