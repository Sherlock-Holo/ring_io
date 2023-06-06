use std::io;
use std::os::fd::RawFd;

use io_uring::opcode;
use io_uring::types::Fd;

use crate::op::{Completable, Op};
use crate::operation::{Droppable, Operation, OperationResult};
use crate::per_thread::runtime::with_driver;

pub struct Close {
    _priv: (),
}

impl Close {
    pub(crate) fn new(fd: RawFd) -> Op<Self> {
        let entry = opcode::Close::new(Fd(fd)).build();
        let (operation, receiver, data_drop) = Operation::new();

        with_driver(|driver| driver.push_sqe(entry, operation)).unwrap();

        Op::new(Self { _priv: () }, receiver, data_drop)
    }
}

impl Completable for Close {
    type Output = io::Result<()>;

    fn complete(self, result: OperationResult) -> Self::Output {
        result.result.map(|_| ())
    }

    fn data_drop(self) -> Option<Box<dyn Droppable>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{ErrorKind, Read};
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::IntoRawFd;
    use std::thread;

    use super::*;
    use crate::block_on;

    #[test]
    fn test_close_file() {
        block_on(async move {
            let file = File::open("testdata/book.txt").unwrap();
            let fd = file.into_raw_fd();

            Close::new(fd).await.unwrap();
        })
    }

    #[test]
    fn test_close_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let join_handle = thread::spawn(move || listener.accept().unwrap().0);

        let tcp1 = TcpStream::connect(addr).unwrap();
        let mut tcp2 = join_handle.join().unwrap();
        let fd = tcp1.into_raw_fd();

        block_on(async move {
            Close::new(fd).await.unwrap();
        });

        assert_eq!(tcp2.read(&mut [0; 1]).unwrap(), 0);
    }

    #[test]
    fn test_close_tcp_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let fd = listener.into_raw_fd();

        block_on(async move {
            Close::new(fd).await.unwrap();
        });

        assert_eq!(
            TcpStream::connect(addr).unwrap_err().kind(),
            ErrorKind::ConnectionRefused
        );
    }
}
