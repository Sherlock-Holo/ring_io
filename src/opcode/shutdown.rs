use std::os::fd::RawFd;
use std::{io, net};

use io_uring::opcode;
use io_uring::types::Fd;

use crate::op::Completable;
use crate::operation::{Droppable, Operation, OperationResult};
use crate::runtime::with_runtime_context;
use crate::Op;

pub struct Shutdown {
    _priv: (),
}

impl Shutdown {
    pub(crate) fn new(fd: RawFd, how: net::Shutdown) -> Op<Self> {
        let how = match how {
            net::Shutdown::Read => libc::SHUT_RD,
            net::Shutdown::Write => libc::SHUT_WR,
            net::Shutdown::Both => libc::SHUT_RDWR,
        };

        let entry = opcode::Shutdown::new(Fd(fd), how).build();
        let (operation, receiver, data_drop) = Operation::new();

        with_runtime_context(|runtime| runtime.submit(entry, operation)).unwrap();

        Op::new(Self { _priv: () }, receiver, data_drop)
    }
}

impl Completable for Shutdown {
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
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::AsRawFd;
    use std::thread;

    use super::*;
    use crate::block_on;

    #[test]
    fn test_tcp_shutdown_write() {
        block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let join_handle = thread::spawn(move || listener.accept().unwrap().0);

            let mut tcp1 = TcpStream::connect(addr).unwrap();
            let mut tcp2 = join_handle.join().unwrap();
            let fd = tcp1.as_raw_fd();

            Shutdown::new(fd, net::Shutdown::Write).await.unwrap();

            assert_eq!(tcp2.read(&mut [0; 1]).unwrap(), 0);

            tcp1.write(b"test").unwrap_err();
        })
    }

    #[test]
    fn test_tcp_shutdown_read() {
        block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let join_handle = thread::spawn(move || listener.accept().unwrap().0);

            let mut tcp1 = TcpStream::connect(addr).unwrap();
            let _tcp2 = join_handle.join().unwrap();
            let fd = tcp1.as_raw_fd();

            Shutdown::new(fd, net::Shutdown::Read).await.unwrap();

            assert_eq!(tcp1.read(&mut [0; 1]).unwrap(), 0);
        })
    }
}
