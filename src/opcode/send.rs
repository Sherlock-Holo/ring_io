use std::os::fd::RawFd;

use io_uring::opcode;
use io_uring::types::Fd;

use crate::buf::IoBuf;
use crate::op::{Completable, Op};
use crate::operation::{Droppable, Operation, OperationResult};
use crate::runtime::with_runtime;
use crate::BufResult;

pub struct Send<T: IoBuf> {
    buffer: T,
}

impl<T: IoBuf> Send<T> {
    pub(crate) fn new(fd: RawFd, buf: T) -> Op<Self> {
        let entry = opcode::Send::new(Fd(fd), buf.stable_ptr(), buf.bytes_init() as _).build();
        let (operation, receiver, waker, data_drop) = Operation::new();

        with_runtime(|runtime| runtime.submit(entry, operation)).unwrap();

        Op::new(Self { buffer: buf }, receiver, waker, data_drop)
    }
}

impl<T: IoBuf> Completable for Send<T> {
    type Output = BufResult<usize, T>;

    fn complete(self, result: OperationResult) -> Self::Output {
        let result = result.result;

        (result.map(|n| n as _), self.buffer)
    }

    fn data_drop(self) -> Box<dyn Droppable> {
        Box::new(self.buffer)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::net::{TcpListener, TcpStream, UdpSocket};
    use std::os::fd::AsRawFd;
    use std::thread;

    use super::*;
    use crate::runtime::block_on;

    #[test]
    fn test_tcp_send() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let join_handle = thread::spawn(move || listener.accept().unwrap().0);

        let tcp1 = TcpStream::connect(addr).unwrap();
        let mut tcp2 = join_handle.join().unwrap();

        block_on(async move {
            let fd = tcp1.as_raw_fd();

            let (result, _) = Send::new(fd, b"test").await;
            assert_eq!(result.unwrap(), 4);

            let mut buf = [0; 4];
            tcp2.read_exact(&mut buf).unwrap();

            assert_eq!(&buf, b"test");
        })
    }

    #[test]
    fn test_udp_send() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        let client = UdpSocket::bind("0.0.0.0:0").unwrap();
        client.connect(addr).unwrap();

        block_on(async move {
            let fd = client.as_raw_fd();

            let (result, _) = Send::new(fd, b"test".as_slice()).await;
            assert_eq!(result.unwrap(), 4);

            let mut buf = [0; 4];
            let (_, from) = server.recv_from(&mut buf).unwrap();

            assert_eq!(&buf, b"test");
            assert_eq!(client.local_addr().unwrap(), from);
        })
    }
}
