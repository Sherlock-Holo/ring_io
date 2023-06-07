use std::os::fd::RawFd;

use io_uring::opcode;
use io_uring::types::Fd;

use crate::buf::IoBufMut;
use crate::op::{Completable, Op};
use crate::operation::{Droppable, Operation, OperationResult};
use crate::per_thread::runtime::with_driver;
use crate::BufResult;

pub struct Recv<T: IoBufMut> {
    buffer: T,
}

impl<T: IoBufMut> Recv<T> {
    pub(crate) fn new(fd: RawFd, mut buf: T) -> Op<Self> {
        let entry = opcode::Recv::new(Fd(fd), buf.stable_mut_ptr(), buf.bytes_total() as _).build();
        let (operation, receiver, data_drop) = Operation::new();

        with_driver(|driver| driver.push_sqe(entry, operation)).unwrap();

        Op::new(Self { buffer: buf }, receiver, data_drop)
    }
}

impl<T: IoBufMut> Completable for Recv<T> {
    type Output = BufResult<usize, T>;

    fn complete(mut self, result: OperationResult) -> Self::Output {
        let result = result.result;

        if let Ok(n) = result {
            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe {
                self.buffer.set_init(n as _);
            }
        }

        (result.map(|n| n as _), self.buffer)
    }

    fn data_drop(self) -> Option<Box<dyn Droppable>> {
        Some(Box::new(self.buffer))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::{TcpListener, TcpStream, UdpSocket};
    use std::os::fd::AsRawFd;
    use std::thread;

    use super::*;
    use crate::runtime::block_on;

    #[test]
    fn test_tcp_recv() {
        block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let join_handle = thread::spawn(move || listener.accept().unwrap().0);

            let mut tcp1 = TcpStream::connect(addr).unwrap();
            let tcp2 = join_handle.join().unwrap();

            tcp1.write_all(b"test").unwrap();

            let fd = tcp2.as_raw_fd();

            let buf = vec![0; 4];

            let (result, buf) = Recv::new(fd, buf).await;
            let n = result.unwrap();
            assert_eq!(n, 4);

            assert_eq!(buf, b"test");
        })
    }

    #[test]
    fn test_udp_recv() {
        block_on(async move {
            let server = UdpSocket::bind("127.0.0.1:0").unwrap();
            let addr = server.local_addr().unwrap();

            let client = UdpSocket::bind("0.0.0.0:0").unwrap();
            client.connect(addr).unwrap();
            let client_addr = client.local_addr().unwrap();

            server.send_to(b"test", client_addr).unwrap();

            let fd = client.as_raw_fd();
            let buf = vec![0; 4];

            let (result, buf) = Recv::new(fd, buf).await;
            let n = result.unwrap();
            assert_eq!(n, 4);

            assert_eq!(buf, b"test");
        })
    }
}
