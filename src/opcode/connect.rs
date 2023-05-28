use std::io;
use std::net::SocketAddr;
use std::os::fd::RawFd;

use io_uring::opcode;
use io_uring::types::Fd;
use socket2::SockAddr;

use crate::op::{Completable, Op};
use crate::operation::{Droppable, Operation, OperationResult};
use crate::runtime::with_runtime;

pub struct Connect {
    addr: Box<SockAddr>,
}

impl Connect {
    pub(crate) fn new(fd: RawFd, addr: SocketAddr) -> Op<Self> {
        let addr = Box::new(SockAddr::from(addr));
        let entry = opcode::Connect::new(Fd(fd), addr.as_ptr(), addr.len()).build();
        let (operation, receiver, data_drop) = Operation::new();

        with_runtime(|runtime| runtime.submit(entry, operation)).unwrap();

        Op::new(Self { addr }, receiver, data_drop)
    }
}

impl Completable for Connect {
    type Output = io::Result<()>;

    fn complete(self, result: OperationResult) -> Self::Output {
        result.result?;

        Ok(())
    }

    fn data_drop(self) -> Option<Box<dyn Droppable>> {
        Some(self.addr as _)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, TcpListener, TcpStream, UdpSocket};
    use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
    use std::os::raw::c_int;
    use std::thread;

    use socket2::{Domain, Socket, Type};

    use super::*;
    use crate::runtime::block_on;

    #[test]
    fn test_connect_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let join_handle = thread::spawn(move || {
            listener.accept().unwrap();
        });

        let socket_type = c_int::from(Type::STREAM) | libc::SOCK_CLOEXEC;
        let socket = Socket::new(Domain::IPV4, socket_type.into(), None).unwrap();
        socket
            .bind(&SocketAddr::new(IpAddr::from([0, 0, 0, 0]), 0).into())
            .unwrap();
        let fd = socket.as_raw_fd();

        block_on(async move { Connect::new(fd, addr).await.unwrap() });

        let tcp = unsafe { TcpStream::from_raw_fd(socket.into_raw_fd()) };
        assert_eq!(tcp.peer_addr().unwrap(), addr);
        join_handle.join().unwrap();
    }

    #[test]
    fn test_connect_udp() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        let join_handle = thread::spawn(move || {
            let mut buf = [0; 4];
            let (_, from) = server.recv_from(&mut buf).unwrap();

            (buf, from)
        });

        let socket_type = c_int::from(Type::DGRAM) | libc::SOCK_CLOEXEC;
        let socket = Socket::new(Domain::IPV4, socket_type.into(), None).unwrap();
        socket
            .bind(&SocketAddr::new(IpAddr::from([0, 0, 0, 0]), 0).into())
            .unwrap();
        let fd = socket.as_raw_fd();

        block_on(async move { Connect::new(fd, addr).await.unwrap() });

        let client = unsafe { UdpSocket::from_raw_fd(socket.into_raw_fd()) };
        let client_addr = client.local_addr().unwrap();

        client.send(b"test").unwrap();

        let (buf, from) = join_handle.join().unwrap();
        assert_eq!(&buf, b"test");
        assert_eq!(client_addr, from);
    }
}
