use std::io::ErrorKind;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::ptr::addr_of_mut;
use std::{io, mem};

use io_uring::opcode;
use io_uring::types::Fd;
use socket2::SockAddr;

use crate::op::{Completable, Op};
use crate::operation::{Droppable, Operation, OperationResult};
use crate::runtime::with_runtime;

pub struct Accept {
    addr_with_len: Box<AddrWithLen>,
}

struct AddrWithLen {
    addr: libc::sockaddr_storage,
    len: libc::socklen_t,
}

impl Accept {
    pub(crate) fn new(fd: RawFd) -> Op<Self> {
        let mut addr_with_len = Box::new(AddrWithLen {
            addr: unsafe { mem::zeroed() },
            len: mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
        });
        let entry = opcode::Accept::new(
            Fd(fd),
            addr_of_mut!(addr_with_len.addr) as _,
            addr_of_mut!(addr_with_len.len),
        )
        .build();
        let (operation, receiver, data_drop) = Operation::new();

        with_runtime(|runtime| runtime.submit(entry, operation)).unwrap();

        Op::new(Self { addr_with_len }, receiver, data_drop)
    }
}

impl Completable for Accept {
    type Output = io::Result<(RawFd, SocketAddr)>;

    fn complete(self, result: OperationResult) -> Self::Output {
        let fd = result.result?;
        let addr = self.addr_with_len.addr;
        let len = self.addr_with_len.len;

        // Safety: accept op is success
        let addr = unsafe { SockAddr::new(addr, len) };
        let addr = addr.as_socket().ok_or_else(|| {
            io::Error::new(ErrorKind::InvalidData, "kernel return socket addr invalid")
        })?;

        Ok((fd, addr))
    }

    fn data_drop(self) -> Option<Box<dyn Droppable>> {
        Some(self.addr_with_len as _)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::thread;

    use super::*;
    use crate::runtime::block_on;

    #[test]
    fn test_accept() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let join_handle = thread::spawn(move || TcpStream::connect(addr).unwrap());

        let (mut tcp, from) = block_on(async move {
            let fd = listener.as_raw_fd();

            let (fd, from) = Accept::new(fd).await.unwrap();

            let tcp = unsafe { TcpStream::from_raw_fd(fd) };

            (tcp, from)
        });

        let mut tcp2 = join_handle.join().unwrap();
        assert_eq!(tcp2.local_addr().unwrap(), from);

        tcp.write_all(b"test").unwrap();
        let mut buf = [0; 4];
        tcp2.read_exact(&mut buf).unwrap();

        assert_eq!(&buf, b"test");
    }
}
