use std::io::{ErrorKind, IoSliceMut};
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::{io, mem};

use io_uring::opcode;
use io_uring::types::Fd;
use libc::{sockaddr_in, sockaddr_in6};
use socket2::SockAddr;

use crate::buf::IoBufMut;
use crate::op::{Completable, Op};
use crate::operation::{Droppable, Operation, OperationResult};
use crate::per_thread::runtime::with_driver;
use crate::BufResult;

pub struct RecvFrom<T: IoBufMut> {
    data: Box<RecvFromData<T>>,
}

struct RecvFromData<T: IoBufMut> {
    buffer: T,
    io_slices: Vec<IoSliceMut<'static>>,
    addr: libc::sockaddr_storage,
    msg: libc::msghdr,
}

impl<T: IoBufMut> RecvFrom<T> {
    pub(crate) fn new(fd: RawFd, mut buf: T) -> Op<Self> {
        let io_slices = vec![IoSliceMut::new(unsafe {
            std::slice::from_raw_parts_mut(buf.stable_mut_ptr(), buf.bytes_total())
        })];

        let mut data = Box::new(RecvFromData {
            buffer: buf,
            io_slices,
            addr: unsafe { mem::zeroed() },
            msg: unsafe { mem::zeroed() },
        });

        data.msg.msg_iov = data.io_slices.as_mut_ptr() as _;
        data.msg.msg_iovlen = data.io_slices.len() as _;
        data.msg.msg_name = &mut data.addr as *mut _ as *mut _;
        data.msg.msg_namelen = mem::size_of::<libc::sockaddr_storage>() as _;

        let entry = opcode::RecvMsg::new(Fd(fd), &mut data.msg as *mut _).build();
        let (operation, receiver, data_drop) = Operation::new();

        with_driver(|driver| driver.push_sqe(entry, operation)).unwrap();

        Op::new(Self { data }, receiver, data_drop)
    }
}

unsafe impl<T: IoBufMut> Send for RecvFromData<T> {}

impl<T: IoBufMut> Completable for RecvFrom<T> {
    type Output = BufResult<(usize, SocketAddr), T>;

    fn complete(mut self, result: OperationResult) -> Self::Output {
        let result = result.result;

        match result {
            Ok(n) => {
                // Safety: the kernel wrote `n` bytes to the buffer.
                let addr = unsafe {
                    self.data.buffer.set_init(n as _);

                    if self.data.addr.ss_family == libc::AF_INET as _ {
                        SockAddr::new(self.data.addr, mem::size_of::<sockaddr_in>() as _)
                    } else if self.data.addr.ss_family == libc::AF_INET6 as _ {
                        SockAddr::new(self.data.addr, mem::size_of::<sockaddr_in6>() as _)
                    } else {
                        return (
                            Err(io::Error::new(
                                ErrorKind::InvalidData,
                                "invalid addr family",
                            )),
                            self.data.buffer,
                        );
                    }
                };

                let addr = addr.as_socket().unwrap();

                (Ok((n as _, addr)), self.data.buffer)
            }

            Err(err) => (Err(err), self.data.buffer),
        }
    }

    fn data_drop(self) -> Option<Box<dyn Droppable>> {
        Some(Box::new(self.data))
    }
}

#[cfg(test)]
mod tests {
    use std::net::UdpSocket;
    use std::os::fd::AsRawFd;

    use super::*;
    use crate::runtime::block_on;

    #[test]
    fn test_udp_recv_from() {
        block_on(async move {
            let server = UdpSocket::bind("127.0.0.1:0").unwrap();
            let addr = server.local_addr().unwrap();

            let client = UdpSocket::bind("0.0.0.0:0").unwrap();
            client.connect(addr).unwrap();
            let client_addr = client.local_addr().unwrap();

            client.send(b"test").unwrap();

            let fd = server.as_raw_fd();
            let buf = vec![0; 4];

            let (result, buf) = RecvFrom::new(fd, buf).await;
            let (n, from) = result.unwrap();
            assert_eq!(n, 4);
            assert_eq!(buf, b"test");
            assert_eq!(client_addr, from);
        })
    }
}
