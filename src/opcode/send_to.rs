use std::io::IoSlice;
use std::net::SocketAddr;
use std::os::fd::RawFd;

use io_uring::opcode;
use io_uring::types::Fd;
use socket2::SockAddr;

use crate::buf::IoBuf;
use crate::op::{Completable, Op};
use crate::operation::{Droppable, Operation, OperationResult};
use crate::runtime::with_runtime;
use crate::BufResult;

pub struct SendTo<T: IoBuf> {
    buffer: T,
    io_slices: Vec<IoSlice<'static>>,
    addr: Box<SockAddr>,
    msg: Box<libc::msghdr>,
}

impl<T: IoBuf> SendTo<T> {
    pub(crate) fn new(fd: RawFd, buf: T, addr: SocketAddr) -> Op<Self> {
        // Safety: buf is valid
        let io_slices = vec![IoSlice::new(unsafe {
            std::slice::from_raw_parts(buf.stable_ptr(), buf.bytes_init())
        })];

        let addr = Box::new(SockAddr::from(addr));

        // Safety: we set value after zeroed
        let mut msghdr: Box<libc::msghdr> = Box::new(unsafe { std::mem::zeroed() });
        msghdr.msg_iov = io_slices.as_ptr() as *mut _;
        msghdr.msg_iovlen = io_slices.len() as _;
        msghdr.msg_name = addr.as_ptr() as *mut libc::c_void;
        msghdr.msg_namelen = addr.len();

        let entry = opcode::SendMsg::new(Fd(fd), msghdr.as_ref() as *const _).build();
        let (operation, receiver, data_drop) = Operation::new();

        with_runtime(|runtime| runtime.submit(entry, operation)).unwrap();

        Op::new(
            Self {
                buffer: buf,
                io_slices,
                addr,
                msg: msghdr,
            },
            receiver,
            data_drop,
        )
    }
}

unsafe impl<T: IoBuf> Send for SendTo<T> {}

impl<T: IoBuf> Completable for SendTo<T> {
    type Output = BufResult<usize, T>;

    fn complete(self, result: OperationResult) -> Self::Output {
        let result = result.result;

        (result.map(|n| n as _), self.buffer)
    }

    fn data_drop(self) -> Option<Box<dyn Droppable>> {
        Some(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use std::net::UdpSocket;
    use std::os::fd::AsRawFd;

    use super::*;
    use crate::runtime::block_on;

    #[test]
    fn test_udp_send_to() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();

        let client = UdpSocket::bind("0.0.0.0:0").unwrap();
        client.connect(addr).unwrap();
        let client_addr = client.local_addr().unwrap();

        block_on(async move {
            let fd = server.as_raw_fd();

            let data = Vec::from("test");
            let (result, _) = SendTo::new(fd, data, client_addr).await;
            assert_eq!(result.unwrap(), 4);

            let mut buf = [0; 4];
            client.recv(&mut buf).unwrap();

            assert_eq!(&buf, b"test");
        })
    }
}
