use std::io;
use std::net::SocketAddr;
use std::os::fd::{IntoRawFd, RawFd};
use std::os::raw::c_int;

use socket2::{Domain, SockAddr, Socket, Type};

use crate::buf::{IoBuf, IoBufMut};
use crate::fd_trait;
use crate::op::Op;
use crate::opcode::{self, Close, Connect, Recv, RecvFrom, SendTo};
use crate::runtime::{in_ring_io_context, spawn};

#[derive(Debug)]
pub struct UdpSocket {
    fd: RawFd,
}

impl UdpSocket {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };

        let socket_type = c_int::from(Type::DGRAM) | libc::SOCK_CLOEXEC;
        let socket = Socket::new(domain, socket_type.into(), None)?;

        socket.bind(&SockAddr::from(addr))?;

        Ok(Self {
            fd: socket.into_raw_fd(),
        })
    }

    pub fn connect(&self, addr: SocketAddr) -> Op<Connect> {
        Connect::new(self.fd, addr)
    }

    pub fn send<B: IoBuf>(&self, buf: B) -> Op<opcode::Send<B>> {
        opcode::Send::new(self.fd, buf)
    }

    pub fn recv<B: IoBufMut>(&self, buf: B) -> Op<Recv<B>> {
        Recv::new(self.fd, buf)
    }

    pub fn send_to<B: IoBuf>(&self, buf: B, addr: SocketAddr) -> Op<SendTo<B>> {
        SendTo::new(self.fd, buf, addr)
    }

    pub fn recv_from<B: IoBufMut>(&self, buf: B) -> Op<RecvFrom<B>> {
        RecvFrom::new(self.fd, buf)
    }

    pub fn close(&mut self) -> Op<Close> {
        let fd = self.fd;
        self.fd = -1;

        Close::new(fd)
    }
}

fd_trait!(UdpSocket);

impl Drop for UdpSocket {
    fn drop(&mut self) {
        if self.fd == -1 {
            return;
        }

        if in_ring_io_context() {
            spawn(self.close()).detach();
        } else {
            unsafe {
                libc::close(self.fd);
            }
        }
    }
}
