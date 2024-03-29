use std::io;
use std::mem::ManuallyDrop;
use std::net::SocketAddr;
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};
use std::os::raw::c_int;

use socket2::{Domain, SockAddr, Socket, Type};

use crate::buf::{IoBuf, IoBufMut};
use crate::fd_trait;
use crate::op::{MultiOp, Op};
use crate::opcode::{self, Close, Connect, Recv, RecvFrom, RecvMulti, RecvWithBufRing, SendTo};
use crate::per_thread::runtime::in_per_thread_runtime;
use crate::runtime::spawn;

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

    pub fn from_std(udp_socket: std::net::UdpSocket) -> Self {
        Self {
            fd: udp_socket.into_raw_fd(),
        }
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

    pub fn recv_with_buf_ring(&self, buffer_group: u16) -> Op<RecvWithBufRing> {
        RecvWithBufRing::new(self.fd, buffer_group)
    }

    pub fn recv_multi(&self, buffer_group: u16) -> MultiOp<RecvMulti> {
        RecvMulti::new(self.fd, buffer_group)
    }

    pub fn send_to<B: IoBuf>(&self, buf: B, addr: SocketAddr) -> Op<SendTo<B>> {
        SendTo::new(self.fd, buf, addr)
    }

    pub fn recv_from<B: IoBufMut>(&self, buf: B) -> Op<RecvFrom<B>> {
        RecvFrom::new(self.fd, buf)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let std_udp_socket =
            ManuallyDrop::new(unsafe { std::net::UdpSocket::from_raw_fd(self.fd) });

        std_udp_socket.local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        let std_udp_socket =
            ManuallyDrop::new(unsafe { std::net::UdpSocket::from_raw_fd(self.fd) });

        std_udp_socket.peer_addr()
    }

    pub fn close(self) -> Op<Close> {
        let fd = self.fd;
        let _ = ManuallyDrop::new(self);

        Close::new(fd)
    }
}

fd_trait!(UdpSocket);

impl Drop for UdpSocket {
    fn drop(&mut self) {
        if in_per_thread_runtime() {
            spawn(Close::new(self.fd)).detach();
        } else {
            unsafe {
                libc::close(self.fd);
            }
        }
    }
}
