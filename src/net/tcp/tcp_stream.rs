use std::io;
use std::net::SocketAddr;
use std::os::fd::{IntoRawFd, RawFd};
use std::os::raw::c_int;

use socket2::{Domain, Socket, Type};

use crate::buf::{IoBuf, IoBufMut};
use crate::fd_trait;
use crate::op::Op;
use crate::opcode::{Close, Connect, Read, Write};
use crate::runtime::{in_ring_io_context, spawn};

pub struct TcpStream {
    fd: RawFd,
}

impl TcpStream {
    fn create_socket(domain: Domain) -> io::Result<Self> {
        let socket_type = c_int::from(Type::STREAM) | libc::SOCK_CLOEXEC;
        let socket = Socket::new(domain, socket_type.into(), None)?;

        Ok(Self {
            fd: socket.into_raw_fd(),
        })
    }

    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };

        let tcp_stream = Self::create_socket(domain)?;

        Connect::new(tcp_stream.fd, addr).await?;

        Ok(tcp_stream)
    }

    pub fn read<B: IoBufMut>(&self, buf: B) -> Op<Read<B>> {
        Read::new(self.fd, buf, 0)
    }

    pub fn write<B: IoBuf>(&self, buf: B) -> Op<Write<B>> {
        Write::new(self.fd, buf, 0)
    }

    pub fn close(&mut self) -> Op<Close> {
        let fd = self.fd;
        self.fd = -1;

        Close::new(fd)
    }
}

fd_trait!(TcpStream);

impl Drop for TcpStream {
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
