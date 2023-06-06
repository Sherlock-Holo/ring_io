use std::io;
use std::mem::ManuallyDrop;
use std::net::{Shutdown, SocketAddr};
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};
use std::os::raw::c_int;

use socket2::{Domain, Socket, Type};

use crate::buf::{IoBuf, IoBufMut};
use crate::io::WriteAll;
use crate::op::{MultiOp, Op};
use crate::opcode::{Close, Connect, Read, ReadWithBufRing, RecvMulti, Write};
use crate::per_thread::runtime::in_per_thread_runtime;
use crate::runtime::spawn;
use crate::{fd_trait, opcode};

#[derive(Debug)]
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

    pub fn from_std(tcp_stream: std::net::TcpStream) -> Self {
        Self {
            fd: tcp_stream.into_raw_fd(),
        }
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

    pub fn read_with_buf_ring(&self, buffer_group: u16) -> Op<ReadWithBufRing> {
        ReadWithBufRing::new(self.fd, buffer_group, 0)
    }

    pub fn recv_multi(&self, buffer_group: u16) -> MultiOp<RecvMulti> {
        RecvMulti::new(self.fd, buffer_group)
    }

    pub fn write<B: IoBuf>(&self, buf: B) -> Op<Write<B>> {
        Write::new(self.fd, buf, 0)
    }

    pub fn write_all<B: IoBuf>(&self, buf: B) -> WriteAll<B, Self> {
        WriteAll::new(self, buf)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let std_stream = ManuallyDrop::new(unsafe { std::net::TcpStream::from_raw_fd(self.fd) });

        std_stream.local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        let std_stream = ManuallyDrop::new(unsafe { std::net::TcpStream::from_raw_fd(self.fd) });

        std_stream.peer_addr()
    }

    pub fn shutdown(&self, how: Shutdown) -> Op<opcode::Shutdown> {
        opcode::Shutdown::new(self.fd, how)
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

        if in_per_thread_runtime() {
            spawn(self.close()).detach();
        } else {
            unsafe {
                libc::close(self.fd);
            }
        }
    }
}
