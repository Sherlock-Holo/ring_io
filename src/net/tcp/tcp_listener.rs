use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::io;
use std::mem::ManuallyDrop;
use std::net::SocketAddr;
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};
use std::os::raw::c_int;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures_util::{FutureExt, Stream};
use socket2::{Domain, SockAddr, Socket, Type};

use crate::fd_trait;
use crate::net::tcp::TcpStream;
use crate::op::Op;
use crate::opcode::{self, Close};
use crate::per_thread::runtime::in_per_thread_runtime;
use crate::runtime::spawn;

#[derive(Debug)]
pub struct TcpListener {
    fd: RawFd,
}

impl TcpListener {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };

        let socket_type = c_int::from(Type::STREAM) | libc::SOCK_CLOEXEC;
        let socket = Socket::new(domain, socket_type.into(), None)?;

        socket.set_reuse_address(true)?;
        socket.bind(&SockAddr::from(addr))?;
        socket.listen(128)?;

        Ok(Self {
            fd: socket.into_raw_fd(),
        })
    }

    pub fn from_std(tcp_listener: std::net::TcpListener) -> Self {
        Self {
            fd: tcp_listener.into_raw_fd(),
        }
    }

    pub fn accept(&self) -> Accept {
        Accept {
            op: opcode::Accept::new(self.fd),
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let std_listener =
            ManuallyDrop::new(unsafe { std::net::TcpListener::from_raw_fd(self.fd) });

        std_listener.local_addr()
    }

    pub fn close(&mut self) -> Op<Close> {
        let fd = self.fd;
        self.fd = -1;

        Close::new(fd)
    }
}

fd_trait!(TcpListener);

impl Drop for TcpListener {
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

#[derive(Debug)]
pub struct TcpListenerStream {
    listener: TcpListener,
    fut: Option<Accept>,
}

impl TcpListenerStream {
    pub fn into_listener(self) -> (TcpListener, Option<Accept>) {
        (self.listener, self.fut)
    }
}

impl From<TcpListener> for TcpListenerStream {
    fn from(value: TcpListener) -> Self {
        Self {
            listener: value,
            fut: None,
        }
    }
}

impl Stream for TcpListenerStream {
    type Item = io::Result<TcpStream>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.fut.as_mut() {
                None => {
                    let accept = self.listener.accept();

                    self.fut = Some(accept);
                }

                Some(fut) => {
                    return Poll::Ready(Some(ready!(fut.poll_unpin(cx)).map(|(stream, _)| stream)));
                }
            }
        }
    }
}

pub struct Accept {
    op: Op<opcode::Accept>,
}

impl Debug for Accept {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Accept").finish_non_exhaustive()
    }
}

impl Future for Accept {
    type Output = io::Result<(TcpStream, SocketAddr)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (fd, addr) = ready!(self.op.poll_unpin(cx))?;

        // Safety: fd is valid
        unsafe { Poll::Ready(Ok((TcpStream::from_raw_fd(fd), addr))) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_on;

    #[test]
    fn test() {
        block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();

            let task = spawn(async move { listener.accept().await.unwrap().0 });

            let stream1 = TcpStream::connect(addr).await.unwrap();
            let stream2 = task.await;

            assert_eq!(stream1.write_all(b"test").await.0.unwrap(), 4);

            let buf = vec![0; 4];
            let (result, buf) = stream2.read(buf).await;
            assert_eq!(result.unwrap(), 4);

            assert_eq!(buf, b"test");
        });
    }
}
