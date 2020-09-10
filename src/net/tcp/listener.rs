use std::io;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::future;
use futures_util::stream::Stream;
use nix::sys::socket;
use nix::sys::socket::{AddressFamily, SockFlag, SockProtocol, SockType};

use crate::drive::{self, DemoDriver, Drive};
use crate::from_nix_err;
use crate::io::FileDescriptor;
use crate::net::TcpStream;

const BACKLOG: usize = 128;

pub struct TcpListener<D> {
    fd: FileDescriptor<D>,
}

impl<D> TcpListener<D> {
    pub fn bind_with_driver(addr: SocketAddr, driver: D) -> io::Result<Self> {
        let addr_family = if addr.is_ipv4() {
            AddressFamily::Inet
        } else {
            AddressFamily::Inet6
        };

        let fd = socket::socket(
            addr_family,
            SockType::Stream,
            SockFlag::SOCK_CLOEXEC | SockFlag::SOCK_NONBLOCK,
            SockProtocol::Tcp,
        )
        .map_err(from_nix_err)?;

        socket::bind(
            fd,
            &socket::SockAddr::Inet(socket::InetAddr::from_std(&addr)),
        )
        .map_err(from_nix_err)?;

        socket::listen(fd, BACKLOG).map_err(from_nix_err)?;

        Ok(Self {
            fd: FileDescriptor::new(fd, driver, None),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let nix_addr = socket::getsockname(self.fd.as_raw_fd()).map_err(from_nix_err)?;

        match nix_addr {
            nix::sys::socket::SockAddr::Inet(inet_addr) => Ok(inet_addr.to_std()),

            addr => Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid sock addr family {:?}", addr.family()),
            )),
        }
    }
}

impl TcpListener<DemoDriver> {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        Self::bind_with_driver(addr, drive::get_default_driver())
    }
}

impl<D: Drive + Unpin + Clone> TcpListener<D> {
    #[inline]
    pub fn poll_accept(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<(TcpStream<D>, SocketAddr)>> {
        Pin::new(&mut self.fd).poll_accept(cx)
    }

    #[inline]
    pub async fn accept(&mut self) -> io::Result<(TcpStream<D>, SocketAddr)> {
        future::poll_fn(|cx| Pin::new(&mut self.fd).poll_accept(cx)).await
    }
}

impl<D: Drive + Unpin + Clone> Stream for TcpListener<D> {
    type Item = io::Result<TcpStream<D>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (tcp_stream, _) = futures_util::ready!(Pin::new(&mut self.fd).poll_accept(cx))?;

        Poll::Ready(Some(Ok(tcp_stream)))
    }
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;

    use super::*;

    #[test]
    fn test_tcp_listener_bind() {
        futures_executor::block_on(async {
            let _listener = TcpListener::bind("0.0.0.0:0".parse().unwrap()).unwrap();

            // let stream = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        })
    }

    #[test]
    fn test_tcp_listener_accept() {
        futures_executor::block_on(async {
            let mut listener = TcpListener::bind("0.0.0.0:0".parse().unwrap()).unwrap();

            let _stream = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();

            listener.accept().await.unwrap();
        })
    }

    #[test]
    fn test_tcp_listener_close() {
        futures_executor::block_on(async {
            let listener = TcpListener::bind("0.0.0.0:0".parse().unwrap()).unwrap();

            let addr = listener.local_addr().unwrap();

            drop(listener);

            assert!(std::net::TcpStream::connect(addr).is_err());
        })
    }

    #[test]
    fn test_tcp_listener_stream() {
        futures_executor::block_on(async {
            let mut listener = TcpListener::bind("0.0.0.0:0".parse().unwrap()).unwrap();

            let _stream = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();

            listener.next().await.unwrap().unwrap();
        })
    }
}
