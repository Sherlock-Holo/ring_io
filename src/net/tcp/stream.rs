use std::future::Future;
use std::io::Result;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::FromRawFd;
use std::os::unix::io::IntoRawFd;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};
use nix::sys::socket;
use nix::sys::socket::{AddressFamily, SockFlag, SockProtocol, SockType};

use crate::drive::{self, ConnectEvent, DemoDriver, Drive, Event};
use crate::from_nix_err;
use crate::io::{FileDescriptor, ReadHalf, WriteHalf};

type MergeResult<D, T> = std::result::Result<T, (ReadHalf<D, T>, WriteHalf<D, T>)>;

pub struct TcpStream<D> {
    fd: FileDescriptor<D>,
}

impl<D> TcpStream<D> {
    pub(crate) fn new(fd: RawFd, driver: D) -> Self {
        Self {
            fd: FileDescriptor::new(fd, driver, None),
        }
    }

    pub fn connect_with_driver(addr: SocketAddr, driver: D) -> Connect<D> {
        Connect::new_with_driver(addr, driver)
    }

    #[inline]
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        self.get_addr(false)
    }

    #[inline]
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.get_addr(true)
    }

    fn get_addr(&self, is_local: bool) -> Result<SocketAddr> {
        let nix_addr = if is_local {
            socket::getsockname(self.fd.as_raw_fd()).map_err(from_nix_err)?
        } else {
            socket::getpeername(self.fd.as_raw_fd()).map_err(from_nix_err)?
        };

        match nix_addr {
            nix::sys::socket::SockAddr::Inet(inet_addr) => Ok(inet_addr.to_std()),

            addr => Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid sock addr family {:?}", addr.family()),
            )),
        }
    }

    pub fn merge(
        mut read_half: ReadHalf<D, Self>,
        mut write_half: WriteHalf<D, Self>,
    ) -> MergeResult<D, Self> {
        if read_half.fd.merge(&mut write_half.fd).is_err() {
            Err((read_half, write_half))
        } else {
            Ok(Self { fd: read_half.fd })
        }
    }
}

impl<D: Clone> TcpStream<D> {
    pub fn split(self) -> (ReadHalf<D, Self>, WriteHalf<D, Self>) {
        let (fd_r, fd_w) = self.fd.split();

        (ReadHalf::new(fd_r), WriteHalf::new(fd_w))
    }
}

impl TcpStream<DemoDriver> {
    #[inline]
    pub fn connect(addr: SocketAddr) -> Connect<DemoDriver> {
        Connect::new_with_driver(addr, drive::get_default_driver())
    }
}

impl<D: Drive + Unpin> AsyncRead for TcpStream<D> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.fd).poll_read(cx, buf)
    }
}

impl<D: Drive + Unpin> AsyncBufRead for TcpStream<D> {
    #[inline]
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        Pin::new(&mut self.get_mut().fd).poll_fill_buf(cx)
    }

    #[inline]
    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.fd).consume(amt)
    }
}

impl<D: Drive + Unpin> AsyncWrite for TcpStream<D> {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.fd).poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.fd).poll_flush(cx)
    }

    #[inline]
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.fd).poll_close(cx)
    }
}

impl<D> AsRawFd for TcpStream<D> {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl<D> IntoRawFd for TcpStream<D> {
    fn into_raw_fd(self) -> RawFd {
        self.fd.into_raw_fd()
    }
}

impl FromRawFd for TcpStream<DemoDriver> {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        TcpStream::new(fd, drive::get_default_driver())
    }
}

impl From<std::net::TcpStream> for TcpStream<DemoDriver> {
    fn from(std_tcp_stream: std::net::TcpStream) -> Self {
        unsafe { Self::from_raw_fd(std_tcp_stream.into_raw_fd()) }
    }
}

pub struct Connect<D> {
    addr: SocketAddr,
    event: Arc<Event>,
    driver: Option<D>,
    finish: bool,
}

impl<D> Connect<D> {
    pub fn new_with_driver(addr: SocketAddr, driver: D) -> Self {
        Self {
            addr,
            event: Arc::new(Event::Nothing),
            driver: Some(driver),
            finish: false,
        }
    }
}

impl<D: Drive + Unpin> Future for Connect<D> {
    type Output = Result<TcpStream<D>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match &*this.event {
            Event::Nothing => {
                let address_family = if this.addr.is_ipv4() {
                    AddressFamily::Inet
                } else {
                    AddressFamily::Inet6
                };

                let fd = match socket::socket(
                    address_family,
                    SockType::Stream,
                    SockFlag::SOCK_CLOEXEC | SockFlag::SOCK_NONBLOCK,
                    SockProtocol::Tcp,
                ) {
                    Err(err) => {
                        return match err.as_errno() {
                            Some(errno) => Poll::Ready(Err(errno.into())),
                            None => Poll::Ready(Err(Error::new(ErrorKind::Other, err))),
                        };
                    }

                    Ok(fd) => fd,
                };

                let addr = this.addr;

                let event = futures_util::ready!(Pin::new(this.driver.as_mut().unwrap())
                    .poll_prepare(cx, |sqe, cx| {
                        let connect_event = ConnectEvent::new(fd, &addr);

                        unsafe {
                            sqe.prep_connect(fd, &connect_event.addr);
                        }

                        connect_event.waker.register(cx.waker());

                        Arc::new(Event::Connect(Mutex::new(connect_event)))
                    }))?;

                this.event = event;

                futures_util::ready!(
                    Pin::new(this.driver.as_mut().unwrap()).poll_submit(cx, false)
                )?;

                Poll::Pending
            }

            Event::Connect(connect_event) => {
                let mut connect_event = connect_event.lock().unwrap();

                match connect_event.result.take() {
                    None => {
                        connect_event.waker.register(cx.waker());

                        futures_util::ready!(
                            Pin::new(this.driver.as_mut().unwrap()).poll_submit(cx, false)
                        )?;

                        Poll::Pending
                    }

                    Some(result) => {
                        result?;

                        this.finish = true;

                        Poll::Ready(Ok(TcpStream::new(
                            connect_event.fd,
                            this.driver.take().unwrap(),
                        )))
                    }
                }
            }

            _ => unreachable!(),
        }
    }
}

impl<D> Drop for Connect<D> {
    fn drop(&mut self) {
        if !self.finish {
            self.event.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use futures_util::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[test]
    fn test_tcp_stream_connect() {
        let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();

        let addr = listener.local_addr().unwrap();

        futures_executor::block_on(async {
            let _stream = TcpStream::connect(addr).await.unwrap();

            listener.accept().unwrap();
        })
    }

    #[test]
    fn test_tcp_stream_write() {
        let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();

        let addr = listener.local_addr().unwrap();

        futures_executor::block_on(async {
            let mut stream1 = TcpStream::connect(addr).await.unwrap();

            let (mut stream2, _) = listener.accept().unwrap();

            stream1.write_all(b"test").await.unwrap();
            stream1.flush().await.unwrap();

            let mut buf = vec![0; 4];
            stream2.read_exact(&mut buf).unwrap();

            assert_eq!(buf, b"test");
        })
    }

    #[test]
    fn test_tcp_stream_read() {
        let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();

        let addr = listener.local_addr().unwrap();

        futures_executor::block_on(async {
            let mut stream1 = TcpStream::connect(addr).await.unwrap();

            let (mut stream2, _) = listener.accept().unwrap();

            stream2.write_all(b"test").unwrap();
            stream2.flush().unwrap();

            let mut buf = vec![0; 4];
            stream1.read_exact(&mut buf).await.unwrap();

            assert_eq!(buf, b"test");
        })
    }

    #[test]
    fn test_tcp_stream_close() {
        let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();

        let addr = listener.local_addr().unwrap();

        futures_executor::block_on(async {
            let stream1 = TcpStream::connect(addr).await.unwrap();

            let (mut stream2, _) = listener.accept().unwrap();

            drop(stream1);

            assert_eq!(stream2.read(&mut [0; 10]).unwrap(), 0);
        })
    }

    #[test]
    fn test_tcp_stream_read_zero() {
        let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();

        let addr = listener.local_addr().unwrap();

        futures_executor::block_on(async {
            let mut stream1 = TcpStream::connect(addr).await.unwrap();

            let (stream2, _) = listener.accept().unwrap();

            drop(stream2);

            assert_eq!(stream1.read(&mut [0; 10]).await.unwrap(), 0);
        })
    }

    #[test]
    fn test_tcp_stream_split() {
        let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();

        let addr = listener.local_addr().unwrap();

        futures_executor::block_on(async {
            let stream1 = TcpStream::connect(addr).await.unwrap();

            let (mut stream2, _) = listener.accept().unwrap();

            let (mut read, mut write) = stream1.split();

            write.write_all(b"test").await.unwrap();

            let mut buf = vec![0; 4];

            stream2.read_exact(&mut buf).unwrap();

            assert_eq!(buf, b"test");

            stream2.write_all(b"abcd").unwrap();

            read.read_exact(&mut buf).await.unwrap();

            assert_eq!(buf, b"abcd");
        })
    }

    #[test]
    fn test_tcp_stream_merge() {
        let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();

        let addr = listener.local_addr().unwrap();

        futures_executor::block_on(async {
            let stream1 = TcpStream::connect(addr).await.unwrap();

            let (mut stream2, _) = listener.accept().unwrap();

            let (read, write) = stream1.split();

            let stream1 = TcpStream::merge(read, write).unwrap_or_else(|_| panic!("merge failed"));

            drop(stream1);

            assert_eq!(stream2.read(&mut [0]).unwrap(), 0);
        })
    }
}
