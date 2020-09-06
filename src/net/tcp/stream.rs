use std::future::Future;
use std::io::Result;
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};
use iou::InetAddr;

use crate::drive::{self, ConnectEvent, DemoDriver, Drive, Event};
use crate::file_descriptor::FileDescriptor;
use crate::net::socket;

pub struct TcpStream<D> {
    fd: FileDescriptor<D>,
    // local_addr: SocketAddr,
    peer_addr: SocketAddr,
}

impl<D> TcpStream<D> {
    fn new(fd: RawFd, remote_addr: SocketAddr, driver: D) -> Self {
        Self {
            fd: FileDescriptor::new(fd, driver, None),
            peer_addr: remote_addr,
        }
    }

    pub fn connect_with_driver(addr: SocketAddr, driver: D) -> Connect<D> {
        Connect::new_with_driver(addr, driver)
    }

    pub fn peer_addr(&self) -> Result<SocketAddr> {
        Ok(self.peer_addr)
    }
}

impl TcpStream<DemoDriver> {
    pub fn connect(addr: SocketAddr) -> Connect<DemoDriver> {
        Connect::new_with_driver(addr, drive::get_default_driver())
    }
}

impl<D: Drive + Unpin> AsyncRead for TcpStream<D> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.fd).poll_read(cx, buf)
    }
}

impl<D: Drive + Unpin> AsyncBufRead for TcpStream<D> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        Pin::new(&mut self.get_mut().fd).poll_fill_buf(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.fd).consume(amt)
    }
}

impl<D: Drive + Unpin> AsyncWrite for TcpStream<D> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.fd).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.fd).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.fd).poll_close(cx)
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
                let (fd, addr) = socket::socket(this.addr)?;

                this.addr = addr;

                let event = futures_util::ready!(Pin::new(this.driver.as_mut().unwrap())
                    .poll_prepare(cx, |sqe, cx| {
                        unsafe {
                            sqe.prep_connect(
                                fd,
                                &iou::SockAddr::new_inet(InetAddr::from_std(&addr)),
                            );
                        }

                        let connect_event = ConnectEvent::new(fd);

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
                            this.addr,
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
}
