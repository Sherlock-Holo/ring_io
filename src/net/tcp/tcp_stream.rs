use std::cell::UnsafeCell;
use std::future::Future;
use std::io::{Error, ErrorKind, IoSlice, IoSliceMut, Result};
use std::net::Shutdown as HowShutdown;
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};
use io_uring::opcode::Connect as RingConnect;
use io_uring::types::Fd;
use nix::sys::socket;
use nix::sys::socket::sockopt::{ReuseAddr, ReusePort};
use nix::sys::socket::{InetAddr, SockAddr, SockFlag, SockProtocol, SockType};
use nix::unistd;

use crate::cqe_ext::EntryExt;
use crate::driver::DRIVER;
use crate::io::ring_fd::RingFd;
use crate::io::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf};
use crate::net::peek::Peek;
use crate::net::shutdown::Shutdown;

#[derive(Debug)]
pub struct TcpStream(RingFd);

impl TcpStream {
    pub(crate) unsafe fn new(ring_fd: RingFd) -> Self {
        Self(ring_fd)
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        let fd = self.0.as_raw_fd();

        if let SockAddr::Inet(inet_addr) = socket::getsockname(fd)? {
            Ok(inet_addr.to_std())
        } else {
            unreachable!()
        }
    }

    pub fn peer_addr(&self) -> Result<SocketAddr> {
        let fd = self.0.as_raw_fd();

        if let SockAddr::Inet(inet_addr) = socket::getpeername(fd)? {
            Ok(inet_addr.to_std())
        } else {
            unreachable!()
        }
    }

    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let addrs = addr.to_socket_addrs()?;

        let mut last_err = None;

        for addr in addrs {
            match Self::connect_addr(addr).await {
                Err(err) => last_err = Some(err),
                Ok(tcp_stream) => return Ok(tcp_stream),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "could not resolve to any address")
        }))
    }

    pub fn connect_addr(addr: SocketAddr) -> Connect {
        let sockaddr = SockAddr::Inet(InetAddr::from_std(&addr));

        Connect {
            peer_addr: Some(Box::new(sockaddr)),
            fd: None,
            user_data: None,
        }
    }

    pub fn split_half(&mut self) -> (ReadHalf, WriteHalf) {
        let ring_fd: *mut RingFd = &mut self.0;

        // Safety: the read one only do read job, the write on only do write job
        let read_ring_fd = unsafe { &mut *ring_fd };
        let write_ring_fd = unsafe { &mut *ring_fd };

        (ReadHalf::new(read_ring_fd), WriteHalf::new(write_ring_fd))
    }

    pub fn into_split(self) -> (OwnedReadHalf<Self>, OwnedWriteHalf<Self>) {
        let ring_fd = Arc::new(UnsafeCell::new(self.0));

        (
            OwnedReadHalf::new(ring_fd.clone()),
            OwnedWriteHalf::new(ring_fd),
        )
    }

    pub fn shutdown(&self, how: HowShutdown) -> Shutdown {
        Shutdown::new(&self.0, how)
    }

    pub async fn peek(&mut self, buf: &mut [u8]) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // not allow peek to big data, this limit may be removed in the future
        let wan_buf_size = buf.len().min(65536).next_power_of_two();

        let buffer = Peek::new(&mut self.0, wan_buf_size).await?;

        let can_copy_size = buffer.len().min(buf.len());
        (&mut buf[..can_copy_size]).copy_from_slice(&buffer[..can_copy_size]);

        DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.give_back_buffer(buffer);
        });

        Ok(can_copy_size)
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl IntoRawFd for TcpStream {
    fn into_raw_fd(self) -> RawFd {
        self.0.into_raw_fd()
    }
}

impl FromRawFd for TcpStream {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self(RingFd::from_raw_fd(fd))
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.0).poll_read_vectored(cx, bufs)
    }
}

impl AsyncBufRead for TcpStream {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        Pin::new(&mut self.get_mut().0).poll_fill_buf(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.0).consume(amt)
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.0).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.0).poll_close(cx)
    }
}

#[derive(Debug)]
pub struct Connect {
    peer_addr: Option<Box<SockAddr>>,
    fd: Option<RawFd>,
    user_data: Option<u64>,
}

impl Future for Connect {
    type Output = Result<TcpStream>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let connect_cqe = DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().expect("Driver is not running");

                driver
                    .take_cqe_with_waker(user_data, cx.waker())
                    .map(|cqe| cqe.ok())
                    .transpose()
            })?;

            return match connect_cqe {
                None => Poll::Pending,
                Some(connect_cqe) => {
                    // drop won't send useless cancel
                    self.user_data.take();
                    let fd = self.fd.take().unwrap();

                    connect_cqe.ok()?;

                    // Safety: fd is valid
                    Poll::Ready(Ok(unsafe { TcpStream::new(RingFd::from_raw_fd(fd)) }))
                }
            };
        }

        let this = self.get_mut();

        let peer_addr = this.peer_addr.as_ref().expect("peer addr not init");

        let fd = socket::socket(
            peer_addr.family(),
            SockType::Stream,
            SockFlag::SOCK_CLOEXEC,
            SockProtocol::Tcp,
        )?;

        socket::setsockopt(fd, ReusePort, &true)?;
        socket::setsockopt(fd, ReuseAddr, &true)?;

        this.fd.replace(fd);

        let (libc_sockaddr, addr_len) = peer_addr.as_ffi_pair();

        let connect_sqe = RingConnect::new(Fd(fd), libc_sockaddr, addr_len).build();

        let user_data = DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.push_sqe_with_waker(connect_sqe, cx.waker().clone())
        })?;

        user_data.map(|user_data| this.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Connect {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                match driver.as_mut() {
                    None => {}
                    Some(driver) => {
                        let peer_addr = self.peer_addr.take().expect("peer addr not init");

                        let _ = driver.cancel_connect(
                            user_data,
                            peer_addr,
                            self.fd.expect("fd not init"),
                        );
                    }
                }
            })
        } else if let Some(fd) = self.fd {
            let _ = unistd::close(fd);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    use futures_util::lock::BiLock;
    use futures_util::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::{block_on, spawn};

    #[test]
    fn test_connect_v4() {
        block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let connect_stream = TcpStream::connect(addr).await.unwrap();

            assert_eq!(connect_stream.peer_addr().unwrap(), addr);

            let (accept_stream, accept_peer_addr) = handle.join().unwrap().unwrap();

            assert_eq!(connect_stream.local_addr().unwrap(), accept_peer_addr);

            assert_eq!(
                connect_stream.peer_addr().unwrap(),
                accept_stream.local_addr().unwrap()
            );
        })
    }

    #[test]
    fn test_connect_v6() {
        block_on(async {
            let listener = TcpListener::bind("[::1]:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let connect_stream = TcpStream::connect(addr).await.unwrap();

            assert_eq!(connect_stream.peer_addr().unwrap(), addr);

            let (accept_stream, accept_peer_addr) = handle.join().unwrap().unwrap();

            assert_eq!(connect_stream.local_addr().unwrap(), accept_peer_addr);

            assert_eq!(
                connect_stream.peer_addr().unwrap(),
                accept_stream.local_addr().unwrap()
            );
        })
    }

    #[test]
    fn test_write() {
        block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let mut connect_stream = TcpStream::connect(addr).await.unwrap();

            let (mut accept_stream, _) = handle.join().unwrap().unwrap();

            connect_stream.write(b"test").await.unwrap();

            let mut buf = vec![0; 4];

            accept_stream.read_exact(&mut buf).unwrap();

            assert_eq!(&buf, b"test");
        })
    }

    #[test]
    fn test_read() {
        block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let mut connect_stream = TcpStream::connect(addr).await.unwrap();

            let (mut accept_stream, _) = handle.join().unwrap().unwrap();

            accept_stream.write_all(b"test").unwrap();

            let mut buf = vec![0; 4];

            dbg!("written");

            connect_stream.read_exact(&mut buf).await.unwrap();

            assert_eq!(&buf, b"test");
        })
    }

    #[test]
    fn test_multi_write() {
        let mut data = Cursor::new(std::fs::read("testdata/book.txt").unwrap());

        block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let mut connect_stream = TcpStream::connect(addr).await.unwrap();

            let (mut accept_stream, _) = handle.join().unwrap().unwrap();

            let mut read_buf = vec![0; 4096];
            let mut write_buf = vec![0; 4096];

            loop {
                let n = data.read(&mut write_buf).unwrap();
                if n == 0 {
                    break;
                }

                connect_stream.write_all(&write_buf[..n]).await.unwrap();

                accept_stream.read_exact(&mut read_buf[..n]).unwrap();

                assert_eq!(&write_buf[..n], &read_buf[..n]);
            }
        })
    }

    #[test]
    fn test_multi_read() {
        let mut data = Cursor::new(std::fs::read("testdata/book.txt").unwrap());

        block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let mut connect_stream = TcpStream::connect(addr).await.unwrap();

            let (mut accept_stream, _) = handle.join().unwrap().unwrap();

            let mut read_buf = vec![0; 4096];
            let mut write_buf = vec![0; 4096];

            loop {
                let n = data.read(&mut write_buf).unwrap();
                if n == 0 {
                    break;
                }

                dbg!(n);

                accept_stream.write_all(&write_buf[..n]).unwrap();

                connect_stream.read_exact(&mut read_buf[..n]).await.unwrap();

                assert_eq!(&write_buf[..n], &read_buf[..n]);
            }
        })
    }

    #[test]
    fn test_multi_tcp_read() {
        block_on(async move {
            let mut listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let mut connect_streams = vec![];
            let mut accept_streams = vec![];

            // create many tcp so we can test if the GroupBuffer select can create a new GroupBuffer
            // when no available GroupBuffer
            for _ in 0..20 {
                let handle = thread::spawn(move || {
                    let result = listener.accept().map(|(stream, _)| stream);
                    (listener, result)
                });

                let connect_stream = TcpStream::connect(addr).await.unwrap();

                let (l, accept_stream) = handle.join().unwrap();
                let accept_stream = accept_stream.unwrap();

                listener = l;

                connect_streams.push(connect_stream);
                accept_streams.push(accept_stream);
            }

            accept_streams.iter_mut().for_each(|accept_stream| {
                accept_stream.write_all(b"test").unwrap();
            });

            let mut read_tasks = vec![];

            for mut connect_stream in connect_streams {
                let task = spawn(async move {
                    connect_stream.read_exact(&mut [0; 4]).await.unwrap();
                });

                read_tasks.push(task);
            }

            for task in read_tasks {
                task.await;
            }
        })
    }

    #[test]
    fn test_peer_close() {
        block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let mut connect_stream = TcpStream::connect(addr).await.unwrap();

            let (accept_stream, _) = handle.join().unwrap().unwrap();

            drop(accept_stream);

            let mut buf = [0; 1];

            let n = connect_stream.read(&mut buf).await.unwrap();

            assert_eq!(n, 0);
        })
    }

    #[test]
    fn test_close() {
        block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let connect_stream = TcpStream::connect(addr).await.unwrap();

            let (mut accept_stream, _) = handle.join().unwrap().unwrap();

            drop(connect_stream);

            let mut buf = [0; 1];

            let n = accept_stream.read(&mut buf).unwrap();

            assert_eq!(n, 0);
        })
    }

    #[test]
    fn test_async_close() {
        block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let mut connect_stream = TcpStream::connect(addr).await.unwrap();

            let (mut accept_stream, _) = handle.join().unwrap().unwrap();

            connect_stream.close().await.unwrap();

            let n = accept_stream.read(&mut [0; 1]).unwrap();

            assert_eq!(n, 0);
        })
    }

    #[test]
    fn split() {
        block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let mut connect_stream = TcpStream::connect(addr).await.unwrap();

            let (mut accept_stream, _) = handle.join().unwrap().unwrap();

            let (mut read, mut write) = connect_stream.split_half();

            let mut buf = [0; 4];

            write.write_all(b"test").await.unwrap();
            accept_stream.read_exact(&mut buf).unwrap();

            assert_eq!(&buf, b"test");

            accept_stream.write_all(b"test").unwrap();
            read.read_exact(&mut buf).await.unwrap();

            assert_eq!(&buf, b"test");
        })
    }

    #[test]
    fn into_split() {
        block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let connect_stream = TcpStream::connect(addr).await.unwrap();

            let (mut accept_stream, _) = handle.join().unwrap().unwrap();

            let (mut read, mut write) = connect_stream.into_split();

            let mut buf = [0; 4];

            write.write_all(b"test").await.unwrap();
            accept_stream.read_exact(&mut buf).unwrap();

            assert_eq!(&buf, b"test");

            accept_stream.write_all(b"test").unwrap();
            read.read_exact(&mut buf).await.unwrap();

            assert_eq!(&buf, b"test");

            // test for check if reunite works well or not
            let mut connect_stream = read.reunite(write).unwrap();

            connect_stream.write_all(b"test").await.unwrap();
            accept_stream.read_exact(&mut buf).unwrap();
        })
    }

    #[test]
    fn test_shutdown_write() {
        block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let connect_stream = TcpStream::connect(addr).await.unwrap();

            let (mut accept_stream, _) = handle.join().unwrap().unwrap();

            connect_stream.shutdown(HowShutdown::Write).await.unwrap();

            let n = accept_stream.read(&mut [0; 1]).unwrap();

            assert_eq!(n, 0);
        })
    }

    #[test]
    fn test_shutdown_read() {
        block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let connect_stream = TcpStream::connect(addr).await.unwrap();

            let (_accept_stream, _) = handle.join().unwrap().unwrap();

            // actually I don't think there's people use BiLock to do this job, but here BiLock make
            // the test easier
            let (use_to_shutdown, use_to_read) = BiLock::new(connect_stream);

            let task = spawn(async move {
                use_to_read
                    .lock()
                    .await
                    .as_pin_mut()
                    .read(&mut [0; 1])
                    .await
                    .unwrap()
            });

            thread::sleep(Duration::from_secs(1));

            use_to_shutdown
                .lock()
                .await
                .shutdown(HowShutdown::Read)
                .await
                .unwrap();

            assert_eq!(task.await, 0);
        })
    }

    #[test]
    fn test_peek() {
        block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || listener.accept());

            let mut connect_stream = TcpStream::connect(addr).await.unwrap();

            let (mut accept_stream, _) = handle.join().unwrap().unwrap();

            accept_stream.write_all(b"test").unwrap();

            let mut buf = [0; 4];

            dbg!("written");

            let n = connect_stream.peek(&mut buf).await.unwrap();
            assert!(n <= 4);

            assert_eq!(&buf[..n], &b"test"[..n]);

            // check if the data loss or not
            let n = connect_stream.peek(&mut buf).await.unwrap();
            assert!(n <= 4);

            assert_eq!(&buf[..n], &b"test"[..n]);

            connect_stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"test");
        })
    }
}
