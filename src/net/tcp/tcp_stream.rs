use std::fmt::Debug;
use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use std::pin::Pin;
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
use crate::nix_error::NixErrorExt;

pub struct TcpStream(RingFd);

impl TcpStream {
    pub(crate) unsafe fn new(ring_fd: RingFd) -> Self {
        Self(ring_fd)
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        let fd = self.0.as_raw_fd();

        if let SockAddr::Inet(inet_addr) =
            socket::getsockname(fd).map_err(|err| err.into_io_error())?
        {
            Ok(inet_addr.to_std())
        } else {
            unreachable!()
        }
    }

    pub fn peer_addr(&self) -> Result<SocketAddr> {
        let fd = self.0.as_raw_fd();

        if let SockAddr::Inet(inet_addr) =
            socket::getpeername(fd).map_err(|err| err.into_io_error())?
        {
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
        )
        .map_err(|err| err.into_io_error())?;

        socket::setsockopt(fd, ReusePort, &true).map_err(|err| err.into_io_error())?;
        socket::setsockopt(fd, ReuseAddr, &true).map_err(|err| err.into_io_error())?;

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

// pub struct Write {
//     tcp_fd: RawFd,
//     write_buffer: Vec<u8>,
//     data_size: u32,
//     user_data: Option<u64>,
// }
//
// impl Future for Write {
//     type Output = Result<usize>;
//
//     fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if let Some(user_data) = self.user_data {
//             let write_cqe = DRIVER.with(|driver| {
//                 let mut driver = driver.borrow_mut();
//                 let driver = driver.as_mut().expect("Driver is not running");
//
//                 driver
//                     .take_cqe_with_waker(user_data, cx.waker())
//                     .map(|cqe| cqe.ok())
//                     .transpose()
//             })?;
//
//             return match write_cqe {
//                 None => Poll::Pending,
//                 Some(write_cqe) => {
//                     // drop won't send useless cancel
//                     self.user_data.take();
//
//                     Poll::Ready(Ok(write_cqe.result() as _))
//                 }
//             };
//         }
//
//         let write_sqe =
//             RingWrite::new(Fd(self.tcp_fd), self.write_buffer.as_ptr(), self.data_size).build();
//
//         let user_data = DRIVER.with(|driver| {
//             let mut driver = driver.borrow_mut();
//             let driver = driver.as_mut().expect("Driver is not running");
//
//             driver.push_sqe_with_waker(write_sqe, cx.waker().clone())
//         })?;
//
//         user_data.map(|user_data| self.user_data.replace(user_data));
//
//         Poll::Pending
//     }
// }
//
// impl Drop for Write {
//     fn drop(&mut self) {
//         if let Some(user_data) = self.user_data {
//             DRIVER.with(|driver| {
//                 let mut driver = driver.borrow_mut();
//                 match driver.as_mut() {
//                     None => {}
//                     Some(driver) => {
//                         let _ = driver.cancel_normal(user_data);
//                     }
//                 }
//             })
//         }
//     }
// }
//
// struct BufRead {
//     tcp_fd: RawFd,
//     want_buf_size: usize,
//     group_id: Option<u16>,
//     user_data: Option<u64>,
// }
//
// impl Future for BufRead {
//     type Output = Result<Buffer>;
//
//     fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         match (self.group_id, self.user_data) {
//             (Some(group_id), Some(user_data)) => {
//                 let buffer = DRIVER.with(|driver| {
//                     let mut driver = driver.borrow_mut();
//                     let driver = driver.as_mut().expect("Driver is not running");
//
//                     match driver.take_cqe_with_waker(user_data, cx.waker()) {
//                         None => Ok(None),
//                         Some(cqe) => {
//                             // although read is error, we also need to check if buffer is used or not
//                             if cqe.is_err() {
//                                 if let Some(buffer_id) = buffer_select(cqe.flags()) {
//                                     driver.give_back_buffer_with_id(group_id, buffer_id);
//                                 }
//
//                                 return Err(Error::from_raw_os_error(-cqe.result()));
//                             }
//
//                             let buffer_id =
//                                 buffer_select(cqe.flags()).expect("read success but no buffer id");
//
//                             Ok(Some(driver.take_buffer(
//                                 self.want_buf_size,
//                                 group_id,
//                                 buffer_id,
//                                 cqe.result() as _,
//                             )))
//                         }
//                     }
//                 })?;
//
//                 match buffer {
//                     None => Poll::Pending,
//                     Some(buffer) => {
//                         // drop won't send useless cancel
//                         self.user_data.take();
//
//                         // won't decrease on fly by mistake
//                         self.group_id.take();
//
//                         Poll::Ready(Ok(buffer))
//                     }
//                 }
//             }
//
//             (None, Some(_)) => unreachable!(),
//
//             (Some(group_id), None) => {
//                 let sqe = RingRead::new(Fd(self.tcp_fd), ptr::null_mut(), self.want_buf_size as _)
//                     .buf_group(group_id)
//                     .build()
//                     .flags(Flags::BUFFER_SELECT);
//
//                 let user_data = DRIVER.with(|driver| {
//                     let mut driver = driver.borrow_mut();
//                     let driver = driver.as_mut().expect("Driver is not running");
//
//                     driver.push_sqe_with_waker(sqe, cx.waker().clone())
//                 })?;
//
//                 user_data.map(|user_data| self.user_data.replace(user_data));
//
//                 Poll::Pending
//             }
//
//             (None, None) => {
//                 let (group_id, user_data) = DRIVER.with(|driver| {
//                     let mut driver = driver.borrow_mut();
//                     let driver = driver.as_mut().expect("Driver is not running");
//
//                     let group_id = driver.select_group_buffer(self.want_buf_size, cx.waker())?;
//
//                     match group_id {
//                         None => Ok::<_, Error>((None, None)),
//                         Some(group_id) => {
//                             let sqe = RingRead::new(
//                                 Fd(self.tcp_fd),
//                                 ptr::null_mut(),
//                                 self.want_buf_size as _,
//                             )
//                             .buf_group(group_id)
//                             .build()
//                             .flags(Flags::BUFFER_SELECT);
//
//                             let user_data = driver.push_sqe_with_waker(sqe, cx.waker().clone())?;
//
//                             Ok((Some(group_id), user_data))
//                         }
//                     }
//                 })?;
//
//                 if let Some(group_id) = group_id {
//                     self.group_id.replace(group_id);
//
//                     user_data.map(|user_data| self.user_data.replace(user_data));
//                 }
//
//                 Poll::Pending
//             }
//         }
//     }
// }
//
// impl Drop for BufRead {
//     fn drop(&mut self) {
//         DRIVER.with(|driver| {
//             let mut driver = driver.borrow_mut();
//             match driver.as_mut() {
//                 None => {}
//                 Some(driver) => match (self.group_id, self.user_data) {
//                     (Some(group_id), Some(user_data)) => {
//                         let _ = driver.cancel_read(user_data, group_id);
//                     }
//
//                     (Some(group_id), None) => {
//                         driver
//                             .decrease_on_fly_for_not_use_group_buffer(self.want_buf_size, group_id);
//                     }
//
//                     (None, Some(_)) => unreachable!(),
//
//                     (None, None) => {}
//                 },
//             }
//         })
//     }
// }

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use futures_util::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::block_on;

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
}
