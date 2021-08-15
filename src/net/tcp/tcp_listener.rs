use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::net::{SocketAddr, TcpListener as StdTcpListener, ToSocketAddrs};
use std::os::unix::io::{AsRawFd, IntoRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::{FutureExt, Stream};
use io_uring::opcode::Accept as RingAccept;
use io_uring::types::Fd;
use nix::sys::socket;
use nix::sys::socket::{InetAddr, SockAddr};

use crate::cqe_ext::EntryExt;
use crate::driver::DRIVER;
use crate::io::ring_fd::RingFd;
use crate::net::tcp::tcp_stream::TcpStream;

#[derive(Debug)]
pub struct TcpListener {
    // there is no need use io_uring close, normal close(2) is enough
    std_listener: StdTcpListener,
}

impl TcpListener {
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let std_listener = StdTcpListener::bind(addr)?;

        Ok(Self { std_listener })
    }

    pub fn accept(&self) -> Accept {
        // Safey: we use the value after accept success
        let peer_addr: Box<libc::sockaddr_storage> = unsafe { Box::new(mem::zeroed()) };
        let addr_size = Box::new(mem::size_of_val(&*peer_addr) as _);

        Accept {
            listener: self,
            peer_addr: Some(peer_addr),
            addr_size: Some(addr_size),
            user_data: None,
        }
    }

    pub fn incoming(&self) -> Incoming {
        Incoming {
            listener: self,
            accept: None,
        }
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        let addr = socket::getsockname(self.std_listener.as_raw_fd())?;
        if let SockAddr::Inet(addr) = addr {
            Ok(addr.to_std())
        } else {
            unreachable!()
        }
    }
}

impl AsRawFd for TcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.std_listener.as_raw_fd()
    }
}

impl IntoRawFd for TcpListener {
    fn into_raw_fd(self) -> RawFd {
        self.std_listener.into_raw_fd()
    }
}

#[derive(Debug)]
pub struct Accept<'a> {
    listener: &'a TcpListener,
    peer_addr: Option<Box<libc::sockaddr_storage>>,
    addr_size: Option<Box<libc::socklen_t>>,
    user_data: Option<u64>,
}

impl<'a> Future for Accept<'a> {
    type Output = Result<(TcpStream, SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(user_data) = this.user_data {
            let accept_cqe = DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().expect("Driver is not running");

                driver
                    .take_cqe_with_waker(user_data, cx.waker())
                    .map(|cqe| cqe.ok())
                    .transpose()
            })?;

            return match accept_cqe {
                None => Poll::Pending,
                Some(accept_cqe) => {
                    // drop won't send useless cancel
                    this.user_data.take();

                    let tcp_fd = accept_cqe.result();

                    let peer_addr = &**this.peer_addr.as_ref().expect("peer addr is not init");
                    let addr_size = **this.addr_size.as_ref().expect("addr size is not init");

                    let addr = libc_sockaddr_to_socket_addr(peer_addr, addr_size as _)?;

                    // Safety: fd is valid
                    let stream = unsafe { TcpStream::new(RingFd::new(tcp_fd)) };
                    Poll::Ready(Ok((stream, addr)))
                }
            };
        }

        let listener_fd = this.listener.std_listener.as_raw_fd();
        let peer_addr = &mut **this.peer_addr.as_mut().expect("peer addr is not init");
        let addr_size = &mut **this.addr_size.as_mut().expect("peer addr size is not init");

        let accept_sqe = RingAccept::new(
            Fd(listener_fd),
            (peer_addr as *mut libc::sockaddr_storage).cast(),
            addr_size as _,
        )
        .flags(libc::SOCK_CLOEXEC)
        .build();

        let user_data = DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.push_sqe_with_waker(accept_sqe, cx.waker().clone())
        })?;

        user_data.map(|user_data| this.user_data.replace(user_data));

        Poll::Pending
    }
}

impl<'a> Drop for Accept<'a> {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                match driver.as_mut() {
                    None => {}
                    Some(driver) => {
                        let peer_addr = self.peer_addr.take().expect("peer_addr not init");
                        let addr_size = self.addr_size.take().expect("addr_size not init");

                        let _ = driver.cancel_accept(user_data, peer_addr, addr_size);
                    }
                }
            })
        }
    }
}

fn libc_sockaddr_to_socket_addr(
    storage: &libc::sockaddr_storage,
    len: usize,
) -> Result<SocketAddr> {
    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            assert!(len as usize >= mem::size_of::<libc::sockaddr_in>());

            // Safety: storage is v4 addr and libc::sockaddr_in and nix::sys::socket::sockaddr_in
            // have the same layout
            let sockaddr_in = unsafe {
                *(storage as *const _ as *const libc::sockaddr_in
                    as *const nix::sys::socket::sockaddr_in)
            };

            Ok(InetAddr::V4(sockaddr_in).to_std())
        }
        libc::AF_INET6 => {
            assert!(len as usize >= mem::size_of::<libc::sockaddr_in6>());

            // Safety: storage is v6 addr and libc::sockaddr_in6 and nix::sys::socket::sockaddr_in6
            // have the same layout
            let sockaddr_in6 = unsafe {
                *(storage as *const _ as *const libc::sockaddr_in6
                    as *const nix::sys::socket::sockaddr_in6)
            };

            Ok(InetAddr::V6(sockaddr_in6).to_std())
        }
        _ => Err(Error::new(ErrorKind::InvalidInput, "invalid sock addr")),
    }
}

#[derive(Debug)]
pub struct Incoming<'a> {
    listener: &'a TcpListener,
    accept: Option<Accept<'a>>,
}

impl<'a> Stream for Incoming<'a> {
    type Item = Result<TcpStream>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.accept {
            None => {
                let mut accept = self.listener.accept();

                match accept.poll_unpin(cx) {
                    Poll::Pending => {
                        self.accept.replace(accept);

                        Poll::Pending
                    }

                    Poll::Ready(result) => Poll::Ready(Some(result.map(|(stream, _)| stream))),
                }
            }

            Some(accept) => match accept.poll_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    self.accept.take();

                    Poll::Ready(Some(result.map(|(stream, _)| stream)))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpStream as StdTcpStream;
    use std::thread;

    use futures_util::StreamExt;

    use super::*;
    use crate::runtime::Runtime;

    #[test]
    fn test_tcp_listener_bind() {
        Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();

                dbg!(addr);
            })
    }

    #[test]
    fn test_tcp_listener_accept_v4() {
        Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();

                dbg!(addr);

                let handle = thread::spawn(move || StdTcpStream::connect(addr));

                let (_accept_stream, peer_addr) = listener.accept().await.unwrap();
                let connect_stream = handle.join().unwrap().unwrap();

                assert_eq!(addr, connect_stream.peer_addr().unwrap());
                assert_eq!(peer_addr, connect_stream.local_addr().unwrap());
            })
    }

    #[test]
    fn test_tcp_listener_accept_v6() {
        Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async {
                let listener = TcpListener::bind("[::1]:0").await.unwrap();
                let addr = listener.local_addr().unwrap();

                dbg!(addr);

                let handle = thread::spawn(move || StdTcpStream::connect(addr));

                let (_accept_stream, peer_addr) = listener.accept().await.unwrap();
                let connect_stream = handle.join().unwrap().unwrap();

                assert_eq!(addr, connect_stream.peer_addr().unwrap());
                assert_eq!(peer_addr, connect_stream.local_addr().unwrap());
            })
    }

    #[test]
    fn test_tcp_listener_incoming() {
        Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let mut incoming = listener.incoming();

                dbg!(addr);

                {
                    let handle = thread::spawn(move || StdTcpStream::connect(addr));

                    let accept_stream = incoming.next().await.unwrap().unwrap();
                    let connect_stream = handle.join().unwrap().unwrap();

                    assert_eq!(addr, connect_stream.peer_addr().unwrap());
                    assert_eq!(
                        accept_stream.peer_addr().unwrap(),
                        connect_stream.local_addr().unwrap()
                    );
                }

                let handle = thread::spawn(move || StdTcpStream::connect(addr));

                let accept_stream = incoming.next().await.unwrap().unwrap();
                let connect_stream = handle.join().unwrap().unwrap();

                assert_eq!(addr, connect_stream.peer_addr().unwrap());
                assert_eq!(
                    accept_stream.peer_addr().unwrap(),
                    connect_stream.local_addr().unwrap()
                );
            })
    }
}
