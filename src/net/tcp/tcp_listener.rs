use std::future::Future;
use std::io::Result;
use std::net::{SocketAddr, TcpListener as StdTcpListener, ToSocketAddrs};
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll};

use io_uring::opcode::Accept as RingAccept;
use io_uring::types::Fd;
use nix::sys::socket;
use nix::sys::socket::SockAddr;

use crate::cqe_ext::EntryExt;
use crate::driver::DRIVER;
use crate::io::ring_fd::RingFd;
use crate::net::tcp::tcp_stream::TcpStream;
use crate::nix_error::NixErrorExt;

#[derive(Debug)]
pub struct TcpListener {
    // there is no need use io_uring close, normal close(2) is enough
    std_listener: StdTcpListener,
}

impl TcpListener {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let std_listener = StdTcpListener::bind(addr)?;

        Ok(Self { std_listener })
    }

    pub fn accept(&self) -> Accept {
        Accept {
            listener: self,
            user_data: None,
        }
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        match socket::getsockname(self.std_listener.as_raw_fd()) {
            Err(err) => Err(err.into_io_error()),
            Ok(addr) => match addr {
                SockAddr::Inet(addr) => Ok(addr.to_std()),
                _ => unreachable!(),
            },
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

pub struct Accept<'a> {
    listener: &'a TcpListener,
    user_data: Option<u64>,
}

impl<'a> Future for Accept<'a> {
    type Output = Result<TcpStream>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
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
                    self.user_data.take();

                    let tcp_fd = accept_cqe.result();

                    // Safety: fd is valid
                    Poll::Ready(Ok(unsafe { TcpStream::new(RingFd::from_raw_fd(tcp_fd)) }))
                }
            };
        }

        let listener_fd = self.listener.std_listener.as_raw_fd();

        let accept_sqe = RingAccept::new(Fd(listener_fd), ptr::null_mut(), ptr::null_mut()).build();

        let user_data = DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.push_sqe_with_waker(accept_sqe, cx.waker().clone())
        })?;

        user_data.map(|user_data| self.user_data.replace(user_data));

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
                        let _ = driver.cancel_normal(user_data);
                    }
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpStream as StdTcpStream;
    use std::thread;

    use super::*;
    use crate::block_on;

    #[test]
    fn test_tcp_listener_bind() {
        block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);
        })
    }

    #[test]
    fn test_tcp_listener_accept_v4() {
        block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || StdTcpStream::connect(addr));

            let accept_stream = listener.accept().await.unwrap();
            let connect_stream = handle.join().unwrap().unwrap();

            assert_eq!(addr, connect_stream.peer_addr().unwrap());
            assert_eq!(
                accept_stream.peer_addr().unwrap(),
                connect_stream.local_addr().unwrap()
            );
        })
    }

    #[test]
    fn test_tcp_listener_accept_v6() {
        block_on(async {
            let listener = TcpListener::bind("[::1]:0").unwrap();
            let addr = listener.local_addr().unwrap();

            dbg!(addr);

            let handle = thread::spawn(move || StdTcpStream::connect(addr));

            let accept_stream = listener.accept().await.unwrap();
            let connect_stream = handle.join().unwrap().unwrap();

            assert_eq!(addr, connect_stream.peer_addr().unwrap());
            assert_eq!(
                accept_stream.peer_addr().unwrap(),
                connect_stream.local_addr().unwrap()
            );
        })
    }
}
