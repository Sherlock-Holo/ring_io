use std::future::Future;
use std::io::Result;
use std::net::{TcpListener as StdTcpListener, ToSocketAddrs};
use std::os::unix::io::AsRawFd;
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll};

use io_uring::opcode::Accept as RingAccept;
use io_uring::types::Fd;

use crate::cqe_ext::EntryExt;
use crate::driver::DRIVER;
use crate::net::tcp::tcp_stream::TcpStream;

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

    pub fn accept(&mut self) -> Accept {
        Accept {
            listener: self,
            user_data: None,
        }
    }
}

pub struct Accept<'a> {
    listener: &'a mut TcpListener,
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

                    Poll::Ready(Ok(TcpStream::new(tcp_fd)))
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
