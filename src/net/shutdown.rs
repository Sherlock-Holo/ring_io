use std::future::Future;
use std::io::Result;
use std::net::Shutdown as HowShutdown;
use std::os::unix::io::AsRawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::opcode::Shutdown as RingShutdown;
use io_uring::types::Fd;

use crate::cqe_ext::EntryExt;
use crate::driver::DRIVER;
use crate::io::ring_fd::RingFd;

#[derive(Debug)]
pub struct Shutdown<'a> {
    ring_fd: &'a RingFd,
    user_data: Option<u64>,
    how: HowShutdown,
}

impl<'a> Shutdown<'a> {
    pub(crate) fn new(ring_fd: &'a RingFd, how: HowShutdown) -> Self {
        Self {
            ring_fd,
            user_data: None,
            how,
        }
    }
}

impl<'a> Future for Shutdown<'a> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let shutdown_cqe = DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().expect("Driver is not running");

                driver
                    .take_cqe_with_waker(user_data, cx.waker())
                    .map(|cqe| cqe.ok())
                    .transpose()
            })?;

            return match shutdown_cqe {
                None => Poll::Pending,
                Some(_) => Poll::Ready(Ok(())),
            };
        }

        let how = match self.how {
            HowShutdown::Read => libc::SHUT_RD,
            HowShutdown::Write => libc::SHUT_WR,
            HowShutdown::Both => libc::SHUT_RDWR,
        };

        let shutdown_sqe = RingShutdown::new(Fd(self.ring_fd.as_raw_fd()), how).build();

        let user_data = DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.push_sqe_with_waker(shutdown_sqe, cx.waker().clone())
        })?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl<'a> Drop for Shutdown<'a> {
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
