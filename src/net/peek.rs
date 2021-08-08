use std::future::Future;
use std::io::{Error, Result};
use std::os::unix::io::AsRawFd;
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll};

use io_uring::cqueue::buffer_select;
use io_uring::opcode::Recv as RingRecv;
use io_uring::squeue::Flags;
use io_uring::types::Fd;

use crate::buffer::Buffer;
use crate::cqe_ext::EntryExt;
use crate::driver::DRIVER;
use crate::io::ring_fd::RingFd;

pub(crate) struct Peek<'a> {
    fd: &'a mut RingFd,
    want_buf_size: usize,
    group_id: Option<u16>,
    user_data: Option<u64>,
}

impl<'a> Peek<'a> {
    pub(crate) fn new(fd: &'a mut RingFd, want_buf_size: usize) -> Self {
        Self {
            fd,
            want_buf_size,
            group_id: None,
            user_data: None,
        }
    }
}

impl<'a> Future for Peek<'a> {
    type Output = Result<Buffer>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match (self.group_id, self.user_data) {
            (Some(group_id), Some(user_data)) => {
                let buffer = DRIVER.with(|driver| {
                    let mut driver = driver.borrow_mut();
                    let driver = driver.as_mut().expect("Driver is not running");

                    match driver.take_cqe_with_waker(user_data, cx.waker()) {
                        None => Ok(None),
                        Some(cqe) => {
                            // although read is error, we also need to check if buffer is used or not
                            if cqe.is_err() {
                                if let Some(buffer_id) = buffer_select(cqe.flags()) {
                                    driver.give_back_buffer_with_id(group_id, buffer_id);
                                }

                                return Err(Error::from_raw_os_error(-cqe.result()));
                            }

                            let buffer_id =
                                buffer_select(cqe.flags()).expect("read success but no buffer id");

                            Ok(Some(driver.take_buffer(
                                self.want_buf_size,
                                group_id,
                                buffer_id,
                                cqe.result() as _,
                            )))
                        }
                    }
                })?;

                match buffer {
                    None => Poll::Pending,
                    Some(buffer) => {
                        // drop won't send useless cancel
                        self.user_data.take();

                        // won't decrease on fly by mistake
                        self.group_id.take();

                        Poll::Ready(Ok(buffer))
                    }
                }
            }

            (None, Some(_)) => unreachable!(),

            (Some(group_id), None) => {
                let sqe = RingRecv::new(
                    Fd(self.fd.as_raw_fd()),
                    ptr::null_mut(),
                    self.want_buf_size as _,
                )
                .flags(libc::MSG_PEEK)
                .buf_group(group_id)
                .build()
                .flags(Flags::BUFFER_SELECT);

                let user_data = DRIVER.with(|driver| {
                    let mut driver = driver.borrow_mut();
                    let driver = driver.as_mut().expect("Driver is not running");

                    driver.push_sqe_with_waker(sqe, cx.waker().clone())
                })?;

                user_data.map(|user_data| self.user_data.replace(user_data));

                Poll::Pending
            }

            (None, None) => {
                let (group_id, user_data) = DRIVER.with(|driver| {
                    let mut driver = driver.borrow_mut();
                    let driver = driver.as_mut().expect("Driver is not running");

                    let group_id = driver.select_group_buffer(self.want_buf_size, cx.waker())?;

                    match group_id {
                        None => Ok::<_, Error>((None, None)),
                        Some(group_id) => {
                            let sqe = RingRecv::new(
                                Fd(self.fd.as_raw_fd()),
                                ptr::null_mut(),
                                self.want_buf_size as _,
                            )
                            .flags(libc::MSG_PEEK)
                            .buf_group(group_id)
                            .build()
                            .flags(Flags::BUFFER_SELECT);

                            let user_data = driver.push_sqe_with_waker(sqe, cx.waker().clone())?;

                            Ok((Some(group_id), user_data))
                        }
                    }
                })?;

                if let Some(group_id) = group_id {
                    self.group_id.replace(group_id);

                    user_data.map(|user_data| self.user_data.replace(user_data));
                }

                Poll::Pending
            }
        }
    }
}

impl<'a> Drop for Peek<'a> {
    fn drop(&mut self) {
        DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            match driver.as_mut() {
                None => {}
                Some(driver) => match (self.group_id, self.user_data) {
                    (Some(group_id), Some(user_data)) => {
                        let _ = driver.cancel_read(user_data, group_id);
                    }

                    (Some(group_id), None) => {
                        driver
                            .decrease_on_fly_for_not_use_group_buffer(self.want_buf_size, group_id);
                    }

                    (None, Some(_)) => unreachable!(),

                    (None, None) => {}
                },
            }
        })
    }
}
