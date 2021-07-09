use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::io::{Error, Result};
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll};
use std::{fmt, mem};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};
use io_uring::cqueue::buffer_select;
use io_uring::opcode::Read as RingRead;
use io_uring::opcode::Write as RingWrite;
use io_uring::squeue::Flags;
use io_uring::types::Fd;
use nix::unistd;

use crate::buffer::Buffer;
use crate::cqe_ext::EntryExt;
use crate::driver::DRIVER;

enum ReadState {
    Idle(usize),
    Reading(BufRead),
    Done(Buffer),
}

impl Debug for ReadState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ReadState::Idle(_) => write!(f, "Idle"),
            ReadState::Reading(_) => write!(f, "Reading"),
            ReadState::Done(_) => write!(f, "Done"),
        }
    }
}

enum WriteState {
    NotInit,
    Idle { write_buffer: Vec<u8> },
    Writing(Write),
}

impl Debug for WriteState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let s = match self {
            WriteState::NotInit => "NotInit",
            WriteState::Idle { .. } => "Idle",
            WriteState::Writing(_) => "Writing",
        };

        f.write_str(s)
    }
}

#[derive(Debug)]
pub struct RingFd {
    fd: RawFd,
    need_close_fd: bool,
    read_state: ReadState,
    write_state: WriteState,
}

impl RingFd {
    pub(crate) unsafe fn new(fd: RawFd) -> Self {
        Self {
            fd,
            need_close_fd: true,
            read_state: ReadState::Idle(0),
            write_state: WriteState::NotInit,
        }
    }
}

impl AsRawFd for RingFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl IntoRawFd for RingFd {
    fn into_raw_fd(mut self) -> RawFd {
        self.need_close_fd = false;

        self.fd
    }
}

impl FromRawFd for RingFd {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self::new(fd)
    }
}

impl Drop for RingFd {
    fn drop(&mut self) {
        if self.need_close_fd {
            let _ = unistd::close(self.fd);
        }
    }
}

impl AsyncRead for RingFd {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        const MAX_WANT_SIZE: usize = 128 * 1024;

        if let ReadState::Idle(want_size) = &mut self.read_state {
            *want_size = buf.len().min(MAX_WANT_SIZE);
        }

        match self.as_mut().poll_fill_buf(cx) {
            Poll::Pending => Poll::Pending,

            Poll::Ready(result) => {
                let read_data = result?;

                let copy_len = read_data.len().min(buf.len());

                (&mut buf[..copy_len]).copy_from_slice(&read_data[..copy_len]);

                self.consume(copy_len);

                Poll::Ready(Ok(copy_len))
            }
        }
    }
}

impl AsyncBufRead for RingFd {
    fn poll_fill_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        let read_state = mem::replace(&mut self.read_state, ReadState::Idle(0));

        let mut buf_read_fut = match read_state {
            ReadState::Idle(mut want_size) => {
                // if choose group id is 0, means call poll_fill_buf directly
                if want_size == 0 {
                    want_size = 65536;
                } else {
                    want_size = want_size.next_power_of_two();
                }

                BufRead {
                    fd: self.fd,
                    want_buf_size: want_size,
                    group_id: None,
                    user_data: None,
                }
            }

            ReadState::Reading(buf_read_fut) => buf_read_fut,

            read_state @ ReadState::Done(_) => {
                self.read_state = read_state;

                if let ReadState::Done(buffer) = &self.get_mut().read_state {
                    return Poll::Ready(Ok(buffer));
                } else {
                    unreachable!()
                }
            }
        };

        match Pin::new(&mut buf_read_fut).poll(cx) {
            Poll::Ready(result) => match result {
                Err(err) => Poll::Ready(Err(err)),
                Ok(buffer) => {
                    self.read_state = ReadState::Done(buffer);

                    if let ReadState::Done(buffer) = &self.get_mut().read_state {
                        Poll::Ready(Ok(buffer))
                    } else {
                        unreachable!()
                    }
                }
            },
            Poll::Pending => {
                self.read_state = ReadState::Reading(buf_read_fut);

                Poll::Pending
            }
        }
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        if let ReadState::Done(buffer) = &mut self.read_state {
            buffer.consume(amt);

            if buffer.is_empty() {
                let read_state = mem::replace(&mut self.read_state, ReadState::Idle(0));
                if let ReadState::Done(buffer) = read_state {
                    DRIVER.with(|driver| {
                        let mut driver = driver.borrow_mut();
                        let driver = driver.as_mut().expect("Driver is not running");

                        driver.give_back_buffer(buffer);
                    });
                } else {
                    unreachable!()
                }
            }
        }
    }
}

impl AsyncWrite for RingFd {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: &[u8],
    ) -> Poll<Result<usize>> {
        const MAX_WRITE_BUFFER_SIZE: usize = 65535;

        let write_state = mem::replace(&mut self.write_state, WriteState::NotInit);

        let mut write_fut = match write_state {
            WriteState::NotInit => {
                if buf.len() > MAX_WRITE_BUFFER_SIZE {
                    buf = &buf[..MAX_WRITE_BUFFER_SIZE];
                }

                let write_buffer = buf.to_vec();

                Write {
                    fd: self.fd,
                    write_buffer,
                    data_size: buf.len() as _,
                    user_data: None,
                }
            }

            WriteState::Idle { mut write_buffer } => {
                if buf.len() <= write_buffer.len() {
                    (&mut write_buffer[..buf.len()]).copy_from_slice(buf);
                } else {
                    if buf.len() > MAX_WRITE_BUFFER_SIZE {
                        buf = &buf[..MAX_WRITE_BUFFER_SIZE];
                    }

                    write_buffer.resize(buf.len(), 0);

                    write_buffer.copy_from_slice(buf);
                }

                Write {
                    fd: self.fd,
                    write_buffer,
                    data_size: buf.len() as _,
                    user_data: None,
                }
            }

            WriteState::Writing(write_fut) => write_fut,
        };

        match Pin::new(&mut write_fut).poll(cx) {
            Poll::Ready(result) => {
                let write_buffer = mem::take(&mut write_fut.write_buffer);

                self.write_state = WriteState::Idle { write_buffer };

                Poll::Ready(result)
            }

            Poll::Pending => {
                self.write_state = WriteState::Writing(write_fut);

                Poll::Pending
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        if !self.need_close_fd {
            return Poll::Ready(Ok(()));
        }

        self.need_close_fd = false;

        let write_state = mem::replace(&mut self.write_state, WriteState::NotInit);
        if let WriteState::Writing(write_fut) = write_state {
            drop(write_fut);
        }

        let result = unsafe {
            let errno = libc::close(self.fd);
            if errno < 0 {
                Err(Error::from_raw_os_error(-errno))
            } else {
                Ok(())
            }
        };

        Poll::Ready(result)
    }
}

struct Write {
    fd: RawFd,
    write_buffer: Vec<u8>,
    data_size: u32,
    user_data: Option<u64>,
}

impl Future for Write {
    type Output = Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let write_cqe = DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().expect("Driver is not running");

                driver
                    .take_cqe_with_waker(user_data, cx.waker())
                    .map(|cqe| cqe.ok())
                    .transpose()
            })?;

            return match write_cqe {
                None => Poll::Pending,
                Some(write_cqe) => {
                    // drop won't send useless cancel
                    self.user_data.take();

                    Poll::Ready(Ok(write_cqe.result() as _))
                }
            };
        }

        let write_sqe =
            RingWrite::new(Fd(self.fd), self.write_buffer.as_ptr(), self.data_size).build();

        let user_data = DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.push_sqe_with_waker(write_sqe, cx.waker().clone())
        })?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Write {
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

struct BufRead {
    fd: RawFd,
    want_buf_size: usize,
    group_id: Option<u16>,
    user_data: Option<u64>,
}

impl Future for BufRead {
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
                let sqe = RingRead::new(Fd(self.fd), ptr::null_mut(), self.want_buf_size as _)
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
                            let sqe = RingRead::new(
                                Fd(self.fd),
                                ptr::null_mut(),
                                self.want_buf_size as _,
                            )
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

impl Drop for BufRead {
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
