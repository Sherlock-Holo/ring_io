use std::future::Future;
use std::io::{Error, Result};
use std::mem;
use std::net::TcpStream as StdTcpStream;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};
use io_uring::cqueue::buffer_select;
use io_uring::opcode::Read as RingRead;
use io_uring::opcode::Write as RingWrite;
use io_uring::types::Fd;

use crate::buffer::{Buffer, GroupBuffer};
use crate::cqe_ext::EntryExt;
use crate::driver::{CancelCallback, DRIVER};

enum ReadState {
    Idle(usize),
    Reading(BufRead),
    Done(Buffer),
}

enum WriteState {
    NotInit,
    Idle { write_buffer: Vec<u8> },
    Writing(Write),
}

pub struct TcpStream {
    std_stream: StdTcpStream,
    read_state: ReadState,
    write_state: WriteState,
}

impl TcpStream {
    pub(crate) fn new(fd: RawFd) -> Self {
        // Safety: lib will promise fd is valid
        let std_stream = unsafe { StdTcpStream::from_raw_fd(fd) };

        Self {
            std_stream,
            read_state: ReadState::Idle(0),
            write_state: WriteState::NotInit,
        }
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        const MAX_WANT_SIZE: usize = 65535;

        if let ReadState::Idle(want_size) = &mut self.read_state {
            *want_size = buf.len().min(MAX_WANT_SIZE);
        }

        match self.as_mut().poll_fill_buf(cx) {
            Poll::Pending => Poll::Pending,

            Poll::Ready(result) => {
                let read_data = result?;

                let copy_len = read_data.len().min(read_data.len());

                (&mut buf[..copy_len]).copy_from_slice(&read_data[..copy_len]);

                self.consume(copy_len);

                Poll::Ready(Ok(copy_len))
            }
        }
    }
}

impl AsyncBufRead for TcpStream {
    fn poll_fill_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        let read_state = mem::replace(&mut self.read_state, ReadState::Idle(0));

        let mut buf_read_fut = match read_state {
            ReadState::Idle(mut want_size) => {
                // if choose group id is 0, means call poll_fill_buf directly
                if want_size == 0 {
                    want_size = 65535;
                }

                let group_id = want_size.next_power_of_two().min(65535) as u16;

                BufRead {
                    tcp_fd: self.std_stream.as_raw_fd(),
                    group_id,
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

                        let group_buffer = driver
                            .buffers
                            .get_mut(&buffer.group_id())
                            .expect("GroupBuffer not found");

                        // ignore register failed
                        let _ = group_buffer.register_buffer(buffer);
                    });
                } else {
                    unreachable!()
                }
            }
        }
    }
}

impl AsyncWrite for TcpStream {
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
                    tcp_fd: self.std_stream.as_raw_fd(),
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
                    tcp_fd: self.std_stream.as_raw_fd(),
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
        let write_state = mem::replace(&mut self.write_state, WriteState::NotInit);
        if let WriteState::Writing(write_fut) = write_state {
            drop(write_fut);
        }

        let result = unsafe {
            let errno = libc::close(self.std_stream.as_raw_fd());
            if errno < 0 {
                Err(Error::from_raw_os_error(-errno))
            } else {
                Ok(())
            }
        };

        Poll::Ready(result)
    }
}

pub struct Write {
    tcp_fd: RawFd,
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

                match driver.available_entries.remove(&user_data) {
                    None => {
                        *driver
                            .wakers
                            .get_mut(&user_data)
                            .expect("inserted write waker not found") = cx.waker().clone();

                        Ok::<_, Error>(None)
                    }
                    Some(write_cqe) => {
                        let write_cqe = write_cqe.ok()?;

                        Ok(Some(write_cqe))
                    }
                }
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
            RingWrite::new(Fd(self.tcp_fd), self.write_buffer.as_ptr(), self.data_size).build();

        let user_data = DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.push_sqe(write_sqe, cx.waker().clone())
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
                        driver.cancel_user_data.insert(
                            user_data,
                            CancelCallback::CancelWithoutCallback { user_data },
                        );

                        let _ = driver.cancel_sqe(user_data);
                    }
                }
            })
        }
    }
}

pub struct BufRead {
    tcp_fd: RawFd,
    group_id: u16,
    user_data: Option<u64>,
}

impl Future for BufRead {
    type Output = Result<Buffer>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let buffer = DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().expect("Driver is not running");

                match driver.available_entries.remove(&user_data) {
                    None => {
                        *driver
                            .wakers
                            .get_mut(&user_data)
                            .expect("inserted read waker not found") = cx.waker().clone();

                        Ok::<_, Error>(None)
                    }
                    Some(read_cqe) => {
                        let read_cqe = read_cqe.ok()?;

                        let buffer_id = buffer_select(read_cqe.flags())
                            .expect("buffer id is not in Read flags");

                        let group_buffers = driver
                            .buffers
                            .get_mut(&self.group_id)
                            .expect("group buffer not found");

                        let mut buffer = group_buffers
                            .select_buffer(buffer_id)
                            .expect("buffer not found");

                        buffer.set_available_len(read_cqe.result() as usize);

                        Ok(Some(buffer))
                    }
                }
            })?;

            return match buffer {
                None => Poll::Pending,
                Some(buffer) => {
                    // drop won't send useless cancel
                    self.user_data.take();

                    Poll::Ready(Ok(buffer))
                }
            };
        }

        let ring_read = RingRead::new(Fd(self.tcp_fd), ptr::null_mut(), self.group_id as _);

        let user_data = DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            let mut group_buffer;

            loop {
                group_buffer = driver
                    .buffers
                    .entry(self.group_id)
                    .or_insert_with(|| GroupBuffer::new(self.group_id, 10));

                if group_buffer.is_empty() {
                    if group_buffer.allocate() {
                        break;
                    }

                    // max group_id is 65535
                    if self.group_id == 65535 {
                        driver.wait_for_push_wakers.push_back(cx.waker().clone());

                        return Ok(None);
                    } else if self.group_id == 32768 {
                        self.group_id = 65535;
                    } else {
                        self.group_id *= 2;
                    }
                }
            }

            let read_sqe = ring_read.buf_group(group_buffer.group_id()).build();

            driver.push_sqe(read_sqe, cx.waker().clone())
        })?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for BufRead {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                match driver.as_mut() {
                    None => {}
                    Some(driver) => {
                        driver.cancel_user_data.insert(
                            user_data,
                            CancelCallback::CancelAndRegisterBuffer {
                                user_data,
                                group_id: self.group_id,
                            },
                        );

                        let _ = driver.cancel_sqe(user_data);
                    }
                }
            })
        }
    }
}
