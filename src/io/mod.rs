use std::ffi::CString;
use std::io::Error;
use std::io::ErrorKind;
use std::io::SeekFrom;
use std::io::{Read, Result, Write};
use std::net::SocketAddr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::IntoRawFd;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Buf;
use futures_io::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWrite};
use nix::sys::socket;
use parking_lot::Mutex;

pub use read_half::*;
pub use write_half::*;

use crate::drive::{AcceptEvent, Drive, Event, ReadEvent, RecvEvent, SendEvent, WriteEvent};
use crate::from_nix_err;
use crate::fs::Open;
use crate::io::buffer::Buffer;
use crate::net::TcpStream;

pub(crate) mod buffer;
mod read_half;
mod write_half;

#[derive(Debug, Clone)]
enum RawFdState {
    Closed,
    Opened(RawFd),
    Shared(RawFd, Arc<AtomicU8>),
}

pub(crate) struct FileDescriptor<D> {
    fd: RawFdState,
    buf: Option<Buffer>,
    event: Arc<Event>,
    offset: Option<usize>,
    driver: D,
}

impl<D> FileDescriptor<D> {
    pub(crate) fn new(fd: RawFd, driver: D, offset: impl Into<Option<usize>>) -> Self {
        Self {
            fd: RawFdState::Opened(fd),
            buf: None,
            event: Arc::new(Event::Nothing),
            offset: offset.into(),
            driver,
        }
    }

    pub fn cancel(&mut self) {
        if let Some(buf) = &mut self.buf {
            buf.reset();
        }

        self.event.cancel();

        self.event = Arc::new(Event::Nothing);
    }

    pub fn merge(&mut self, other: &mut Self) -> std::result::Result<(), ()> {
        match &self.fd {
            RawFdState::Shared(fd1, _) => {
                if let RawFdState::Shared(fd2, _) = &other.fd {
                    if *fd1 != *fd2 {
                        return Err(());
                    }
                } else {
                    return Err(());
                }

                let fd = *fd1;

                self.cancel();
                other.cancel();

                self.fd = RawFdState::Opened(fd);

                Ok(())
            }

            _ => Err(()),
        }
    }

    fn get_fd(&self) -> Result<RawFd> {
        match &self.fd {
            RawFdState::Opened(fd) => Ok(*fd),
            RawFdState::Shared(fd, _) => Ok(*fd),
            RawFdState::Closed => Err(Error::from_raw_os_error(libc::EBADFD)),
        }
    }

    pub fn get_driver_mut(&mut self) -> &mut D {
        &mut self.driver
    }
}

impl<D: Clone> FileDescriptor<D> {
    pub fn split(mut self) -> (Self, Self) {
        match self.fd {
            RawFdState::Opened(fd) => {
                let fd1 = RawFdState::Shared(fd, Arc::new(AtomicU8::new(2)));
                let fd2 = fd1.clone();

                self.fd = fd1;
                let offset = self.offset;
                let driver = self.driver.clone();

                (
                    self,
                    Self {
                        fd: fd2,
                        buf: None,
                        event: Arc::new(Event::Nothing),
                        offset,
                        driver,
                    },
                )
            }

            RawFdState::Closed => {
                let offset = self.offset;
                let driver = self.driver.clone();

                (
                    self,
                    Self {
                        fd: RawFdState::Closed,
                        buf: None,
                        event: Arc::new(Event::Nothing),
                        offset,
                        driver,
                    },
                )
            }

            _ => unreachable!(),
        }
    }
}

impl<D: Drive> FileDescriptor<D> {
    pub fn open_with_driver(path: impl AsRef<Path>, driver: D) -> Open<D> {
        let c_path = CString::new(path.as_ref().as_os_str().as_bytes()).unwrap();

        Open::new_read_only(c_path, driver)
    }
}

impl<D: Drive + Unpin + Clone> FileDescriptor<D> {
    pub fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(TcpStream<D>, SocketAddr)>> {
        let this = self.get_mut();

        match &*this.event {
            Event::Accept(accept_event) => {
                let mut accept_event = accept_event.lock();

                match accept_event.result.take() {
                    Some(result) => {
                        let fd = result?;

                        let addr = match socket::getsockname(fd as _).map_err(from_nix_err)? {
                            socket::SockAddr::Inet(inet_addr) => inet_addr.to_std(),
                            addr => {
                                return Poll::Ready(Err(Error::new(
                                    ErrorKind::InvalidData,
                                    format!("invalid addr {:?}", addr),
                                )));
                            }
                        };

                        drop(accept_event);

                        this.cancel();

                        Poll::Ready(Ok((TcpStream::new(fd as _, this.driver.clone()), addr)))
                    }

                    None => {
                        accept_event.waker.register(cx.waker());

                        Poll::Pending
                    }
                }
            }

            _ => {
                this.cancel();

                let fd = this.get_fd()?;

                /*let event = futures_util::ready!(Pin::new(&mut this.driver).poll_prepare(
                    cx,
                    |sqe, _cx| {
                        unsafe {
                            sqe.prep_accept(fd, None, iou::SockFlag::empty());
                        }

                        let accept_event = AcceptEvent::new();

                        accept_event.waker.register(&waker);

                        Arc::new(Event::Accept(Mutex::new(accept_event)))
                    }
                ))?;

                this.event = event;

                futures_util::ready!(Pin::new(&mut this.driver).poll_submit(cx, true))?;

                Poll::Pending*/
                match Pin::new(&mut this.driver).poll_prepare_with_submit(
                    cx,
                    |sqe, cx| {
                        unsafe {
                            sqe.prep_accept(fd, None, iou::SockFlag::empty());
                        }

                        let accept_event = AcceptEvent::new();

                        accept_event.waker.register(cx.waker());

                        Arc::new(Event::Accept(Mutex::new(accept_event)))
                    },
                    Some(false),
                ) {
                    (Poll::Pending, _) => Poll::Pending,
                    (Poll::Ready(result), poll_result) => {
                        this.event = result?;

                        match poll_result {
                            None => {
                                futures_util::ready!(
                                    Pin::new(&mut this.driver).poll_submit(cx, true)
                                )?;

                                Poll::Pending
                            }

                            Some(poll_result) => match poll_result {
                                Poll::Pending => {
                                    futures_util::ready!(
                                        Pin::new(&mut this.driver).poll_submit(cx, true)
                                    )?;

                                    Poll::Pending
                                }

                                Poll::Ready(result) => {
                                    result?;

                                    Poll::Pending
                                }
                            },
                        }
                    }
                }
            }
        }
    }
}

impl<D: Drive + Unpin> AsyncBufRead for FileDescriptor<D> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        const BUFFER_SIZE: usize = 256 * 1024;

        let this = self.get_mut();

        match &*this.event {
            Event::Read(read_event) => {
                let mut read_event = read_event.lock();

                match read_event.result.take() {
                    None => {
                        read_event.waker.register(cx.waker());

                        futures_util::ready!(Pin::new(&mut this.driver).poll_submit(cx, false))?;

                        Poll::Pending
                    }

                    Some(result) => match result {
                        Err(err) => {
                            drop(read_event);

                            this.cancel();

                            Poll::Ready(Err(err))
                        }

                        Ok(result) => {
                            read_event.result.replace(Ok(result));

                            if this.buf.is_none() {
                                this.buf.replace(read_event.buf.take().unwrap());
                            }

                            Poll::Ready(Ok(&this.buf.as_ref().unwrap()[..result]))
                        }
                    },
                }
            }
            _ => {
                this.cancel();

                let mut buf = this
                    .buf
                    .take()
                    .map(|mut buf| {
                        buf.resize(BUFFER_SIZE, 0);

                        buf
                    })
                    .unwrap_or_else(|| Buffer::new(BUFFER_SIZE));

                let offset = this.offset.unwrap_or(0);

                let fd = this.get_fd()?;

                /*let event = futures_util::ready!(Pin::new(&mut this.driver).poll_prepare(
                    cx,
                    |sqe, _cx| {
                        unsafe {
                            sqe.prep_read(fd, buf.as_mut(), offset);
                        }

                        let read_event = ReadEvent::new(buf);
                        read_event.waker.register(&waker);

                        Arc::new(Event::Read(Mutex::new(read_event)))
                    }
                ))?;*/
                match Pin::new(&mut this.driver).poll_prepare_with_submit(
                    cx,
                    |sqe, cx| {
                        unsafe {
                            sqe.prep_read(fd, buf.as_mut(), offset);
                        }

                        let read_event = ReadEvent::new(buf);
                        read_event.waker.register(cx.waker());

                        Arc::new(Event::Read(Mutex::new(read_event)))
                    },
                    Some(false),
                ) {
                    (Poll::Pending, _) => Poll::Pending,
                    (Poll::Ready(result), poll_result) => {
                        this.event = result?;

                        match poll_result {
                            None => {
                                futures_util::ready!(
                                    Pin::new(&mut this.driver).poll_submit(cx, true)
                                )?;

                                Poll::Pending
                            }

                            Some(poll_result) => match poll_result {
                                Poll::Pending => {
                                    futures_util::ready!(
                                        Pin::new(&mut this.driver).poll_submit(cx, true)
                                    )?;

                                    Poll::Pending
                                }

                                Poll::Ready(result) => {
                                    result?;

                                    Poll::Pending
                                }
                            },
                        }
                    }
                }
            }
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();

        if let Event::Read(read_event) = &*this.event {
            let mut read_event = read_event.lock();

            if let Some(Ok(mut result)) = read_event.result.take() {
                debug_assert!(result >= amt);

                result -= amt;

                if let Some(offset) = this.offset.as_mut() {
                    *offset += amt;
                }

                if let Some(buf) = this.buf.as_mut() {
                    buf.advance(amt);
                }

                if result > 0 {
                    read_event.result.replace(Ok(result));
                } else {
                    drop(read_event);

                    this.cancel();
                }
            }
        }
    }
}

impl<D: Drive + Unpin> AsyncRead for FileDescriptor<D> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        let mut inner_buf = futures_util::ready!(self.as_mut().poll_fill_buf(cx))?;

        let n = (&mut inner_buf).read(buf)?;

        self.as_mut().consume(n);

        Poll::Ready(Ok(n))
    }
}

impl<D: Drive + Unpin> AsyncWrite for FileDescriptor<D> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, data: &[u8]) -> Poll<Result<usize>> {
        let this = self.get_mut();

        match &*this.event {
            Event::Write(write_event) => {
                let mut write_event = write_event.lock();

                match write_event.result.take() {
                    None => {
                        write_event.waker.register(cx.waker());

                        futures_util::ready!(Pin::new(&mut this.driver).poll_submit(cx, false))?;

                        Poll::Pending
                    }

                    Some(result) => {
                        drop(write_event);

                        this.cancel();

                        Poll::Ready(result)
                    }
                }
            }

            _ => {
                this.cancel();

                let mut buf = this.buf.take().unwrap_or_else(|| Buffer::new(data.len()));

                buf.write_all(data).expect("fill data to buf failed");

                let fd = this.get_fd()?;

                let offset = this.offset.unwrap_or(0);

                /*let event = futures_util::ready!(Pin::new(&mut this.driver).poll_prepare(
                    cx,
                    |sqe, cx| {
                        unsafe {
                            sqe.prep_write(fd, buf.as_ref(), offset);
                        }

                        let write_event = WriteEvent::new(buf);

                        write_event.waker.register(cx.waker());

                        Arc::new(Event::Write(Mutex::new(write_event)))
                    }
                ))?;

                this.event = event;

                futures_util::ready!(Pin::new(&mut this.driver).poll_submit(cx, false))?;

                Poll::Pending*/

                match Pin::new(&mut this.driver).poll_prepare_with_submit(
                    cx,
                    |sqe, cx| {
                        unsafe {
                            sqe.prep_write(fd, buf.as_ref(), offset);
                        }

                        let write_event = WriteEvent::new(buf);

                        write_event.waker.register(cx.waker());

                        Arc::new(Event::Write(Mutex::new(write_event)))
                    },
                    Some(false),
                ) {
                    (Poll::Pending, _) => Poll::Pending,
                    (Poll::Ready(result), poll_result) => {
                        this.event = result?;

                        match poll_result {
                            None => {
                                futures_util::ready!(
                                    Pin::new(&mut this.driver).poll_submit(cx, true)
                                )?;

                                Poll::Pending
                            }

                            Some(poll_result) => match poll_result {
                                Poll::Pending => {
                                    futures_util::ready!(
                                        Pin::new(&mut this.driver).poll_submit(cx, true)
                                    )?;

                                    Poll::Pending
                                }

                                Poll::Ready(result) => {
                                    result?;

                                    Poll::Pending
                                }
                            },
                        }
                    }
                }
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        futures_util::ready!(Pin::new(&mut self.driver).poll_submit(cx, true))?;

        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();

        this.cancel();

        match &this.fd {
            RawFdState::Closed => Poll::Ready(Ok(())),

            RawFdState::Opened(fd) => {
                let _ = nix::unistd::close(*fd);

                this.fd = RawFdState::Closed;

                Poll::Ready(Ok(()))
            }

            RawFdState::Shared(fd, count) => {
                if count.fetch_sub(1, Ordering::Relaxed) - 1 == 0 {
                    let _ = nix::unistd::close(*fd);
                }

                this.fd = RawFdState::Closed;

                Poll::Ready(Ok(()))
            }
        }
    }
}

impl<D: Drive + Unpin> AsyncSeek for FileDescriptor<D> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<Result<u64>> {
        if let Some(offset) = self.offset.as_mut() {
            match pos {
                SeekFrom::Start(pos) => {
                    *offset = pos as _;
                }

                SeekFrom::End(_) => todo!(),

                SeekFrom::Current(pos) => {
                    if pos < 0 {
                        *offset -= -pos as usize;
                    } else {
                        *offset += pos as usize;
                    }
                }
            }

            Poll::Ready(Ok(*offset as _))
        } else {
            Poll::Ready(Err(Error::from_raw_os_error(libc::ESPIPE)))
        }
    }
}

impl<D: Drive + Unpin> FileDescriptor<D> {
    pub fn poll_recv_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        const BUFFER_SIZE: usize = 12 * 1024;

        let this = self.get_mut();

        match &*this.event {
            Event::Recv(recv_event) => {
                let mut recv_event = recv_event.lock();

                match recv_event.result.take() {
                    None => {
                        recv_event.waker.register(cx.waker());

                        futures_util::ready!(Pin::new(&mut this.driver).poll_submit(cx, false))?;

                        Poll::Pending
                    }

                    Some(result) => match result {
                        Err(err) => {
                            drop(recv_event);

                            this.cancel();

                            Poll::Ready(Err(err))
                        }

                        Ok(result) => {
                            recv_event.result.replace(Ok(result));

                            if this.buf.is_none() {
                                this.buf.replace(recv_event.buf.take().unwrap());
                            }

                            Poll::Ready(Ok(&this.buf.as_ref().unwrap()[..result]))
                        }
                    },
                }
            }
            _ => {
                this.cancel();

                let mut buf = this
                    .buf
                    .take()
                    .map(|mut buf| {
                        buf.resize(BUFFER_SIZE, 0);

                        buf
                    })
                    .unwrap_or_else(|| Buffer::new(BUFFER_SIZE));

                let fd = this.get_fd()?;

                /*let event = futures_util::ready!(Pin::new(&mut this.driver).poll_prepare(
                    cx,
                    |sqe, _cx| {
                        unsafe {
                            uring_sys::io_uring_prep_recv(
                                sqe.raw_mut(),
                                fd,
                                buf.as_mut_ptr() as *mut libc::c_void,
                                buf.len(),
                                0,
                            );
                        }

                        let recv_event = RecvEvent::new(buf);
                        recv_event.waker.register(&waker);

                        Arc::new(Event::Recv(Mutex::new(recv_event)))
                    }
                ))?;

                this.event = event;

                futures_util::ready!(Pin::new(&mut this.driver).poll_submit(cx, true))?;

                Poll::Pending*/
                match Pin::new(&mut this.driver).poll_prepare_with_submit(
                    cx,
                    |sqe, cx| {
                        unsafe {
                            uring_sys::io_uring_prep_recv(
                                sqe.raw_mut(),
                                fd,
                                buf.as_mut_ptr() as *mut libc::c_void,
                                buf.len(),
                                0,
                            );
                        }

                        let recv_event = RecvEvent::new(buf);
                        recv_event.waker.register(cx.waker());

                        Arc::new(Event::Recv(Mutex::new(recv_event)))
                    },
                    Some(false),
                ) {
                    (Poll::Pending, _) => Poll::Pending,
                    (Poll::Ready(result), poll_result) => {
                        this.event = result?;

                        match poll_result {
                            None => {
                                futures_util::ready!(
                                    Pin::new(&mut this.driver).poll_submit(cx, true)
                                )?;

                                Poll::Pending
                            }

                            Some(poll_result) => match poll_result {
                                Poll::Pending => {
                                    futures_util::ready!(
                                        Pin::new(&mut this.driver).poll_submit(cx, true)
                                    )?;

                                    Poll::Pending
                                }

                                Poll::Ready(result) => {
                                    result?;

                                    Poll::Pending
                                }
                            },
                        }
                    }
                }
            }
        }
    }

    pub fn recv_consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();

        if let Event::Recv(recv_event) = &*this.event {
            let mut recv_event = recv_event.lock();

            if let Some(Ok(mut result)) = recv_event.result.take() {
                assert!(result >= amt);

                result -= amt;

                if let Some(offset) = this.offset.as_mut() {
                    *offset += amt;
                }

                if let Some(buf) = this.buf.as_mut() {
                    buf.advance(amt);
                }

                if result > 0 {
                    recv_event.result.replace(Ok(result));
                } else {
                    drop(recv_event);

                    this.cancel();
                }
            }
        }
    }

    pub fn poll_recv(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        let mut inner_buf = futures_util::ready!(self.as_mut().poll_recv_fill_buf(cx))?;

        let n = (&mut inner_buf).read(buf)?;

        self.as_mut().recv_consume(n);

        Poll::Ready(Ok(n))
    }

    pub fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<Result<usize>> {
        let this = self.get_mut();

        match &*this.event {
            Event::Send(send_event) => {
                let mut send_event = send_event.lock();

                match send_event.result.take() {
                    None => {
                        send_event.waker.register(cx.waker());

                        futures_util::ready!(Pin::new(&mut this.driver).poll_submit(cx, false))?;

                        Poll::Pending
                    }

                    Some(result) => {
                        drop(send_event);

                        this.cancel();

                        Poll::Ready(result)
                    }
                }
            }

            _ => {
                this.cancel();

                let mut buf = this.buf.take().unwrap_or_else(|| Buffer::new(data.len()));

                buf.write_all(data).expect("fill data to buf failed");

                let fd = this.get_fd()?;

                /*let event = futures_util::ready!(Pin::new(&mut this.driver).poll_prepare(
                    cx,
                    |sqe, cx| {
                        unsafe {
                            uring_sys::io_uring_prep_send(
                                sqe.raw_mut(),
                                fd,
                                buf.as_ptr() as *const libc::c_void,
                                buf.len(),
                                0,
                            );
                        }

                        let send_event = SendEvent::new(buf);

                        send_event.waker.register(cx.waker());

                        Arc::new(Event::Send(Mutex::new(send_event)))
                    }
                ))?;

                this.event = event;

                futures_util::ready!(Pin::new(&mut this.driver).poll_submit(cx, false))?;

                Poll::Pending*/
                match Pin::new(&mut this.driver).poll_prepare_with_submit(
                    cx,
                    |sqe, cx| {
                        unsafe {
                            uring_sys::io_uring_prep_send(
                                sqe.raw_mut(),
                                fd,
                                buf.as_ptr() as *const libc::c_void,
                                buf.len(),
                                0,
                            );
                        }

                        let send_event = SendEvent::new(buf);

                        send_event.waker.register(cx.waker());

                        Arc::new(Event::Send(Mutex::new(send_event)))
                    },
                    Some(false),
                ) {
                    (Poll::Pending, _) => Poll::Pending,
                    (Poll::Ready(result), poll_result) => {
                        this.event = result?;

                        match poll_result {
                            None => {
                                futures_util::ready!(
                                    Pin::new(&mut this.driver).poll_submit(cx, true)
                                )?;

                                Poll::Pending
                            }

                            Some(poll_result) => match poll_result {
                                Poll::Pending => {
                                    futures_util::ready!(
                                        Pin::new(&mut this.driver).poll_submit(cx, true)
                                    )?;

                                    Poll::Pending
                                }

                                Poll::Ready(result) => {
                                    result?;

                                    Poll::Pending
                                }
                            },
                        }
                    }
                }
            }
        }
    }
}

impl<D> AsRawFd for FileDescriptor<D> {
    fn as_raw_fd(&self) -> RawFd {
        match &self.fd {
            RawFdState::Opened(fd) => *fd,
            RawFdState::Closed => -1,
            RawFdState::Shared(..) => unreachable!(),
        }
    }
}

impl<D> IntoRawFd for FileDescriptor<D> {
    fn into_raw_fd(mut self) -> RawFd {
        self.cancel();

        match self.fd {
            RawFdState::Opened(fd) => fd,
            RawFdState::Closed => -1,
            RawFdState::Shared(..) => unreachable!(),
        }
    }
}

impl<D> Drop for FileDescriptor<D> {
    fn drop(&mut self) {
        self.cancel();

        match &self.fd {
            RawFdState::Opened(fd) => {
                let _ = nix::unistd::close(*fd);
            }
            RawFdState::Closed => {}
            RawFdState::Shared(fd, count) => {
                if count.fetch_sub(1, Ordering::Relaxed) - 1 == 0 {
                    let _ = nix::unistd::close(*fd);
                }
            }
        }
    }
}
