use std::ffi::CString;
use std::io::Error;
use std::io::SeekFrom;
use std::io::{Read, Result, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::IntoRawFd;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Buf;
use futures_io::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWrite};

use crate::drive::{Drive, Event, ReadEvent, WriteEvent};
use crate::file_descriptor::buffer::Buffer;
use crate::fs::Open;

pub(crate) mod buffer;

pub struct FileDescriptor<D> {
    fd: Option<RawFd>,
    buf: Option<Buffer>,
    event: Arc<Event>,
    offset: Option<usize>,
    driver: D,
}

impl<D> FileDescriptor<D> {
    pub(crate) fn new(fd: RawFd, driver: D, offset: impl Into<Option<usize>>) -> Self {
        Self {
            fd: Some(fd),
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
}

impl<D: Drive> FileDescriptor<D> {
    pub fn open_with_driver(path: impl AsRef<Path>, driver: D) -> Open<D> {
        let c_path = CString::new(path.as_ref().as_os_str().as_bytes()).unwrap();

        Open::new_read_only(c_path, driver)
    }
}

impl<D: Drive + Unpin> AsyncBufRead for FileDescriptor<D> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        const BUFFER_SIZE: usize = 12 * 1024;

        let this = self.get_mut();

        match &*this.event {
            Event::Read(read_event) => {
                let mut read_event = read_event.lock().unwrap();

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
                let fd = this.fd.unwrap();
                let waker = cx.waker().clone();

                let event = futures_util::ready!(Pin::new(&mut this.driver).poll_prepare(
                    cx,
                    |sqe, _cx| {
                        unsafe {
                            sqe.prep_read(fd, buf.as_mut(), offset);
                        }

                        let read_event = ReadEvent::new(buf);
                        read_event.waker.register(&waker);

                        Arc::new(Event::Read(Mutex::new(read_event)))
                    }
                ))?;

                this.event = event;

                futures_util::ready!(Pin::new(&mut this.driver).poll_submit(cx, true))?;

                Poll::Pending
            }
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();

        if let Event::Read(read_event) = &*this.event {
            let mut read_event = read_event.lock().unwrap();

            if let Some(Ok(mut result)) = read_event.result.take() {
                assert!(result >= amt);

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

                    this.event = Arc::new(Event::Nothing);
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
                let mut write_event = write_event.lock().unwrap();

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

                let fd = this.fd.unwrap();
                let offset = this.offset.unwrap_or(0);

                let event = futures_util::ready!(Pin::new(&mut this.driver).poll_prepare(
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

                futures_util::ready!(Pin::new(&mut this.driver).poll_submit(cx, true))?;

                Poll::Pending
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

        let _ = nix::unistd::close(this.fd.take().unwrap());

        Poll::Ready(Ok(()))
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

impl<D> AsRawFd for FileDescriptor<D> {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.unwrap()
    }
}

impl<D> IntoRawFd for FileDescriptor<D> {
    fn into_raw_fd(mut self) -> RawFd {
        self.cancel();

        self.fd.take().unwrap()
    }
}

impl<D> Drop for FileDescriptor<D> {
    fn drop(&mut self) {
        self.cancel();

        if let Some(fd) = self.fd.take() {
            let _ = nix::unistd::close(fd);
        }
    }
}
