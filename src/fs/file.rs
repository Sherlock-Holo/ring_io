use std::ffi::CString;
use std::io::SeekFrom;
use std::io::{Read, Result, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Buf;
use futures_io::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWrite};

use crate::drive::{DemoDriver, Drive};
use crate::event::{Event, ReadEvent, WriteEvent};
use crate::fs::open::Open;

use super::buffer::Buffer;

pub struct File<D: Drive = DemoDriver> {
    fd: RawFd,
    buf: Option<Buffer>,
    event: Arc<Event>,
    offset: usize,
    driver: D,
}

impl<D: Drive> File<D> {
    pub(crate) fn new(fd: RawFd, driver: D) -> Self {
        Self {
            fd,
            buf: None,
            event: Arc::new(Event::Nothing),
            offset: 0,
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

impl File<DemoDriver> {
    pub fn open(path: impl AsRef<Path>) -> Open<DemoDriver> {
        let c_path = CString::new(path.as_ref().as_os_str().as_bytes()).unwrap();

        Open::new_read_only(c_path, DemoDriver::default())
    }
}

impl<D: Drive + Unpin> AsyncBufRead for File<D> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
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
                            if result == 0 {
                                drop(read_event);

                                this.cancel();

                                return Pin::new(this).poll_fill_buf(cx);
                            }

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
                        buf.resize(4096, 0);

                        buf
                    })
                    .unwrap_or_else(|| Buffer::new(4096));

                let offset = this.offset;
                let fd = this.fd;
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

                this.offset += amt;

                if let Some(buf) = this.buf.as_mut() {
                    buf.advance(amt);
                }

                read_event.result.replace(Ok(result));
            }
        }
    }
}

impl<D: Drive + Unpin> AsyncRead for File<D> {
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

impl<D: Drive + Unpin> AsyncWrite for File<D> {
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

                let fd = this.fd;
                let offset = this.offset;

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

        unsafe {
            libc::close(this.fd);
        }

        Poll::Ready(Ok(()))
    }
}

impl<D: Drive + Unpin> AsyncSeek for File<D> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<Result<u64>> {
        match pos {
            SeekFrom::Start(pos) => {
                self.offset = pos as _;
            }

            SeekFrom::End(_) => todo!(),

            SeekFrom::Current(pos) => {
                if pos < 0 {
                    self.offset -= -pos as usize;
                } else {
                    self.offset += pos as usize;
                }
            }
        }

        Poll::Ready(Ok(self.offset as u64))
    }
}

impl<D: Drive> Drop for File<D> {
    fn drop(&mut self) {
        self.cancel();

        unsafe {
            libc::close(self.fd);
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    use crate::fs::OpenOptions;

    use super::*;

    #[test]
    fn test_file_open() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();

        let path = temp_file.path();

        futures_executor::block_on(async {
            let _file = File::open(path).await.unwrap();
        })
    }

    #[test]
    fn test_file_read() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();

        temp_file.as_file_mut().write_all(b"test").unwrap();
        temp_file.as_file_mut().flush().unwrap();

        let path = temp_file.path();

        futures_executor::block_on(async {
            let mut file = File::open(path).await.unwrap();

            let mut buf = vec![0; 4];

            file.read_exact(&mut buf).await.unwrap();

            assert_eq!(b"test".as_ref(), buf.as_slice());
        })
    }

    #[test]
    fn test_file_write() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();

        let path = temp_file.path();

        futures_executor::block_on(async {
            let mut file = OpenOptions::new().write(true).open(path).await.unwrap();

            file.write_all(b"test").await.unwrap();
            file.flush().await.unwrap();
        });

        let mut buf = vec![0; 4];

        temp_file.read_exact(&mut buf).unwrap();

        assert_eq!(b"test".as_ref(), buf.as_slice());
    }

    #[test]
    fn test_file_write_read() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();

        let path = temp_file.path();

        futures_executor::block_on(async {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .await
                .unwrap();

            file.write_all(b"test").await.unwrap();
            file.flush().await.unwrap();

            assert_eq!(file.seek(SeekFrom::Start(0)).await.unwrap(), 0);

            let mut buf = vec![0; 4];

            file.read_exact(&mut buf).await.unwrap();

            assert_eq!(b"test".as_ref(), buf.as_slice());
        })
    }
}
