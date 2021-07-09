use std::cell::UnsafeCell;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::{IoSlice, IoSliceMut};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};
pub use futures_util::io::{copy, copy_buf};

use crate::io::ring_fd::RingFd;

pub mod ring_fd;

pub struct ReadHalf<'a> {
    ring_fd: &'a mut RingFd,
}

impl<'a> ReadHalf<'a> {
    pub(crate) fn new(ring_fd: &'a mut RingFd) -> Self {
        Self { ring_fd }
    }
}

impl<'a> AsyncRead for ReadHalf<'a> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().ring_fd).poll_read(cx, buf)
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().ring_fd).poll_read_vectored(cx, bufs)
    }
}

impl<'a> AsyncBufRead for ReadHalf<'a> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<&[u8]>> {
        Pin::new(&mut self.get_mut().ring_fd).poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.get_mut().ring_fd).consume(amt)
    }
}

pub struct WriteHalf<'a> {
    ring_fd: &'a mut RingFd,
}

impl<'a> WriteHalf<'a> {
    pub(crate) fn new(ring_fd: &'a mut RingFd) -> Self {
        Self { ring_fd }
    }
}

impl<'a> AsyncWrite for WriteHalf<'a> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().ring_fd).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().ring_fd).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().ring_fd).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().ring_fd).poll_close(cx)
    }
}

fn reunite<T: sealed::FromRingFd>(
    read: OwnedReadHalf<T>,
    write: OwnedWriteHalf<T>,
) -> Result<T, ReuniteError<T>> {
    if !Arc::ptr_eq(&read.ring_fd, &write.ring_fd) {
        Err(ReuniteError(read, write))
    } else {
        drop(write);

        let ring_fd = Arc::try_unwrap(read.ring_fd)
            .expect("try_unwrap failed in reunite")
            .into_inner();

        // Safety: Arc make sure the ring_fd is the same one
        unsafe { Ok(T::from_ring_fd(ring_fd)) }
    }
}

pub struct OwnedReadHalf<T> {
    ring_fd: Arc<UnsafeCell<RingFd>>,
    _phantom_data: PhantomData<T>,
}

impl<T> OwnedReadHalf<T> {
    pub(crate) fn new(ring_fd: Arc<UnsafeCell<RingFd>>) -> Self {
        Self {
            ring_fd,
            _phantom_data: Default::default(),
        }
    }
}

impl<T: sealed::FromRingFd> OwnedReadHalf<T> {
    pub fn reunite(self, other: OwnedWriteHalf<T>) -> Result<T, ReuniteError<T>> {
        reunite(self, other)
    }
}

unsafe impl<T> Send for OwnedReadHalf<T> {}

unsafe impl<T> Sync for OwnedReadHalf<T> {}

impl<T> AsyncRead for OwnedReadHalf<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let ring_fd = self.ring_fd.get();

        // Safety: only 1 OwnedReadHalf exists, and no other can do the read operation
        let ring_fd = unsafe { &mut *ring_fd };

        Pin::new(ring_fd).poll_read(cx, buf)
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let ring_fd = self.ring_fd.get();

        // Safety: only 1 OwnedReadHalf exists, and no other can do the read operation
        let ring_fd = unsafe { &mut *ring_fd };

        Pin::new(ring_fd).poll_read_vectored(cx, bufs)
    }
}

impl<T> AsyncBufRead for OwnedReadHalf<T> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<&[u8]>> {
        let ring_fd = self.ring_fd.get();

        // Safety: only 1 OwnedReadHalf exists, and no other can do the read operation
        let ring_fd = unsafe { &mut *ring_fd };

        Pin::new(ring_fd).poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let ring_fd = self.ring_fd.get();

        // Safety: only 1 OwnedReadHalf exists, and no other can do the read operation
        let ring_fd = unsafe { &mut *ring_fd };

        Pin::new(ring_fd).consume(amt)
    }
}

pub struct OwnedWriteHalf<T> {
    ring_fd: Arc<UnsafeCell<RingFd>>,
    _phantom_data: PhantomData<T>,
}

impl<T> OwnedWriteHalf<T> {
    pub(crate) fn new(ring_fd: Arc<UnsafeCell<RingFd>>) -> Self {
        Self {
            ring_fd,
            _phantom_data: Default::default(),
        }
    }
}

impl<T: sealed::FromRingFd> OwnedWriteHalf<T> {
    pub fn reunite(self, other: OwnedReadHalf<T>) -> Result<T, ReuniteError<T>> {
        reunite(other, self)
    }
}

unsafe impl<T> Send for OwnedWriteHalf<T> {}

unsafe impl<T> Sync for OwnedWriteHalf<T> {}

impl<T> AsyncWrite for OwnedWriteHalf<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let ring_fd = self.ring_fd.get();

        // Safety: only 1 OwnedWriteHalf exists, and no other can do the write operation
        let ring_fd = unsafe { &mut *ring_fd };

        Pin::new(ring_fd).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let ring_fd = self.ring_fd.get();

        // Safety: only 1 OwnedWriteHalf exists, and no other can do the write operation
        let ring_fd = unsafe { &mut *ring_fd };

        Pin::new(ring_fd).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let ring_fd = self.ring_fd.get();

        // Safety: only 1 OwnedWriteHalf exists, and no other can do the write operation
        let ring_fd = unsafe { &mut *ring_fd };

        Pin::new(ring_fd).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let ring_fd = self.ring_fd.get();

        // Safety: only 1 OwnedWriteHalf exists, and no other can do the close operation
        let ring_fd = unsafe { &mut *ring_fd };

        Pin::new(ring_fd).poll_close(cx)
    }
}

mod sealed {
    use crate::io::ring_fd::RingFd;
    use crate::net::tcp::TcpStream;

    pub trait FromRingFd: Sized {
        unsafe fn from_ring_fd(ring_fd: RingFd) -> Self;
    }

    impl FromRingFd for TcpStream {
        unsafe fn from_ring_fd(ring_fd: RingFd) -> Self {
            Self::new(ring_fd)
        }
    }
}

pub struct ReuniteError<T>(OwnedReadHalf<T>, OwnedWriteHalf<T>);

impl<T> Debug for ReuniteError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReuniteError").finish()
    }
}

impl<T> Display for ReuniteError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl<T> Error for ReuniteError<T> {}
