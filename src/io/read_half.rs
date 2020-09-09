use std::io::Result;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_io::{AsyncBufRead, AsyncRead};

use crate::drive::Drive;
use crate::io::FileDescriptor;

pub struct ReadHalf<D, T> {
    pub(crate) fd: FileDescriptor<D>,
    _marker: PhantomData<T>,
}

impl<D, T> ReadHalf<D, T> {
    pub(crate) fn new(fd: FileDescriptor<D>) -> Self {
        Self {
            fd,
            _marker: Default::default(),
        }
    }
}

impl<D: Drive + Unpin, T: Unpin> AsyncRead for ReadHalf<D, T> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.fd).poll_read(cx, buf)
    }
}

impl<D: Drive + Unpin, T: Unpin> AsyncBufRead for ReadHalf<D, T> {
    #[inline]
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        Pin::new(&mut self.get_mut().fd).poll_fill_buf(cx)
    }

    #[inline]
    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.fd).consume(amt)
    }
}
