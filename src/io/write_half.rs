use std::io::Result;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_io::AsyncWrite;

use crate::drive::Drive;
use crate::io::FileDescriptor;

pub struct WriteHalf<D, T> {
    pub(crate) fd: FileDescriptor<D>,
    _marker: PhantomData<T>,
}

impl<D, T> WriteHalf<D, T> {
    pub(crate) fn new(fd: FileDescriptor<D>) -> Self {
        Self {
            fd,
            _marker: Default::default(),
        }
    }
}

impl<D: Drive + Unpin, T: Unpin> AsyncWrite for WriteHalf<D, T> {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.fd).poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.fd).poll_flush(cx)
    }

    #[inline]
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.fd).poll_close(cx)
    }
}
