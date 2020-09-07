use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_io::AsyncRead;
use futures_util::lock::BiLock;

use crate::drive::Drive;
use crate::io::FileDescriptor;

pub struct ReadHalf<'a, D> {
    fd: BiLock<&'a mut FileDescriptor<D>>,
}

impl<D: Drive + Unpin> AsyncRead for ReadHalf<'_, D> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        futures_util::ready!(self.fd.poll_lock(cx))
            .as_pin_mut()
            .poll_read(cx, buf)
    }
}

pub struct OwnedReadHalf<D> {
    fd: BiLock<FileDescriptor<D>>,
}

impl<D: Drive + Unpin> AsyncRead for OwnedReadHalf<D> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        futures_util::ready!(self.fd.poll_lock(cx))
            .as_pin_mut()
            .poll_read(cx, buf)
    }
}
