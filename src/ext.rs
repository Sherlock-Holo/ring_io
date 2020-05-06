use std::os::unix::io::AsRawFd;

use crate::{ReadExactFuture, ReadFuture, WriteAllFuture, WriteFuture};

pub trait AsyncRead: AsRawFd {
    fn read<'a>(&'a self, buf: &'a mut [u8]) -> ReadFuture<'a>;

    fn read_exact<'a>(&'a self, buf: &'a mut [u8]) -> ReadExactFuture<'a>;
}

impl<F: AsRawFd> AsyncRead for F {
    fn read<'a>(&'a self, buf: &'a mut [u8]) -> ReadFuture<'a> {
        ReadFuture::new(self.as_raw_fd(), buf)
    }

    fn read_exact<'a>(&'a self, buf: &'a mut [u8]) -> ReadExactFuture<'a> {
        ReadExactFuture::new(self.as_raw_fd(), buf)
    }
}

pub trait AsyncWrite: AsRawFd {
    fn write<'a>(&'a self, buf: &'a [u8]) -> WriteFuture<'a>;

    fn write_all<'a>(&'a self, buf: &'a [u8]) -> WriteAllFuture<'a>;
}

impl<F: AsRawFd> AsyncWrite for F {
    fn write<'a>(&'a self, buf: &'a [u8]) -> WriteFuture<'a> {
        WriteFuture::new(self.as_raw_fd(), buf)
    }

    fn write_all<'a>(&'a self, buf: &'a [u8]) -> WriteAllFuture<'a> {
        WriteAllFuture::new(self.as_raw_fd(), buf)
    }
}
