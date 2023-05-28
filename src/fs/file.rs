use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::path::Path;

use super::OpenOptions;
use crate::buf::IoBufMut;
use crate::op::Op;
use crate::opcode::{Close, Read};
use crate::runtime::{in_ring_io_context, spawn};

pub struct File {
    fd: RawFd,
}

impl File {
    pub async fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path).await
    }

    pub fn read<B: IoBufMut>(&self, buf: B) -> Op<Read<B>> {
        Read::new(self.fd, buf, u64::MAX)
    }

    pub fn close(&mut self) -> Op<Close> {
        let fd = self.fd;
        self.fd = -1;

        Close::new(fd)
    }
}

impl Drop for File {
    fn drop(&mut self) {
        if self.fd < 0 {
            return;
        }

        if in_ring_io_context() {
            spawn(Close::new(self.fd)).detach()
        } else {
            unsafe {
                libc::close(self.fd);
            }
        }
    }
}

impl FromRawFd for File {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self { fd }
    }
}

impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl IntoRawFd for File {
    fn into_raw_fd(mut self) -> RawFd {
        let fd = self.fd;

        // make sure don't close fd
        self.fd = -1;

        fd
    }
}

impl From<File> for OwnedFd {
    fn from(value: File) -> Self {
        // Safety: fd is valid
        unsafe { OwnedFd::from_raw_fd(value.into_raw_fd()) }
    }
}

impl AsFd for File {
    fn as_fd(&self) -> BorrowedFd<'_> {
        // Safety: fd is valid
        unsafe { BorrowedFd::borrow_raw(self.fd) }
    }
}
