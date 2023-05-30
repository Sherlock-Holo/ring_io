use std::io;
use std::os::fd::{IntoRawFd, RawFd};
use std::path::Path;

use super::OpenOptions;
use crate::buf::{IoBuf, IoBufMut};
use crate::fd_trait;
use crate::io::WriteAll;
use crate::op::Op;
use crate::opcode::{Close, Read, Write};
use crate::runtime::{in_ring_io_context, spawn};

#[derive(Debug)]
pub struct File {
    fd: RawFd,
}

impl File {
    pub async fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path).await
    }

    pub async fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
    }

    pub async fn create_new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .truncate(true)
            .open(path)
            .await
    }

    pub fn from_std(file: std::fs::File) -> Self {
        Self {
            fd: file.into_raw_fd(),
        }
    }

    pub fn read<B: IoBufMut>(&self, buf: B) -> Op<Read<B>> {
        Read::new(self.fd, buf, u64::MAX)
    }

    pub fn write<B: IoBuf>(&self, buf: B) -> Op<Write<B>> {
        Write::new(self.fd, buf, u64::MAX)
    }

    pub fn write_all<B: IoBuf>(&self, buf: B) -> WriteAll<B, Self> {
        WriteAll::new(self, buf)
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

fd_trait!(File);
