use std::marker::PhantomData;
use std::os::fd::AsRawFd;

use crate::buf::{IoBuf, IoBufMut};
use crate::op::Op;
use crate::opcode;
use crate::opcode::{Read, Recv, Write};

pub struct Seekable;

pub struct NotSeekable;

pub struct RingFd<T: AsRawFd, S> {
    fd: T,
    _phantom_data: PhantomData<S>,
}

impl<T: AsRawFd> RingFd<T, Seekable> {
    pub fn new_seekable(fd: T) -> Self {
        Self {
            fd,
            _phantom_data: PhantomData,
        }
    }

    pub fn read<B: IoBufMut>(&self, buf: B) -> Op<Read<B>> {
        Read::new(self.fd.as_raw_fd(), buf, u64::MAX)
    }

    pub fn write<B: IoBuf>(&self, buf: B) -> Op<Write<B>> {
        Write::new(self.fd.as_raw_fd(), buf, u64::MAX)
    }
}

impl<T: AsRawFd> RingFd<T, NotSeekable> {
    pub fn new_not_seekable(fd: T) -> Self {
        Self {
            fd,
            _phantom_data: PhantomData,
        }
    }

    pub fn read<B: IoBufMut>(&self, buf: B) -> Op<Read<B>> {
        Read::new(self.fd.as_raw_fd(), buf, 0)
    }

    pub fn write<B: IoBuf>(&self, buf: B) -> Op<Write<B>> {
        Write::new(self.fd.as_raw_fd(), buf, 0)
    }

    pub fn send<B: IoBuf>(&self, buf: B) -> Op<opcode::Send<B>> {
        opcode::Send::new(self.fd.as_raw_fd(), buf)
    }

    pub fn recv<B: IoBufMut>(&self, buf: B) -> Op<Recv<B>> {
        Recv::new(self.fd.as_raw_fd(), buf)
    }
}
