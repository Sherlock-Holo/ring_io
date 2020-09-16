use std::io::{Result, Write};
use std::ops::Deref;
use std::ops::DerefMut;

use bytes::{Buf, BytesMut};

#[derive(Debug, Clone)]
pub struct Buffer {
    offset: usize,
    buf: BytesMut,
}

impl Buffer {
    pub fn new(size: usize) -> Self {
        let mut buf = BytesMut::with_capacity(size);

        // buf.resize(size, 0);
        unsafe { buf.set_len(size) }

        Self { offset: 0, buf }
    }

    pub fn len(&self) -> usize {
        self.buf.len() - self.offset
    }

    pub fn reset(&mut self) {
        self.offset = 0;

        self.buf.clear();

        // self.buf.resize(self.buf.capacity(), 0);
        unsafe { self.buf.set_len(self.buf.capacity()) }
    }

    pub fn resize(&mut self, mut new_len: usize, value: u8) {
        new_len += self.offset;

        self.buf.resize(new_len, value);
    }
}

impl Buf for Buffer {
    fn remaining(&self) -> usize {
        self.len()
    }

    fn bytes(&self) -> &[u8] {
        self.as_ref()
    }

    fn advance(&mut self, cnt: usize) {
        if self.offset + cnt <= self.buf.len() {
            self.offset += cnt;
        } else {
            self.offset = self.buf.len();
        }
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        &self.buf.as_ref()[self.offset..]
    }
}

impl AsMut<[u8]> for Buffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buf.as_mut()[self.offset..]
    }
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

impl Write for Buffer {
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        self.offset = 0;

        // self.buf.resize(data.len(), 0);
        if self.buf.len() > data.len() {
            unsafe { self.buf.set_len(data.len()) }
        } else {
            self.buf.resize(data.len(), 0);
        }

        self.buf.copy_from_slice(data);

        Ok(data.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
