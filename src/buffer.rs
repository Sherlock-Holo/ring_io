use std::collections::HashMap;
use std::io::Result;
use std::ops::{Deref, DerefMut};

use io_uring::opcode::ProvideBuffers;
use slab::Slab;

use crate::driver::DRIVER;

#[derive(Debug)]
pub struct Buffer {
    group_id: u16,
    buffer_id: u16,
    buffer: Vec<u8>,
    available_len: usize,
}

impl Buffer {
    pub fn new(group_id: u16, buffer_id: u16) -> Self {
        Self {
            group_id,
            buffer_id,
            buffer: vec![0; group_id as _],
            available_len: 0,
        }
    }

    pub fn set_available_len(&mut self, available_len: usize) {
        self.available_len = available_len;
    }

    pub fn consume(&mut self, amt: usize) {
        self.available_len -= amt;
    }

    pub fn group_id(&self) -> u16 {
        self.group_id
    }
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer[..self.available_len]
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer[..self.available_len]
    }
}

#[derive(Debug)]
pub struct GroupBuffer {
    group_id: u16,
    buffer_id_gen: Slab<()>,
    available_buffers: HashMap<u16, Buffer>,
    allocated_buffers: usize,
    capacity_buffers: usize,
}

impl GroupBuffer {
    /// create a GroupBuffer and pre-allocate a buffer
    pub fn new(buf_size: u16, cap_buf: usize) -> Self {
        assert!(cap_buf > 0);

        let mut slab = Slab::new();
        let buffer_id = slab.insert(()) as u16;

        let buffer = Buffer::new(buf_size, buffer_id);

        let mut available_buffers = HashMap::new();
        available_buffers.insert(buffer_id, buffer);

        Self {
            group_id: buf_size,
            buffer_id_gen: Default::default(),
            available_buffers,
            allocated_buffers: 1,
            capacity_buffers: cap_buf,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.available_buffers.is_empty()
    }

    /// if allocated == capacity, return false
    pub fn allocate(&mut self) -> bool {
        if self.allocated_buffers == self.capacity_buffers {
            false
        } else {
            let new_buffer_id = self.buffer_id_gen.insert(()) as u16;
            let new_buffer = Buffer::new(self.group_id, new_buffer_id);

            self.available_buffers.insert(new_buffer_id, new_buffer);

            self.allocated_buffers += 1;

            true
        }
    }

    /// select a buffer by buffer_id and remove it from GroupBuffer
    pub fn select_buffer(&mut self, buffer_id: u16) -> Option<Buffer> {
        self.available_buffers.remove(&buffer_id)
    }

    pub fn group_id(&self) -> u16 {
        self.group_id
    }

    pub fn register_buffer(&mut self, mut buffer: Buffer) -> Result<()> {
        let provide_buffer_sqe = ProvideBuffers::new(
            buffer.buffer.as_mut_ptr(),
            buffer.buffer.len() as _,
            1,
            self.group_id,
            buffer.buffer_id,
        )
        .build();

        DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.register_provide_buffer(provide_buffer_sqe)
        })
    }
}
