use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::{fmt, mem, ptr};

use crate::driver::Driver;

const EVERY_GROUP_BUFFER_BUFFER_COUNT: usize = 10;

type GroupIdGenerator = u16;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum GroupBufferRegisterState {
    NotRegister,
    Registering,
    Registered,
}

#[derive(Debug)]
pub struct BufferManager {
    group_id_gen: GroupIdGenerator,
    /// key: buffer len
    group_buffers: HashMap<usize, GroupBuffers>,
}

impl BufferManager {
    pub(crate) fn new() -> Self {
        Self {
            group_id_gen: 1,
            group_buffers: Default::default(),
        }
    }

    pub(crate) fn select_group_buffer(&mut self, want_buffer_size: usize) -> &mut GroupBuffer {
        let group_id_gen = &mut self.group_id_gen;

        self.group_buffers
            .entry(want_buffer_size)
            .or_insert_with(|| GroupBuffers::new(want_buffer_size))
            .select_group_buffer(group_id_gen)
    }

    /// take a buffer by (buffer_size, group_id, buffer_id, buffer_data_size), if any argument not
    /// valid, will panic
    pub(crate) fn take_buffer(
        &mut self,
        buffer_size: usize,
        group_id: u16,
        buffer_id: u16,
        buffer_data_size: usize,
        driver: Driver,
    ) -> Buffer {
        let group_buffers = self
            .group_buffers
            .get_mut(&buffer_size)
            .unwrap_or_else(|| panic!("buffer size {} not exist", buffer_size));

        assert_eq!(group_buffers.buffer_size, buffer_size);

        let group_buffer = group_buffers
            .group_buffers
            .get_mut(&group_id)
            .unwrap_or_else(|| panic!("group id {} not found", group_id));
        assert_eq!(
            group_buffer.register_state,
            GroupBufferRegisterState::Registered
        );

        assert!(group_buffer.every_buf_size >= buffer_data_size);

        // Safety: all argument is checked by assert
        unsafe { group_buffer.take_buffer(buffer_id, buffer_data_size, driver) }
    }

    pub(crate) fn give_back_buffer(&mut self, buffer: Buffer) {
        let buf_len = buffer.buffer_size();

        let group_buffers = self
            .group_buffers
            .get_mut(&buf_len)
            .unwrap_or_else(|| panic!("buffer len {} group buffers not exist", buf_len));

        group_buffers.give_back_buffer(buffer);
    }

    pub(crate) fn decrease_on_fly_for_not_use_group_buffer(
        &mut self,
        buffer_size: usize,
        group_id: u16,
    ) {
        let group_buffers = self
            .group_buffers
            .get_mut(&buffer_size)
            .unwrap_or_else(|| panic!("buffer size {} not exist", buffer_size));

        let group_buffer = group_buffers
            .group_buffers
            .get_mut(&group_id)
            .unwrap_or_else(|| panic!("group id {} not exist", group_id));

        group_buffer.decrease_on_fly();
    }

    pub(crate) fn group_buffer_mut(&mut self, group_id: u16) -> Option<&mut GroupBuffer> {
        self.group_buffers
            .values_mut()
            .find_map(|group_buffers| group_buffers.group_buffer_mut(group_id))
    }
}

#[derive(Debug)]
pub(crate) struct GroupBuffers {
    buffer_size: usize,
    group_buffers: HashMap<u16, GroupBuffer>,
}

impl GroupBuffers {
    fn new(buffer_size: usize) -> Self {
        Self {
            buffer_size,
            group_buffers: Default::default(),
        }
    }

    fn insert_new_group_buffer(&mut self, group_id: u16, group_buffer: GroupBuffer) {
        // the group_id should not be used
        debug_assert!(!self.group_buffers.contains_key(&group_id));

        self.group_buffers.insert(group_id, group_buffer);
    }

    /// select a registered and contains available buffer group buffer at first
    ///
    /// if not exist, return a registering group buffer to hint driver it only need to wait
    ///
    /// if no available or registering group buffer, create a new group buffer
    fn select_group_buffer(&mut self, group_id_gen: &mut GroupIdGenerator) -> &mut GroupBuffer {
        if let Some(group_buffer) = self
            .group_buffers
            .values_mut()
            .filter(|group_buffer| {
                group_buffer.register_state == GroupBufferRegisterState::Registered
            })
            .find(|group_buffer| group_buffer.available > group_buffer.on_fly)
        {
            // consider a problem: https://github.com/rust-lang/rust/issues/86842
            // use some unsafe to beat the rustc

            // return group_buffer;

            let group_buffer: *mut GroupBuffer = group_buffer;

            return unsafe { &mut (*group_buffer) };
        }

        if let Some(group_buffer) = self.group_buffers.values_mut().find(|group_buffer| {
            group_buffer.register_state == GroupBufferRegisterState::Registering
        }) {
            // consider a problem: https://github.com/rust-lang/rust/issues/86842
            // use some unsafe to beat the rustc

            // return group_buffer;

            let group_buffer: *mut GroupBuffer = group_buffer;

            return unsafe { &mut (*group_buffer) };
        }

        let new_group_id = *group_id_gen;
        *group_id_gen += 1;

        let group_buffer = GroupBuffer::new(new_group_id, self.buffer_size);

        self.insert_new_group_buffer(new_group_id, group_buffer);

        self.group_buffers.get_mut(&new_group_id).unwrap()
    }

    fn give_back_buffer(&mut self, buffer: Buffer) {
        let group_id = buffer.group_id();

        let group_buffer = self
            .group_buffers
            .get_mut(&group_id)
            .unwrap_or_else(|| panic!("group id {} group buffer not exist", group_id));

        group_buffer.give_back_buffer();
    }

    fn group_buffer_mut(&mut self, group_id: u16) -> Option<&mut GroupBuffer> {
        self.group_buffers.get_mut(&group_id)
    }
}

pub(crate) struct GroupBuffer {
    group_id: u16,
    low_level_buffer: Vec<u8>,
    every_buf_size: usize,
    on_fly: u16,
    available: u16,
    register_state: GroupBufferRegisterState,
}

impl Debug for GroupBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupBuffer")
            .field("group_id", &self.group_id)
            .field("every_buf_size", &self.every_buf_size)
            .field("on_fly", &self.on_fly)
            .field("available", &self.available)
            .field("register_state", &self.register_state)
            .finish()
    }
}

impl GroupBuffer {
    fn new(group_id: u16, buf_len: usize) -> Self {
        let low_level_buffer = vec![0; buf_len * EVERY_GROUP_BUFFER_BUFFER_COUNT];

        Self {
            group_id,
            low_level_buffer,
            every_buf_size: buf_len,
            on_fly: 0,
            available: 0,
            register_state: GroupBufferRegisterState::NotRegister,
        }
    }

    pub(crate) unsafe fn take_buffer(
        &mut self,
        buffer_id: u16,
        available_size: usize,
        driver: Driver,
    ) -> Buffer {
        // when take a buffer, there should be someone waiting for a buffer so on_fly must >1
        debug_assert!(self.on_fly > 0);

        // when take a buffer, there should be at least one buffer is available
        debug_assert!(self.available > 0);

        self.decrease_on_fly();
        self.decrease_available();

        let start = (buffer_id - 1) as usize * self.every_buf_size;
        let end = start + self.every_buf_size;

        let buffer_slice = &mut self.low_level_buffer[start..end];

        Buffer::new(
            self.group_id,
            buffer_id,
            buffer_slice.as_mut_ptr(),
            buffer_slice.len(),
            available_size,
            driver,
        )
    }

    pub(crate) fn increase_on_fly(&mut self) {
        debug_assert!(self.on_fly < self.available);

        self.on_fly += 1;
    }

    pub(crate) fn decrease_on_fly(&mut self) {
        debug_assert!(self.on_fly > 0);

        self.on_fly -= 1;
    }

    pub(crate) fn increase_available(&mut self) {
        debug_assert!(
            (self.low_level_buffer.len() / self.every_buf_size) > self.available as usize,
            "GroupBuffer {:?}",
            self,
        );

        self.available += 1;
    }

    pub(crate) fn decrease_available(&mut self) {
        debug_assert!(self.available > 0);

        self.available -= 1;
    }

    pub(crate) fn register_state(&self) -> GroupBufferRegisterState {
        self.register_state
    }

    /// only can use when registering -> registered
    pub(crate) fn set_can_be_selected(&mut self) {
        debug_assert_eq!(self.register_state, GroupBufferRegisterState::Registering);

        self.available = (self.low_level_buffer.len() / self.every_buf_size) as _;

        self.set_register_state(GroupBufferRegisterState::Registered);
    }

    /// set a new register state and return old
    pub(crate) fn set_register_state(
        &mut self,
        register_state: GroupBufferRegisterState,
    ) -> GroupBufferRegisterState {
        mem::replace(&mut self.register_state, register_state)
    }

    pub(crate) fn group_id(&self) -> u16 {
        self.group_id
    }

    pub(crate) fn low_level_buffer_addr(&mut self) -> *mut u8 {
        self.low_level_buffer.as_mut_ptr()
    }

    pub(crate) fn low_level_buffer_by_buffer_id(&mut self, buffer_id: u16) -> *mut u8 {
        // because buffer_id start from 1
        let offset = buffer_id as usize - 1;

        (&mut self.low_level_buffer[(offset as usize * self.every_buf_size)..]).as_mut_ptr()
    }

    pub(crate) fn every_buf_size(&self) -> usize {
        self.every_buf_size
    }

    pub(crate) fn buffer_count(&self) -> usize {
        self.low_level_buffer.len() / self.every_buf_size
    }

    fn give_back_buffer(&mut self) {
        // when give back a buffer, there must be at least one buffer is take
        debug_assert!(
            (self.available as usize) < self.low_level_buffer.len() / self.every_buf_size
        );
    }
}

pub struct Buffer {
    group_id: u16,
    buffer_id: u16,
    buf_ptr: *mut u8,
    buf_size: usize,
    available_size: usize,
    used: usize,
    driver: Option<Driver>,
}

impl Buffer {
    /// when create a buffer, should make sure all arguments is valid
    unsafe fn new(
        group_id: u16,
        buffer_id: u16,
        buf_ptr: *mut u8,
        buf_size: usize,
        available_size: usize,
        driver: Driver,
    ) -> Self {
        debug_assert!(available_size <= buf_size);

        Self {
            group_id,
            buffer_id,
            buf_ptr,
            buf_size,
            available_size,
            used: 0,
            driver: Some(driver),
        }
    }

    pub(crate) fn low_level_buf_ptr_mut(&mut self) -> *mut u8 {
        self.buf_ptr
    }

    pub(crate) fn group_id(&self) -> u16 {
        self.group_id
    }

    pub(crate) fn buffer_size(&self) -> usize {
        self.buf_size
    }

    pub(crate) fn buffer_id(&self) -> u16 {
        self.buffer_id
    }

    pub(crate) fn consume(&mut self, amt: usize) {
        self.used += amt;

        debug_assert!(self.used <= self.available_size);
    }

    /// don't return the buffer to the driver
    pub fn forget(mut self) {
        self.driver.take();
    }
}

unsafe impl Send for Buffer {}

unsafe impl Sync for Buffer {}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let slice = unsafe { &*ptr::slice_from_raw_parts(self.buf_ptr, self.available_size) };
        &slice[self.used..]
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if let Some(driver) = &self.driver {
            // don't decrease on fly because when a Buffer created by take_buffer, the on_fly is
            // decreased
            driver.give_back_buffer_with_id(self.group_id, self.buffer_id, false);
        }
    }
}
