use std::collections::VecDeque;
use std::ffi::CString;
use std::fmt::{Debug, Formatter};
use std::io::{Error, ErrorKind, Result};
use std::mem::MaybeUninit;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Waker;
use std::time::Duration;

use io_uring::cqueue::Entry as CqEntry;
use io_uring::opcode::{AsyncCancel, ProvideBuffers};
use io_uring::squeue::Entry as SqEntry;
use io_uring::types::Timespec;
use io_uring::{IoUring, Probe};
use nix::sys::socket::SockAddr;
use parking_lot::Mutex;

use crate::buffer::{Buffer, GroupBufferRegisterState};
use crate::callback::{Callback, CallbacksAndCompleteEntries};
use crate::driver_resource::DriverResource;
use crate::reactor::Reactor;

#[derive(Debug, Clone, Default)]
pub struct DriverBuilder {
    sq_poll: Option<Duration>,
    sq_poll_cpu: Option<u32>,
    io_poll: bool,
    entries: Option<u32>,
}

impl DriverBuilder {
    pub fn sq_poll<T: Into<Option<Duration>>>(self, sq_poll: T) -> Self {
        Self {
            sq_poll: sq_poll.into(),
            ..self
        }
    }

    pub fn sq_poll_cpu<T: Into<Option<u32>>>(self, sq_poll_cpu: T) -> Self {
        Self {
            sq_poll_cpu: sq_poll_cpu.into(),
            ..self
        }
    }

    pub fn io_poll(self, io_poll: bool) -> Self {
        Self { io_poll, ..self }
    }

    pub fn entries<T: Into<Option<u32>>>(self, entries: T) -> Self {
        Self {
            entries: entries.into(),
            ..self
        }
    }

    pub fn build(self) -> Result<(Driver, Reactor)> {
        Driver::new(self.sq_poll, self.sq_poll_cpu, self.io_poll, self.entries)
    }
}

#[derive(Clone)]
pub struct Driver {
    driver_resource: Arc<DriverResource>,

    pub(crate) next_user_data: Arc<AtomicU64>,

    /// when sq is full, store the waker, after consume cqe, wake all of them
    pub(crate) wait_for_push_wakers: Arc<Mutex<VecDeque<Waker>>>,

    pub(crate) callbacks_and_complete_entries: Arc<Mutex<CallbacksAndCompleteEntries>>,

    pub(crate) background_sqes: Arc<Mutex<VecDeque<SqEntry>>>,
}

impl Debug for Driver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Driver").finish()
    }
}

impl Driver {
    pub fn builder() -> DriverBuilder {
        Default::default()
    }

    fn new(
        sq_poll: Option<Duration>,
        sq_poll_cpu: Option<u32>,
        io_poll: bool,
        entries: Option<u32>,
    ) -> Result<(Self, Reactor)> {
        let mut builder = IoUring::builder();

        if let Some(sq_poll) = sq_poll {
            let sq_poll = sq_poll.as_millis();

            builder.setup_sqpoll(sq_poll as _);
        }

        if let Some(sq_poll_cpu) = sq_poll_cpu {
            builder.setup_sqpoll_cpu(sq_poll_cpu);
        }

        if io_poll {
            builder.setup_iopoll();
        }

        let ring = builder.build(entries.unwrap_or(4096))?;

        if !check_support_provide_buffers(&ring)? {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "The kernel doesn't support IORING_OP_PROVIDE_BUFFERS",
            ));
        }

        if !check_support_fast_poll(&ring) {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "The kernel doesn't support io_uring fast_poll",
            ));
        }

        let driver_resource = Arc::new(DriverResource::new(ring));

        let driver = Self {
            driver_resource: driver_resource.clone(),
            next_user_data: Arc::new(AtomicU64::new(1)),
            wait_for_push_wakers: Default::default(),
            callbacks_and_complete_entries: Default::default(),
            background_sqes: Default::default(),
        };

        let reactor = Reactor::new(&driver, driver_resource);

        Ok((driver, reactor))
    }

    pub(crate) fn select_group_buffer(
        &self,
        buffer_size: usize,
        waker: &Waker,
    ) -> Result<Option<u16>> {
        let mut buffer_manager = self.driver_resource.buffer_manager();
        let group_buffer = buffer_manager.select_group_buffer(buffer_size);

        if group_buffer.register_state() == GroupBufferRegisterState::Registered {
            group_buffer.increase_on_fly();

            return Ok(Some(group_buffer.group_id()));
        } else if group_buffer.register_state() == GroupBufferRegisterState::Registering {
            // reduce group buffer allocate, if found registering group buffer, let task wait
            self.wait_for_push_wakers.lock().push_back(waker.clone());

            return Ok(None);
        }

        // no available group buffer found, save the waker at first
        self.wait_for_push_wakers.lock().push_back(waker.clone());

        let user_data = self.next_user_data.fetch_add(1, Ordering::Relaxed);

        let provide_buffer_sqe = ProvideBuffers::new(
            group_buffer.low_level_buffer_addr(),
            group_buffer.every_buf_size() as _,
            group_buffer.buffer_count() as _,
            group_buffer.group_id(),
            1,
        )
        .build()
        .user_data(user_data);

        // lock the callback at first, so the reactor won't acquire the ProviderBuffer but can't
        // find the callback
        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        // register the group buffer
        unsafe {
            if self
                .driver_resource
                .submission()
                .push(&provide_buffer_sqe)
                .is_err()
            {
                self.driver_resource.submitter().submit()?;

                if self
                    .driver_resource
                    .submission()
                    .push(&provide_buffer_sqe)
                    .is_err()
                {
                    return Ok(None);
                }
            }
        }

        self.driver_resource.submitter().submit()?;

        callbacks_and_complete_entries.callbacks.insert(
            user_data,
            Callback::ProvideBuffer {
                group_id: group_buffer.group_id(),
            },
        );

        group_buffer.set_register_state(GroupBufferRegisterState::Registering);

        Ok(None)
    }

    pub(crate) fn take_buffer(
        &self,
        buffer_size: usize,
        group_id: u16,
        buffer_id: u16,
        buffer_data_size: usize,
    ) -> Buffer {
        self.driver_resource.buffer_manager().take_buffer(
            buffer_size,
            group_id,
            buffer_id,
            buffer_data_size,
            self.clone(),
        )
    }

    pub(crate) fn give_back_buffer_with_id(
        &self,
        group_id: u16,
        buffer_id: u16,
        need_decrease_on_fly: bool,
    ) {
        let mut buffer_manager = self.driver_resource.buffer_manager();

        let group_buffer = buffer_manager
            .group_buffer_mut(group_id)
            .unwrap_or_else(|| panic!("group buffer {} not found", group_id));

        if need_decrease_on_fly {
            group_buffer.decrease_on_fly();
        }

        let ptr_mut = group_buffer.low_level_buffer_by_buffer_id(buffer_id);
        let len = group_buffer.every_buf_size();

        let user_data = self.next_user_data.fetch_add(1, Ordering::Relaxed);

        let provide_buffer_sqe = ProvideBuffers::new(ptr_mut, len as _, 1, group_id, buffer_id)
            .build()
            .user_data(user_data);

        // lock the callback at first, so the reactor won't acquire the ProviderBuffer but can't
        // find the callback
        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        callbacks_and_complete_entries
            .callbacks
            .insert(user_data, Callback::ProvideBuffer { group_id });

        self.background_sqes.lock().push_back(provide_buffer_sqe);
    }

    pub(crate) fn decrease_on_fly_for_not_use_group_buffer(
        &self,
        buffer_size: usize,
        group_id: u16,
    ) {
        self.driver_resource
            .buffer_manager()
            .decrease_on_fly_for_not_use_group_buffer(buffer_size, group_id);
    }

    pub(crate) fn push_sqe_with_waker(
        &self,
        mut sqe: SqEntry,
        waker: Waker,
    ) -> Result<Option<u64>> {
        let user_data = self.next_user_data.fetch_add(1, Ordering::Relaxed);

        sqe = sqe.user_data(user_data);

        // lock the callback at first, so the reactor won't acquire the cqe but can't find the
        // callback
        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        unsafe {
            if self.driver_resource.submission().push(&sqe).is_err() {
                self.driver_resource.submitter().submit()?;

                // sq is still full, push in next times
                if self.driver_resource.submission().push(&sqe).is_err() {
                    self.wait_for_push_wakers.lock().push_back(waker);

                    return Ok(None);
                }
            }
        }

        callbacks_and_complete_entries
            .callbacks
            .insert(user_data, Callback::Wakeup { waker });

        self.driver_resource.submitter().submit()?;

        Ok(Some(user_data))
    }

    /// cancel a event without any callback
    pub(crate) fn cancel_normal(&self, user_data: u64) -> Result<()> {
        let cancel_sqe = AsyncCancel::new(user_data).build();

        // lock the callback at first, so the reactor won't acquire the cancel cqe but can't find
        // the callback
        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        // the cqe callback is run, we need to clear the completion_queue_entries
        if callbacks_and_complete_entries
            .callbacks
            .remove(&user_data)
            .is_none()
        {
            callbacks_and_complete_entries
                .completion_queue_entries
                .remove(&user_data);
        }

        unsafe {
            if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                self.driver_resource.submitter().submit()?;

                if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.lock().push_back(cancel_sqe);
                }
            }
        }

        self.driver_resource.submitter().submit()?;

        Ok(())
    }

    /// cancel the read event, when the event canceled or ready after cancel, give back the buffer
    /// if buffer is used
    pub(crate) fn cancel_read(&self, user_data: u64, group_id: u16) -> Result<()> {
        let background_user_data = self.next_user_data.fetch_add(1, Ordering::Relaxed);

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        // lock the callback at first, so the reactor won't acquire the cancel cqe but can't find
        // the callback
        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        // the cqe callback is run, we need to clear the completion_queue_entries, and no need to
        // cancel sqe
        if callbacks_and_complete_entries
            .callbacks
            .remove(&user_data)
            .is_none()
        {
            callbacks_and_complete_entries
                .completion_queue_entries
                .remove(&user_data);

            return Ok(());
        }

        unsafe {
            if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                self.driver_resource.submitter().submit()?;

                if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.lock().push_back(cancel_sqe);
                }
            }
        }

        self.driver_resource.submitter().submit()?;

        // change the callback to CancelRead
        callbacks_and_complete_entries
            .callbacks
            .insert(user_data, Callback::CancelRead { group_id });

        Ok(())
    }

    /// cancel the connect event, when the event canceled or ready after cancel, let the drop
    /// release data
    pub(crate) fn cancel_connect(
        &self,
        user_data: u64,
        addr: Box<SockAddr>,
        fd: RawFd,
    ) -> Result<()> {
        let background_user_data = self.next_user_data.fetch_add(1, Ordering::Relaxed);

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        // lock the callback at first, so the reactor won't acquire the cancel cqe but can't find
        // the callback
        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        // the cqe callback is run, we need to clear the completion_queue_entries, and no need to
        // cancel sqe
        if callbacks_and_complete_entries
            .callbacks
            .remove(&user_data)
            .is_none()
        {
            callbacks_and_complete_entries
                .completion_queue_entries
                .remove(&user_data);

            return Ok(());
        }

        unsafe {
            if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                self.driver_resource.submitter().submit()?;

                if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.lock().push_back(cancel_sqe);
                }
            }
        }

        self.driver_resource.submitter().submit()?;

        // change the callback to CancelConnect
        callbacks_and_complete_entries
            .callbacks
            .insert(user_data, Callback::CancelConnect { addr, fd });

        Ok(())
    }

    /// cancel the open_at event, when the event canceled or ready after cancel, let the drop
    /// release data
    pub(crate) fn cancel_open_at(&self, user_data: u64, path: CString) -> Result<()> {
        let background_user_data = self.next_user_data.fetch_add(1, Ordering::Relaxed);

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        // lock the callback at first, so the reactor won't acquire the cancel cqe but can't find
        // the callback
        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        // the cqe callback is run, we need to clear the completion_queue_entries, and no need to
        // cancel sqe
        if callbacks_and_complete_entries
            .callbacks
            .remove(&user_data)
            .is_none()
        {
            callbacks_and_complete_entries
                .completion_queue_entries
                .remove(&user_data);

            return Ok(());
        }

        unsafe {
            if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                self.driver_resource.submitter().submit()?;

                if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.lock().push_back(cancel_sqe);
                }
            }
        }

        self.driver_resource.submitter().submit()?;

        // change the callback to CancelOpenAt
        callbacks_and_complete_entries
            .callbacks
            .insert(user_data, Callback::CancelOpenAt { path });

        Ok(())
    }

    /// cancel the statx event, when the event canceled or ready after cancel, let the drop release
    /// data
    pub(crate) fn cancel_statx(
        &self,
        user_data: u64,
        path: CString,
        statx: Box<MaybeUninit<libc::statx>>,
    ) -> Result<()> {
        let background_user_data = self.next_user_data.fetch_add(1, Ordering::Relaxed);

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        // lock the callback at first, so the reactor won't acquire the cancel cqe but can't find
        // the callback
        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        // the cqe callback is run, we need to clear the completion_queue_entries, and no need to
        // cancel sqe
        if callbacks_and_complete_entries
            .callbacks
            .remove(&user_data)
            .is_none()
        {
            callbacks_and_complete_entries
                .completion_queue_entries
                .remove(&user_data);

            return Ok(());
        }

        unsafe {
            if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                self.driver_resource.submitter().submit()?;

                if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.lock().push_back(cancel_sqe);
                }
            }
        }

        self.driver_resource.submitter().submit()?;

        // change the callback to CancelStatx
        callbacks_and_complete_entries
            .callbacks
            .insert(user_data, Callback::CancelStatx { path, statx });

        Ok(())
    }

    /// cancel the rename_at event, when the event canceled or ready after cancel, let the drop
    /// release data
    pub(crate) fn cancel_rename_at(
        &self,
        user_data: u64,
        old_path: CString,
        new_path: CString,
    ) -> Result<()> {
        let background_user_data = self.next_user_data.fetch_add(1, Ordering::Relaxed);

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        // lock the callback at first, so the reactor won't acquire the cancel cqe but can't find
        // the callback
        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        // the cqe callback is run, we need to clear the completion_queue_entries, and no need to
        // cancel sqe
        if callbacks_and_complete_entries
            .callbacks
            .remove(&user_data)
            .is_none()
        {
            callbacks_and_complete_entries
                .completion_queue_entries
                .remove(&user_data);

            return Ok(());
        }

        unsafe {
            if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                self.driver_resource.submitter().submit()?;

                if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.lock().push_back(cancel_sqe);
                }
            }
        }

        self.driver_resource.submitter().submit()?;

        // change the callback to CancelStatx
        callbacks_and_complete_entries
            .callbacks
            .insert(user_data, Callback::CancelRenameAt { old_path, new_path });

        Ok(())
    }

    /// cancel the unlink_at event, when the event canceled or ready after cancel, let the drop
    /// release data
    pub(crate) fn cancel_unlink_at(&self, user_data: u64, path: CString) -> Result<()> {
        let background_user_data = self.next_user_data.fetch_add(1, Ordering::Relaxed);

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        // lock the callback at first, so the reactor won't acquire the cancel cqe but can't find
        // the callback
        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        // the cqe callback is run, we need to clear the completion_queue_entries, and no need to
        // cancel sqe
        if callbacks_and_complete_entries
            .callbacks
            .remove(&user_data)
            .is_none()
        {
            callbacks_and_complete_entries
                .completion_queue_entries
                .remove(&user_data);

            return Ok(());
        }

        unsafe {
            if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                self.driver_resource.submitter().submit()?;

                if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.lock().push_back(cancel_sqe);
                }
            }
        }

        self.driver_resource.submitter().submit()?;

        // change the callback to CancelStatx
        callbacks_and_complete_entries
            .callbacks
            .insert(user_data, Callback::CancelUnlinkAt { path });

        Ok(())
    }

    /// cancel the timeout event, when the event canceled or ready after cancel, let the drop
    /// release data
    pub(crate) fn cancel_timeout(&self, user_data: u64, timespec: Box<Timespec>) -> Result<()> {
        let background_user_data = self.next_user_data.fetch_add(1, Ordering::Relaxed);

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        // lock the callback at first, so the reactor won't acquire the cancel cqe but can't find
        // the callback
        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        // the cqe callback is run, we need to clear the completion_queue_entries, and no need to
        // cancel sqe
        if callbacks_and_complete_entries
            .callbacks
            .remove(&user_data)
            .is_none()
        {
            callbacks_and_complete_entries
                .completion_queue_entries
                .remove(&user_data);

            return Ok(());
        }

        unsafe {
            if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                self.driver_resource.submitter().submit()?;

                if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.lock().push_back(cancel_sqe);
                }
            }
        }

        self.driver_resource.submitter().submit()?;

        // change the callback to CancelTimeout
        callbacks_and_complete_entries
            .callbacks
            .insert(user_data, Callback::CancelTimeout { timespec });

        Ok(())
    }

    /// cancel the accept event, when the event canceled or ready after cancel, let the drop
    /// release data
    pub(crate) fn cancel_accept(
        &self,
        user_data: u64,
        peer_addr: Box<libc::sockaddr_storage>,
        addr_size: Box<libc::socklen_t>,
    ) -> Result<()> {
        let background_user_data = self.next_user_data.fetch_add(1, Ordering::Relaxed);

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        // lock the callback at first, so the reactor won't acquire the cancel cqe but can't find
        // the callback
        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        // the cqe callback is run, we need to clear the completion_queue_entries, and no need to
        // cancel sqe
        if callbacks_and_complete_entries
            .callbacks
            .remove(&user_data)
            .is_none()
        {
            callbacks_and_complete_entries
                .completion_queue_entries
                .remove(&user_data);

            return Ok(());
        }

        unsafe {
            if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                self.driver_resource.submitter().submit()?;

                if self.driver_resource.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.lock().push_back(cancel_sqe);
                }
            }
        }

        self.driver_resource.submitter().submit()?;

        // change the callback to CancelConnect
        callbacks_and_complete_entries.callbacks.insert(
            user_data,
            Callback::CancelAccept {
                peer_addr,
                addr_size,
            },
        );

        Ok(())
    }

    pub(crate) fn take_cqe_with_waker(&self, user_data: u64, waker: &Waker) -> Option<CqEntry> {
        // lock the callback at first, so the reactor won't acquire the cqe but can't find the
        // callback
        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        match callbacks_and_complete_entries
            .completion_queue_entries
            .remove(&user_data)
        {
            None => {
                callbacks_and_complete_entries.callbacks.insert(
                    user_data,
                    Callback::Wakeup {
                        waker: waker.clone(),
                    },
                );

                None
            }

            Some(cqe) => {
                callbacks_and_complete_entries.callbacks.remove(&user_data);

                Some(cqe)
            }
        }
    }

    pub(crate) fn take_callback(&self, user_data: u64) -> Option<Callback> {
        self.callbacks_and_complete_entries
            .lock()
            .callbacks
            .remove(&user_data)
    }
}

fn check_support_provide_buffers(ring: &IoUring) -> Result<bool> {
    let mut probe = Probe::new();

    ring.submitter().register_probe(&mut probe)?;

    Ok(probe.is_supported(ProvideBuffers::CODE))
}

fn check_support_fast_poll(ring: &IoUring) -> bool {
    ring.params().is_feature_fast_poll()
}
