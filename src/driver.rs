use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::ffi::CString;
use std::io::Result;
use std::mem::MaybeUninit;
use std::os::unix::io::RawFd;
use std::task::Waker;
use std::time::Duration;

use io_uring::cqueue::Entry as CqEntry;
use io_uring::opcode::{AsyncCancel, ProvideBuffers};
use io_uring::squeue::Entry as SqEntry;
use io_uring::{IoUring, Probe};
use nix::sys::socket::SockAddr;

use crate::buffer::{Buffer, BufferManager, GroupBufferRegisterState};

thread_local! {
    pub static DRIVER: RefCell<Option<Driver>> = RefCell::new(None);
}

#[derive(Debug, Clone)]
pub enum Callback {
    ProvideBuffer {
        group_id: u16,
    },

    CancelRead {
        group_id: u16,
    },

    // save the addr in Box so no matter how to move the addr, won't break the pointer that in the
    // io_uring
    CancelConnect {
        addr: Box<SockAddr>,
        fd: RawFd,
    },

    CancelOpenAt {
        path: CString,
    },

    CancelStatx {
        path: CString,
        statx: Box<MaybeUninit<libc::statx>>,
    },

    CancelRenameAt {
        old_path: CString,
        new_path: CString,
    },

    CancelUnlinkAt {
        path: CString,
    },

    Wakeup {
        waker: Waker,
    },
}

pub struct Driver {
    pub ring: IoUring,

    pub next_user_data: u64,

    /// when sq is full, store the waker, after consume cqe, wake all of them
    pub wait_for_push_wakers: VecDeque<Waker>,

    pub buffer_manager: BufferManager,

    pub completion_queue_entries: HashMap<u64, CqEntry>,

    pub callbacks: HashMap<u64, Callback>,

    pub background_sqes: VecDeque<SqEntry>,
}

impl Driver {
    pub fn new(sq_poll: Option<Duration>) -> Result<Self> {
        let mut builder = IoUring::builder();

        if let Some(sq_poll) = sq_poll {
            let sq_poll = sq_poll.as_millis();

            builder.setup_sqpoll(sq_poll as _);
        }

        let ring = builder.build(4096)?;

        if !check_support_provide_buffers(&ring)? {
            panic!("The kernel doesn't support IORING_OP_PROVIDE_BUFFERS");
        }

        if !check_support_fast_poll(&ring) {
            panic!("The kernel doesn't support io_uring fast_poll");
        }

        Ok(Self {
            ring,
            next_user_data: 1,
            wait_for_push_wakers: Default::default(),
            buffer_manager: BufferManager::new(),
            completion_queue_entries: Default::default(),
            callbacks: Default::default(),
            background_sqes: Default::default(),
        })
    }

    pub fn select_group_buffer(
        &mut self,
        buffer_size: usize,
        waker: &Waker,
    ) -> Result<Option<u16>> {
        let group_buffer = self.buffer_manager.select_group_buffer(buffer_size);

        if group_buffer.register_state() == GroupBufferRegisterState::Registered {
            group_buffer.increase_on_fly();

            return Ok(Some(group_buffer.group_id()));
        } else if group_buffer.register_state() == GroupBufferRegisterState::Registering {
            // reduce group buffer allocate, if found registering group buffer, let task wait
            self.wait_for_push_wakers.push_back(waker.clone());

            return Ok(None);
        }

        // no available group buffer found, save the waker at first
        self.wait_for_push_wakers.push_back(waker.clone());

        let user_data = self.next_user_data;
        self.next_user_data += 1;

        let provide_buffer_sqe = ProvideBuffers::new(
            group_buffer.low_level_buffer_addr(),
            group_buffer.every_buf_size() as _,
            group_buffer.buffer_count() as _,
            group_buffer.group_id(),
            1,
        )
        .build()
        .user_data(user_data);

        // register the group buffer
        unsafe {
            if self.ring.submission().push(&provide_buffer_sqe).is_err() {
                self.ring.submit()?;

                if self.ring.submission().push(&provide_buffer_sqe).is_err() {
                    return Ok(None);
                }
            }

            self.ring.submit()?;
        }

        self.callbacks.insert(
            user_data,
            Callback::ProvideBuffer {
                group_id: group_buffer.group_id(),
            },
        );

        group_buffer.set_register_state(GroupBufferRegisterState::Registering);

        Ok(None)
    }

    pub fn take_buffer(
        &mut self,
        buffer_size: usize,
        group_id: u16,
        buffer_id: u16,
        buffer_data_size: usize,
    ) -> Buffer {
        self.buffer_manager
            .take_buffer(buffer_size, group_id, buffer_id, buffer_data_size)
    }

    pub fn give_back_buffer(&mut self, mut buffer: Buffer) {
        let low_level_buf_ptr_mut = buffer.low_level_buf_ptr_mut();
        let group_id = buffer.group_id();
        let buffer_id = buffer.buffer_id();
        let buffer_size = buffer.buffer_size();

        let user_data = self.next_user_data;
        self.next_user_data += 1;

        let provide_buffer_sqe = ProvideBuffers::new(
            low_level_buf_ptr_mut,
            buffer_size as _,
            1,
            group_id,
            buffer_id,
        )
        .build()
        .user_data(user_data);

        self.buffer_manager.give_back_buffer(buffer);

        self.background_sqes.push_back(provide_buffer_sqe);

        self.callbacks
            .insert(user_data, Callback::ProvideBuffer { group_id });
    }

    pub fn give_back_buffer_with_id(&mut self, group_id: u16, buffer_id: u16) {
        let group_buffer = self
            .buffer_manager
            .group_buffer_mut(group_id)
            .unwrap_or_else(|| panic!("group buffer {} not found", group_id));

        group_buffer.decrease_on_fly();

        let ptr_mut = group_buffer.low_level_buffer_by_buffer_id(buffer_id);
        let len = group_buffer.every_buf_size();

        let user_data = self.next_user_data;
        self.next_user_data += 1;

        let provide_buffer_sqe = ProvideBuffers::new(ptr_mut, len as _, 1, group_id, buffer_id)
            .build()
            .user_data(user_data);

        self.background_sqes.push_back(provide_buffer_sqe);

        self.callbacks
            .insert(user_data, Callback::ProvideBuffer { group_id });
    }

    pub fn decrease_on_fly_for_not_use_group_buffer(&mut self, buffer_size: usize, group_id: u16) {
        self.buffer_manager
            .decrease_on_fly_for_not_use_group_buffer(buffer_size, group_id);
    }

    pub fn push_sqe_with_waker(&mut self, mut sqe: SqEntry, waker: Waker) -> Result<Option<u64>> {
        let user_data = self.next_user_data;
        self.next_user_data += 1;

        sqe = sqe.user_data(user_data);

        unsafe {
            if self.ring.submission().push(&sqe).is_err() {
                self.ring.submit()?;

                // sq is still full, push in next times
                if self.ring.submission().push(&sqe).is_err() {
                    self.wait_for_push_wakers.push_back(waker);

                    return Ok(None);
                }
            }

            self.callbacks.insert(user_data, Callback::Wakeup { waker });

            self.ring.submit()?;

            Ok(Some(user_data))
        }
    }

    /*pub fn push_sqes_with_waker(
        &mut self,
        mut sqes: Vec<SqEntry>,
        waker: Waker,
    ) -> Result<Option<Vec<u64>>> {
        let user_data = self.next_user_data;
        self.next_user_data += sqes.len() as u64;

        let mut user_data_list = Vec::with_capacity(sqes.len());
        for i in 0..sqes.len() {
            user_data_list.push(user_data + i as u64);
        }

        for (index, &user_data) in user_data_list.iter().enumerate() {
            sqes[index] = sqes[index].clone().user_data(user_data);
        }

        unsafe {
            if self.ring.submission().push_multiple(&sqes).is_err() {
                self.ring.submit()?;

                // sq is still full, push in next times
                if self.ring.submission().push_multiple(&sqes).is_err() {
                    self.wait_for_push_wakers.push_back(waker);

                    return Ok(None);
                }
            }

            for &user_data in user_data_list.iter() {
                self.callbacks.insert(
                    user_data,
                    Callback::Wakeup {
                        waker: waker.clone(),
                    },
                );
            }

            self.ring.submit()?;

            Ok(Some(user_data_list))
        }
    }*/

    pub fn push_sqe(&mut self, sqe: &SqEntry) -> Result<bool> {
        unsafe {
            if self.ring.submission().push(sqe).is_err() {
                self.ring.submit()?;

                // sq is still full, push in next times
                if self.ring.submission().push(sqe).is_err() {
                    return Ok(false);
                }
            }

            self.ring.submit()?;

            Ok(true)
        }
    }

    /// cancel a event without any callback
    pub fn cancel_normal(&mut self, user_data: u64) -> Result<()> {
        let cancel_sqe = AsyncCancel::new(user_data).build();

        unsafe {
            if self.ring.submission().push(&cancel_sqe).is_err() {
                self.ring.submit()?;

                if self.ring.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.push_back(cancel_sqe);
                }
            }
        }

        self.callbacks.remove(&user_data);

        Ok(())
    }

    /// cancel the read event, when the event canceled or ready after cancel, give back the buffer
    /// if buffer is used
    pub fn cancel_read(&mut self, user_data: u64, group_id: u16) -> Result<()> {
        let background_user_data = self.next_user_data;
        self.next_user_data += 1;

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        unsafe {
            if self.ring.submission().push(&cancel_sqe).is_err() {
                self.ring.submit()?;

                if self.ring.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.push_back(cancel_sqe);
                }
            }
        }

        // change the callback to CancelRead
        self.callbacks
            .insert(user_data, Callback::CancelRead { group_id });

        Ok(())
    }

    /// cancel the connect event, when the event canceled or ready after cancel, let the drop
    /// release data
    pub fn cancel_connect(&mut self, user_data: u64, addr: Box<SockAddr>, fd: RawFd) -> Result<()> {
        let background_user_data = self.next_user_data;
        self.next_user_data += 1;

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        unsafe {
            if self.ring.submission().push(&cancel_sqe).is_err() {
                self.ring.submit()?;

                if self.ring.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.push_back(cancel_sqe);
                }
            }
        }

        // change the callback to CancelConnect
        self.callbacks
            .insert(user_data, Callback::CancelConnect { addr, fd });

        Ok(())
    }

    /// cancel the open_at event, when the event canceled or ready after cancel, let the drop
    /// release data
    pub fn cancel_open_at(&mut self, user_data: u64, path: CString) -> Result<()> {
        let background_user_data = self.next_user_data;
        self.next_user_data += 1;

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        unsafe {
            if self.ring.submission().push(&cancel_sqe).is_err() {
                self.ring.submit()?;

                if self.ring.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.push_back(cancel_sqe);
                }
            }
        }

        // change the callback to CancelOpenAt
        self.callbacks
            .insert(user_data, Callback::CancelOpenAt { path });

        Ok(())
    }

    /// cancel the statx event, when the event canceled or ready after cancel, let the drop release
    /// data
    pub fn cancel_statx(
        &mut self,
        user_data: u64,
        path: CString,
        statx: Box<MaybeUninit<libc::statx>>,
    ) -> Result<()> {
        let background_user_data = self.next_user_data;
        self.next_user_data += 1;

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        unsafe {
            if self.ring.submission().push(&cancel_sqe).is_err() {
                self.ring.submit()?;

                if self.ring.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.push_back(cancel_sqe);
                }
            }
        }

        // change the callback to CancelStatx
        self.callbacks
            .insert(user_data, Callback::CancelStatx { path, statx });

        Ok(())
    }

    /// cancel the rename_at event, when the event canceled or ready after cancel, let the drop
    /// release data
    pub fn cancel_rename_at(
        &mut self,
        user_data: u64,
        old_path: CString,
        new_path: CString,
    ) -> Result<()> {
        let background_user_data = self.next_user_data;
        self.next_user_data += 1;

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        unsafe {
            if self.ring.submission().push(&cancel_sqe).is_err() {
                self.ring.submit()?;

                if self.ring.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.push_back(cancel_sqe);
                }
            }
        }

        // change the callback to CancelStatx
        self.callbacks
            .insert(user_data, Callback::CancelRenameAt { old_path, new_path });

        Ok(())
    }

    /// cancel the unlink_at event, when the event canceled or ready after cancel, let the drop
    /// release data
    pub fn cancel_unlink_at(&mut self, user_data: u64, path: CString) -> Result<()> {
        let background_user_data = self.next_user_data;
        self.next_user_data += 1;

        let cancel_sqe = AsyncCancel::new(user_data)
            .build()
            .user_data(background_user_data);

        unsafe {
            if self.ring.submission().push(&cancel_sqe).is_err() {
                self.ring.submit()?;

                if self.ring.submission().push(&cancel_sqe).is_err() {
                    self.background_sqes.push_back(cancel_sqe);
                }
            }
        }

        // change the callback to CancelStatx
        self.callbacks
            .insert(user_data, Callback::CancelUnlinkAt { path });

        Ok(())
    }

    pub fn take_cqe_with_waker(&mut self, user_data: u64, waker: &Waker) -> Option<CqEntry> {
        match self.completion_queue_entries.remove(&user_data) {
            None => {
                self.callbacks.insert(
                    user_data,
                    Callback::Wakeup {
                        waker: waker.clone(),
                    },
                );

                None
            }

            Some(cqe) => {
                self.callbacks.remove(&user_data);

                Some(cqe)
            }
        }
    }

    pub fn take_callback(&mut self, user_data: u64) -> Option<Callback> {
        self.callbacks.remove(&user_data)
    }

    pub fn try_run_all_wait_sqes(&mut self) {
        self.background_sqes
            .drain(..)
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|sqe| {
                if !self
                    .push_sqe(&sqe)
                    .unwrap_or_else(|err| panic!("push sqe failed: {}", err))
                {
                    self.background_sqes.push_back(sqe);
                }
            });
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
