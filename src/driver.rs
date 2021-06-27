use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io::Result;
use std::task::Waker;
use std::time::Duration;

use io_uring::cqueue::Entry as CqEntry;
use io_uring::opcode::AsyncCancel;
use io_uring::squeue::Entry as SqEntry;
use io_uring::IoUring;

use crate::buffer::GroupBuffer;

pub struct Driver {
    pub ring: IoUring,

    /// use to provider buffer for io_uring ProvideBuffer
    pub buffers: HashMap<u16, GroupBuffer>,
    pub next_user_data: u64,

    /// wake up the task
    pub wakers: HashMap<u64, Waker>,

    /// when sq is full, store the waker, after consume cqe, wake all of them
    pub wait_for_push_wakers: VecDeque<Waker>,
    pub available_entries: HashMap<u64, CqEntry>,

    /// when a sqe want to cancel, register its user_data and callback
    pub cancel_user_data: HashMap<u64, CancelCallback>,
}

impl Driver {
    pub fn new(sq_poll: Option<Duration>) -> Result<Self> {
        let mut builder = IoUring::builder();

        if let Some(sq_poll) = sq_poll {
            let sq_poll = sq_poll.as_millis();

            builder.setup_sqpoll(sq_poll as _);
        }

        let ring = builder.build(4096)?;

        Ok(Self {
            ring,
            buffers: Default::default(),
            next_user_data: 1,
            wakers: Default::default(),
            wait_for_push_wakers: Default::default(),
            available_entries: Default::default(),
            cancel_user_data: Default::default(),
        })
    }

    /// try to push sqe into ring and return user_data, if push success
    pub fn push_sqe(&mut self, sqe: SqEntry, waker: Waker) -> Result<Option<u64>> {
        let user_data = self.next_user_data;
        self.next_user_data += 1;

        let sqe = sqe.user_data(user_data);

        unsafe {
            if self.ring.submission().push(&sqe).is_err() {
                self.ring.submit()?;

                // sq is still full, push in next times
                if self.ring.submission().push(&sqe).is_err() {
                    self.wait_for_push_wakers.push_back(waker);

                    return Ok(None);
                }
            }

            self.wakers.insert(user_data, waker);

            self.ring.submit()?;

            Ok(Some(user_data))
        }
    }

    /*/// this will keep trying pushing sqe until success or error happened
    pub fn push_sqe_without_waker(&mut self, sqe: SqEntry) -> Result<u64> {
        let user_data = self.next_user_data;
        self.next_user_data += 1;

        let sqe = sqe.user_data(user_data);

        loop {
            unsafe {
                let result = self.ring.submission().push(&sqe);
                self.ring.submit()?;

                if result.is_ok() {
                    return Ok(user_data);
                }
            }
        }
    }*/

    pub fn cancel_sqe(&mut self, user_data: u64) -> Result<()> {
        let cancel_sqe = AsyncCancel::new(user_data).build();

        self.push_sqe_without_waker_or_user_data(cancel_sqe)
    }

    pub fn register_provide_buffer(&mut self, sqe: SqEntry) -> Result<()> {
        self.push_sqe_without_waker_or_user_data(sqe)
    }

    fn push_sqe_without_waker_or_user_data(&mut self, sqe: SqEntry) -> Result<()> {
        loop {
            unsafe {
                let result = self.ring.submission().push(&sqe);
                self.ring.submit()?;

                if result.is_ok() {
                    return Ok(());
                }
            }
        }
    }
}

thread_local! {
    pub static DRIVER: RefCell<Option<Driver>> = RefCell::new(None);
}

#[must_use]
pub enum CancelCallback {
    CancelWithoutCallback { user_data: u64 },
    CancelAndRegisterBuffer { user_data: u64, group_id: u16 },
}
