use std::collections::VecDeque;
use std::io::{Error, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Waker;
use std::time::Duration;

use io_uring::cqueue::{buffer_select, Entry as CqEntry};
use io_uring::opcode::ProvideBuffers;
use io_uring::squeue::Entry as SqEntry;
use io_uring::types::{SubmitArgs, Timespec};
use nix::unistd;
use parking_lot::Mutex;

use crate::buffer::{BufferManager, GroupBufferRegisterState};
use crate::callback::{Callback, CallbacksAndCompleteEntries};
use crate::cqe_ext::EntryExt;
use crate::driver::Driver;
use crate::owned_ring::{CompletionUring, SubmissionUring, SubmitterUring};

pub struct Reactor {
    // use to generate user data for new sqe
    next_user_data: Arc<AtomicU64>,

    // use to submit background sqe
    sq: Arc<Mutex<SubmissionUring>>,
    submitter: SubmitterUring,

    cq: CompletionUring,

    // the op future will store the waker here, if it needs some resources but they're not ready
    wait_for_push_wakers: Arc<Mutex<VecDeque<Waker>>>,

    // use to set GroupBuffer available, or release the buffer
    buffer_manager: Arc<Mutex<BufferManager>>,

    pub(crate) callbacks_and_complete_entries: Arc<Mutex<CallbacksAndCompleteEntries>>,

    // use to take out the background sqes, such as ProvideBuffer sqe
    background_sqes: Arc<Mutex<VecDeque<SqEntry>>>,
}

impl Reactor {
    pub(crate) fn new(driver: &Driver, submitter: SubmitterUring, cq: CompletionUring) -> Self {
        Self {
            next_user_data: driver.next_user_data.clone(),
            sq: driver.sq.clone(),
            submitter,
            cq,
            wait_for_push_wakers: driver.wait_for_push_wakers.clone(),
            buffer_manager: driver.buffer_manager.clone(),
            callbacks_and_complete_entries: driver.callbacks_and_complete_entries.clone(),
            background_sqes: driver.background_sqes.clone(),
        }
    }

    /// wakeup all waiting wakers, try to push all background sqes and submit them, then try to
    /// acquire one cqe and handle it, if one cqe is acquired, return Ok(true)
    pub fn try_run_one(&mut self) -> Result<bool> {
        self.try_wakeup_all_waiting_wakers();

        self.try_push_all_background_sqes()?;

        self.submitter.submitter().submit()?;

        /*if let Some(cqe) = self.cq.completion().next() {
            self.handle_cqe(cqe);

            Ok(true)
        } else {
            Ok(false)
        }*/
        let cqe = self.cq.completion().next();
        if let Some(cqe) = cqe {
            self.handle_cqe(cqe);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// wakeup all waiting wakers, try to push all background sqes and submit them, then try to
    /// acquire all available cqes and handle them, return cqe count
    pub fn try_run(&mut self) -> Result<usize> {
        self.try_wakeup_all_waiting_wakers();

        self.try_push_all_background_sqes()?;

        self.submitter.submitter().submit()?;

        let cq = self.cq.completion().collect::<Vec<_>>();
        if cq.is_empty() {
            return Ok(0);
        }

        let cqe_count = cq.len();

        for cqe in cq {
            self.handle_cqe(cqe);
        }

        Ok(cqe_count)
    }

    /// wakeup all waiting wakers, try to push all background sqes and submit them, then acquire at
    /// least one cqe and handle it, if no cqe is available, will block the thread unitl timeout,
    /// if timeout is set
    pub fn run_at_least_one<T: Into<Option<Duration>>>(&mut self, timeout: T) -> Result<usize> {
        self.try_wakeup_all_waiting_wakers();

        self.try_push_all_background_sqes()?;

        if let Some(timeout) = timeout.into() {
            let second = timeout.as_secs();
            let subsec_nanos = timeout.subsec_nanos();
            let timespec = Timespec::new().sec(second).nsec(subsec_nanos);

            let submit_args = SubmitArgs::new().timespec(&timespec);

            self.submitter
                .submitter()
                .submit_with_args(1, &submit_args)?;
        } else {
            self.submitter.submitter().submit_and_wait(1)?;
        }

        let cq = self.cq.completion().collect::<Vec<_>>();
        if cq.is_empty() {
            return Ok(0);
        }

        let cqe_count = cq.len();

        for cqe in cq {
            self.handle_cqe(cqe);
        }

        Ok(cqe_count)
    }

    fn try_wakeup_all_waiting_wakers(&mut self) {
        for waker in self.wait_for_push_wakers.lock().drain(..) {
            waker.wake();
        }
    }

    fn try_push_all_background_sqes(&mut self) -> Result<()> {
        let mut sq = self.sq.lock();
        let mut background_sqes = self.background_sqes.lock();

        while let Some(sqe) = background_sqes.pop_front() {
            unsafe {
                if sq.submission().push(&sqe).is_err() {
                    // push back the sqe, because it isn't consumed
                    if let Err(err) = self.submitter.submitter().submit() {
                        background_sqes.push_front(sqe);

                        return Err(err);
                    }

                    if sq.submission().push(&sqe).is_err() {
                        background_sqes.push_front(sqe);

                        break;
                    }
                }
            }
        }

        self.submitter.submitter().submit()?;

        Ok(())
    }

    fn handle_cqe(&mut self, cqe: CqEntry) {
        let user_data = cqe.user_data();

        // means this cqe is a cancel cqe
        if user_data == 0 {
            return;
        }

        let result = cqe.result();

        let mut callbacks_and_complete_entries = self.callbacks_and_complete_entries.lock();

        // this cqe is canceled
        if result == -libc::ECANCELED {
            if let Some(callback) = callbacks_and_complete_entries
                .callbacks
                .remove(&cqe.user_data())
            {
                match callback {
                    Callback::ProvideBuffer { .. } => {
                        unreachable!("ProvideBuffers event won't be canceled")
                    }

                    // at normal, a event with wakeup callback won't be canceled without
                    // runtime help, but wake up the waker may be a good idea
                    Callback::Wakeup { waker } => {
                        callbacks_and_complete_entries
                            .completion_queue_entries
                            .insert(cqe.user_data(), cqe);

                        waker.wake();
                    }

                    // a read event is canceled
                    Callback::CancelRead { group_id } => {
                        if let Some(buffer_id) = buffer_select(cqe.flags()) {
                            self.give_back_buffer_with_id(
                                group_id,
                                buffer_id,
                                &mut callbacks_and_complete_entries,
                            );
                        }
                    }

                    Callback::CancelConnect { addr: _, fd } => {
                        let _ = unistd::close(fd);
                    }

                    // no need to do anything
                    Callback::CancelOpenAt { .. }
                    | Callback::CancelStatx { .. }
                    | Callback::CancelRenameAt { .. }
                    | Callback::CancelUnlinkAt { .. }
                    | Callback::CancelTimeout { .. }
                    | Callback::CancelAccept { .. } => {}
                }

                return;
            }
        }

        if let Some(callback) = callbacks_and_complete_entries
            .callbacks
            .remove(&cqe.user_data())
        {
            match callback {
                Callback::ProvideBuffer { group_id } => {
                    if cqe.is_err() {
                        panic!(
                            "unexpect error for ProvideBuffers {}, group_id {}, user_data {}",
                            Error::from_raw_os_error(-cqe.result()),
                            group_id,
                            user_data
                        );
                    }

                    let mut buffer_manager = self.buffer_manager.lock();

                    let group_buffer = buffer_manager
                        .group_buffer_mut(group_id)
                        .unwrap_or_else(|| panic!("group buffer {} not exist", group_id));

                    if group_buffer.register_state() == GroupBufferRegisterState::Registering {
                        group_buffer.set_can_be_selected();
                    } else {
                        group_buffer.increase_available();
                    }
                }

                Callback::Wakeup { waker } => {
                    callbacks_and_complete_entries
                        .completion_queue_entries
                        .insert(cqe.user_data(), cqe);

                    waker.wake();
                }

                // an ready read event is canceled
                Callback::CancelRead { group_id } => {
                    if let Some(buffer_id) = buffer_select(cqe.flags()) {
                        self.give_back_buffer_with_id(
                            group_id,
                            buffer_id,
                            &mut callbacks_and_complete_entries,
                        );
                    }
                }

                Callback::CancelConnect { addr: _, fd } => {
                    let _ = unistd::close(fd);
                }

                Callback::CancelAccept { .. } => {
                    // a stream is accepted
                    if cqe.result() > 0 {
                        let _ = unistd::close(cqe.result());
                    }
                }

                Callback::CancelOpenAt { .. } => {
                    // a file is opened
                    if cqe.result() > 0 {
                        let _ = unistd::close(cqe.result());
                    }
                }

                // no need to do
                Callback::CancelStatx { .. }
                | Callback::CancelRenameAt { .. }
                | Callback::CancelUnlinkAt { .. }
                | Callback::CancelTimeout { .. } => {}
            }
        }
    }

    fn give_back_buffer_with_id(
        &self,
        group_id: u16,
        buffer_id: u16,
        callbacks_and_complete_entries: &mut CallbacksAndCompleteEntries,
    ) {
        let mut buffer_manager = self.buffer_manager.lock();

        let group_buffer = buffer_manager
            .group_buffer_mut(group_id)
            .unwrap_or_else(|| panic!("group buffer {} not found", group_id));

        group_buffer.decrease_on_fly();

        // the buffer is selected, so decrease available
        group_buffer.decrease_available();

        let ptr_mut = group_buffer.low_level_buffer_by_buffer_id(buffer_id);
        let len = group_buffer.every_buf_size();

        let user_data = self.next_user_data.fetch_add(1, Ordering::Relaxed);

        let provide_buffer_sqe = ProvideBuffers::new(ptr_mut, len as _, 1, group_id, buffer_id)
            .build()
            .user_data(user_data);

        callbacks_and_complete_entries
            .callbacks
            .insert(user_data, Callback::ProvideBuffer { group_id });

        self.background_sqes.lock().push_back(provide_buffer_sqe);
    }
}
