use std::collections::HashMap;
use std::io;
use std::time::Duration;

use io_uring::squeue::Entry;
use io_uring::types::{SubmitArgs, Timespec};
use io_uring::{cqueue, IoUring, Submitter};
use slab::Slab;

use crate::buf::FixedSizeBufRing;
use crate::operation::{Operation, OperationResult};

pub struct PerThreadDriver {
    ring: IoUring,
    ops: Slab<Operation>,
    buf_rings: HashMap<u16, FixedSizeBufRing>,
}

impl PerThreadDriver {
    pub fn new(ring: IoUring) -> Self {
        Self {
            ring,
            ops: Default::default(),
            buf_rings: Default::default(),
        }
    }

    pub fn push_sqe(&mut self, mut entry: Entry, operation: Operation) -> io::Result<u64> {
        let vacant_entry = self.ops.vacant_entry();
        let user_data = vacant_entry.key() as u64;
        entry = entry.user_data(user_data);
        vacant_entry.insert(operation);

        loop {
            unsafe {
                if self.ring.submission().push(&entry).is_err() {
                    self.ring.submit()?;

                    continue;
                }

                break;
            }
        }

        Ok(user_data)
    }

    pub fn submit(&self) -> io::Result<()> {
        self.ring.submit()?;

        Ok(())
    }

    pub fn submitter(&self) -> Submitter {
        self.ring.submitter()
    }

    pub fn add_fix_sized_buf_ring(&mut self, bgid: u16, buf_ring: FixedSizeBufRing) {
        self.buf_rings.insert(bgid, buf_ring);
    }

    pub fn delete_fix_sized_buf_ring(&mut self, bgid: u16) {
        if let Some(buf_ring) = self.buf_rings.remove(&bgid) {
            let _ = buf_ring.unregister(&self.ring.submitter());
        }
    }

    pub fn buf_ring_ref(&self, bgid: u16) -> Option<&FixedSizeBufRing> {
        self.buf_rings.get(&bgid)
    }

    pub fn run_io(&mut self) -> io::Result<()> {
        let timespec = Timespec::new().nsec(Duration::from_millis(10).as_nanos() as _);
        let submit_args = SubmitArgs::new().timespec(&timespec);
        if let Err(err) = self.ring.submitter().submit_with_args(1, &submit_args) {
            if matches!(err.raw_os_error(), Some(libc::ETIME) | Some(libc::EINTR)) {
                return Ok(());
            }

            return Err(err);
        }

        let completion_queue = self.ring.completion();
        for cqe in completion_queue {
            let user_data = cqe.user_data();
            match self.ops.get(user_data as _) {
                None => continue,
                Some(op) => {
                    let operation_result = OperationResult::new(&cqe);
                    op.send_result(operation_result);
                }
            }

            if !cqueue::more(cqe.flags()) {
                self.ops.remove(user_data as _);
            }
        }

        Ok(())
    }
}
