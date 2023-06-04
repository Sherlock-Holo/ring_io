use std::io;
use std::sync::Arc;
use std::thread::yield_now;
use std::time::Duration;

use async_task::Runnable;
use flume::{Receiver, Selector, Sender};
use io_uring::squeue::Entry;
use io_uring::types::{SubmitArgs, Timespec};
use io_uring::{cqueue, IoUring, Submitter};
use sharded_slab::Slab;

use crate::operation::{Operation, OperationResult};

pub struct Driver {
    ops: Arc<Slab<Operation>>,
    sqe_buff: Sender<Entry>,
    task_receiver: Receiver<Runnable>,
    io: DriverIo,
}

struct DriverIo {
    ring: Arc<IoUring>,
    pending_sqe: Option<Entry>,
    sqe_consume: Receiver<Entry>,
}

impl Driver {
    pub fn worker_driver(&self, close_notified: Receiver<()>) -> WorkerDriver {
        WorkerDriver {
            ops: self.ops.clone(),
            sqe_buff: self.sqe_buff.clone(),
            task_receiver: self.task_receiver.clone(),
            close_notified,
            ring: self.io.ring.clone(),
        }
    }

    pub fn new(ring: IoUring, task_receiver: Receiver<Runnable>) -> Self {
        let (sqe_buff, sqe_consume) = flume::unbounded();

        Self {
            ops: Arc::new(Default::default()),
            sqe_buff,
            task_receiver,
            io: DriverIo {
                ring: Arc::new(ring),
                pending_sqe: None,
                sqe_consume,
            },
        }
    }

    pub fn main_thread_run(&mut self) -> io::Result<()> {
        const MAX_TASK_ONCE: usize = 61;

        for task in self.task_receiver.try_iter().take(MAX_TASK_ONCE) {
            task.run();
        }

        let buff_sqes = self
            .io
            .pending_sqe
            .take()
            .into_iter()
            .chain(self.io.sqe_consume.try_iter());
        for sqe in buff_sqes {
            // Safety: we use sq only in main/io thread
            unsafe {
                if self.io.ring.submission_shared().push(&sqe).is_err() {
                    self.io.ring.submit()?;

                    if self.io.ring.submission_shared().push(&sqe).is_err() {
                        self.io.pending_sqe.replace(sqe);

                        break;
                    }
                }
            }
        }

        let timespec = Timespec::new().nsec(Duration::from_millis(10).as_nanos() as _);
        let submit_args = SubmitArgs::new().timespec(&timespec);
        if let Err(err) = self.io.ring.submitter().submit_with_args(1, &submit_args) {
            if matches!(err.raw_os_error(), Some(libc::ETIME) | Some(libc::EINTR)) {
                return Ok(());
            }

            return Err(err);
        }

        // Safety: we use cq only in main/io thread
        let completion_queue = unsafe { self.io.ring.completion_shared() };
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

#[derive(Clone)]
pub struct WorkerDriver {
    ops: Arc<Slab<Operation>>,
    sqe_buff: Sender<Entry>,
    task_receiver: Receiver<Runnable>,
    close_notified: Receiver<()>,
    ring: Arc<IoUring>,
}

impl WorkerDriver {
    pub fn submit(&self, mut entry: Entry, operation: Operation) -> u64 {
        let vacant = loop {
            match self.ops.vacant_entry() {
                None => {
                    yield_now();
                }
                Some(vacant) => break vacant,
            }
        };

        let user_data = vacant.key() as _;
        entry = entry.user_data(user_data);
        vacant.insert(operation);

        self.sqe_buff.send(entry).unwrap();

        user_data
    }

    pub fn worker_thread_run(self) {
        while let Some(task) = Selector::new()
            .recv(&self.close_notified, |result| match result {
                Err(_) => None,
                Ok(_) => unreachable!("close notify won't send anything"),
            })
            .recv(&self.task_receiver, |result| result.ok())
            .wait()
        {
            task.run();
        }
    }

    pub fn submitter(&self) -> Submitter {
        self.ring.submitter()
    }
}
