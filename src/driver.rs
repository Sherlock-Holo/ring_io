use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_task::Runnable;
use flume::{Receiver, Selector, Sender};
use io_uring::squeue::Entry;
use io_uring::types::{SubmitArgs, Timespec};
use io_uring::IoUring;
use slab::Slab;

use crate::operation::{Operation, OperationResult};

pub struct Driver {
    ops: Arc<Mutex<Slab<Operation>>>,
    sqe_buff: Sender<Entry>,
    task_receiver: Receiver<Runnable>,
    io: DriverIo,
}

struct DriverIo {
    ring: IoUring,
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
        }
    }

    pub fn new(ring: IoUring, task_receiver: Receiver<Runnable>) -> Self {
        let (sqe_buff, sqe_consume) = flume::unbounded();

        Self {
            ops: Arc::new(Mutex::new(Default::default())),
            sqe_buff,
            task_receiver,
            io: DriverIo {
                ring,
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
            unsafe {
                if self.io.ring.submission().push(&sqe).is_err() {
                    self.io.ring.submit()?;

                    if self.io.ring.submission().push(&sqe).is_err() {
                        self.io.pending_sqe.replace(sqe);

                        break;
                    }
                }
            }
        }

        let timespec = Timespec::new().nsec(Duration::from_millis(10).as_nanos() as _);
        let submit_args = SubmitArgs::new().timespec(&timespec);
        match self.io.ring.submitter().submit_with_args(1, &submit_args) {
            Err(err) if err.raw_os_error() == Some(libc::ETIME) => {
                return Ok(());
            }

            Err(err) => return Err(err),
            Ok(_) => {}
        }

        let completion_queue = self.io.ring.completion();
        let mut ops = self.ops.lock().unwrap();
        for cqe in completion_queue {
            let user_data = cqe.user_data();
            let op = &mut ops[user_data as _];

            let operation_result = OperationResult::new(&cqe);
            op.send_result(operation_result);

            ops.remove(user_data as _);
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct WorkerDriver {
    ops: Arc<Mutex<Slab<Operation>>>,
    sqe_buff: Sender<Entry>,
    task_receiver: Receiver<Runnable>,
    close_notified: Receiver<()>,
}

impl WorkerDriver {
    pub fn submit(&self, mut entry: Entry, operation: Operation) -> u64 {
        let user_data = self.ops.lock().unwrap().insert(operation);
        entry = entry.user_data(user_data as _);

        self.sqe_buff.send(entry).unwrap();

        user_data as _
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
}
