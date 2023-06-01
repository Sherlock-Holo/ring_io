use std::io::Error;
use std::sync::Arc;
use std::thread;
use std::thread::yield_now;
use std::time::Duration;

use async_task::Runnable;
use flume::{Receiver, Selector, Sender};
use io_uring::opcode::Nop;
use io_uring::squeue::Entry;
use io_uring::types::{SubmitArgs, Timespec};
use io_uring::IoUring;
use sharded_slab::Slab;

use crate::operation::{Operation, OperationResult};

pub struct Driver {
    ops: Arc<Slab<Operation>>,
    sqe_buff: Sender<Entry>,
    task_receiver: Receiver<Runnable>,
    io: DriverIo,
}

struct DriverIo {
    ring: IoUring,
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
            ops: Arc::new(Default::default()),
            sqe_buff,
            task_receiver,
            io: DriverIo { ring, sqe_consume },
        }
    }

    pub fn main_thread_run_loop(&mut self, stop_notify: Receiver<()>) {
        const STOP: u64 = u64::MAX - 1;

        thread::scope(|scope| {
            let ring = &self.io.ring;
            let sqe_consume = &self.io.sqe_consume;

            let sq_push_thread = scope.spawn(move || {
                let mut pending_sqe = None;
                loop {
                    // Safety: we use sq only this thread
                    let mut sq = unsafe { ring.submission_shared() };

                    if let Some(sqe) = pending_sqe.take() {
                        if unsafe { sq.push(&sqe).is_err() } {
                            pending_sqe.replace(sqe);

                            match ring.submit() {
                                Err(err) => {
                                    if submit_err_can_ignore(&err) {
                                        continue;
                                    }

                                    panic!("submit panic {err}");
                                }

                                Ok(_) => continue,
                            }
                        }
                    }

                    let sqe = match Selector::new()
                        .recv(&stop_notify, |result| match result {
                            Ok(_) => unreachable!(),
                            Err(_) => None,
                        })
                        .recv(sqe_consume, |result| result.ok())
                        .wait()
                    {
                        None => {
                            let stop_sqe = Nop::new().build().user_data(STOP);

                            loop {
                                sq.sync();

                                if unsafe { sq.push(&stop_sqe).is_ok() } {
                                    if let Err(err) = ring.submit() {
                                        if !submit_err_can_ignore(&err) {
                                            panic!("submit panic {err}");
                                        }
                                    }

                                    return;
                                }

                                if let Err(err) = ring.submit() {
                                    if submit_err_can_ignore(&err) {
                                        continue;
                                    }

                                    panic!("submit panic {err}");
                                }
                            }
                        }
                        Some(sqe) => sqe,
                    };

                    if unsafe { sq.push(&sqe).is_err() } {
                        pending_sqe.replace(sqe);
                    }

                    if let Err(err) = ring.submit() {
                        if !submit_err_can_ignore(&err) {
                            panic!("submit panic {err}");
                        }
                    }
                }
            });

            let ops = &self.ops;
            let cq_consume_thread = scope.spawn(move || loop {
                let timespec = Timespec::new().nsec(Duration::from_millis(10).as_nanos() as _);
                let args = SubmitArgs::new().timespec(&timespec);
                if let Err(err) = ring.submitter().submit_with_args(1, &args) {
                    if submit_err_can_ignore(&err) {
                        continue;
                    }

                    panic!("submit panic {err}");
                }

                // Safety: we only use cq this thread
                let cq = unsafe { ring.completion_shared() };
                for cqe in cq {
                    let user_data = cqe.user_data();
                    if user_data == STOP {
                        return;
                    }

                    let op = match ops.take(user_data as _) {
                        None => continue,
                        Some(op) => op,
                    };

                    let operation_result = OperationResult::new(&cqe);
                    op.send_result(operation_result);
                }
            });

            for join_handle in [sq_push_thread, cq_consume_thread] {
                join_handle.join().unwrap();
            }
        })
    }
}

fn submit_err_can_ignore(err: &Error) -> bool {
    matches!(err.raw_os_error(), Some(libc::ETIME) | Some(libc::EINTR))
}

#[derive(Clone)]
pub struct WorkerDriver {
    ops: Arc<Slab<Operation>>,
    sqe_buff: Sender<Entry>,
    task_receiver: Receiver<Runnable>,
    close_notified: Receiver<()>,
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
}
