use std::sync::Mutex;
use std::{io, thread};

use async_task::Runnable;
use flume::Receiver;
use io_uring::squeue::Entry;
use io_uring::IoUring;
use slab::Slab;

use crate::operation::{Operation, OperationResult};

pub struct Driver {
    ring: Mutex<DriverRing>,
    task_receiver: Receiver<Runnable>,
}

impl Driver {
    pub fn new(task_receiver: Receiver<Runnable>) -> io::Result<Self> {
        let ring = IoUring::new(256)?;
        let ops = Slab::new();

        Ok(Self {
            ring: Mutex::new(DriverRing { ring, ops }),
            task_receiver,
        })
    }
}

struct DriverRing {
    ring: IoUring,
    ops: Slab<Operation>,
}

impl Driver {
    pub fn submit(&self, mut entry: Entry, operation: Operation) -> io::Result<u64> {
        let mut ring = self.ring.lock().unwrap();
        let user_data = ring.ops.insert(operation);
        entry = entry.user_data(user_data as _);

        // Safety: we will make sure related resource won't be released before operation done
        unsafe { while ring.ring.submission().push(&entry).is_err() {} }

        ring.ring.submit()?;

        Ok(user_data as _)
    }

    pub fn run(&self) -> io::Result<()> {
        const MAX_TASK_ONCE: usize = 64;

        for task in self.task_receiver.try_iter().take(MAX_TASK_ONCE) {
            task.run();
        }

        let mut ring = self.ring.lock().unwrap();
        let ring = &mut *ring;
        ring.ring.submit()?;

        let mut completion_queue = ring.ring.completion();
        completion_queue.sync();

        if completion_queue.is_empty() {
            thread::yield_now();

            return Ok(());
        }

        for cqe in completion_queue {
            let user_data = cqe.user_data();
            let op = &mut ring.ops[user_data as _];

            let operation_result = OperationResult::new(&cqe);
            op.send_result(operation_result);

            ring.ops.remove(user_data as _);
        }

        Ok(())
    }
}
