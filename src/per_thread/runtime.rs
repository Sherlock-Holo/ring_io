use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use async_task::Runnable;
use flume::Receiver;
use io_uring::Builder;

use super::IoUringModify;
use crate::per_thread::driver::PerThreadDriver;

thread_local! {
    static DRIVER: OnceLock<RefCell<PerThreadDriver>> = OnceLock::new();
}

pub struct PerThreadRuntime {
    task_receiver: Receiver<Runnable>,
    io_uring_modify_receiver: Receiver<IoUringModify>,
    builder: Builder,
    entries: u32,
}

impl PerThreadRuntime {
    /// new won't create io_uring instance
    pub fn new(
        builder: Builder,
        entries: u32,
        task_receiver: Receiver<Runnable>,
        io_uring_modify_receiver: Receiver<IoUringModify>,
    ) -> Self {
        Self {
            task_receiver,
            io_uring_modify_receiver,
            builder,
            entries,
        }
    }

    pub fn run(&mut self, shutdown: Arc<AtomicBool>) {
        const MAX_TASK_ONCE: usize = 56; // 61-5
        const MAX_IO_URING_MODIFY_ONCE: usize = 5;

        init_driver(&self.builder, self.entries);

        loop {
            for runnable in self.task_receiver.try_iter().take(MAX_TASK_ONCE) {
                runnable.run();
            }

            with_driver(|driver| {
                for modify in self
                    .io_uring_modify_receiver
                    .try_iter()
                    .take(MAX_IO_URING_MODIFY_ONCE)
                {
                    match modify {
                        IoUringModify::RegisterBufRing(builder, result_sender) => {
                            let result = match builder.build(&driver.submitter()) {
                                Ok(buf_ring) => {
                                    driver.add_fix_sized_buf_ring(builder.bgid(), buf_ring);

                                    Ok(())
                                }
                                Err(err) => Err(err),
                            };

                            let _ = result_sender.try_send(result);
                        }

                        IoUringModify::UnRegisterBufRing(bgid, result_sender) => {
                            let result = driver.delete_fix_sized_buf_ring(bgid);
                            let _ = result_sender.try_send(result);
                        }
                    }
                }

                driver.run_io().expect("driver submit failed");
            });

            if shutdown.load(Ordering::Acquire) {
                return;
            }
        }
    }
}

/// init_driver allow multi call, which will only init once
fn init_driver(builder: &Builder, entries: u32) {
    DRIVER.with(|driver| {
        driver.get_or_init(|| {
            let io_uring = builder
                .build(entries)
                .unwrap_or_else(|err| panic!("init io_uring failed: {err}"));

            RefCell::new(PerThreadDriver::new(io_uring))
        });
    });
}

pub(crate) fn in_per_thread_runtime() -> bool {
    DRIVER.with(|driver| driver.get().is_some())
}

pub(crate) fn with_driver<T, F>(f: F) -> T
where
    F: FnOnce(&mut PerThreadDriver) -> T,
{
    DRIVER.with(|driver| {
        let driver = driver.get().expect("PerThreadDriver not init");

        f(&mut driver.borrow_mut())
    })
}
