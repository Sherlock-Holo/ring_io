use std::cell::RefCell;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use async_task::Runnable;
use flume::{Receiver, Sender};
use io_uring::Builder;

use crate::buf;
use crate::per_thread::driver::PerThreadDriver;

thread_local! {
    static DRIVER: OnceLock<RefCell<PerThreadDriver>> = OnceLock::new();
}

pub enum TaskReceive {
    Runnable(Runnable),
    RegisterBufRing(buf::Builder, Sender<io::Result<()>>),
    UnRegisterBufRing(u16),
}

pub struct PerThreadRuntime {
    task_receiver: Receiver<TaskReceive>,
    builder: Builder,
    entries: u32,
}

impl PerThreadRuntime {
    /// new won't create io_uring instance
    pub fn new(builder: Builder, entries: u32, task_receiver: Receiver<TaskReceive>) -> Self {
        Self {
            task_receiver,
            builder,
            entries,
        }
    }

    pub fn run(&mut self, shutdown: Arc<AtomicBool>) {
        const MAX_TASK_ONCE: usize = 61;

        init_driver(&self.builder, self.entries);

        loop {
            for task_receive in self.task_receiver.try_iter().take(MAX_TASK_ONCE) {
                match task_receive {
                    TaskReceive::Runnable(runnable) => {
                        runnable.run();
                    }

                    TaskReceive::RegisterBufRing(builder, result_sender) => {
                        let result =
                            with_driver(|driver| match builder.build(&driver.submitter()) {
                                Ok(buf_ring) => {
                                    driver.add_fix_sized_buf_ring(builder.bgid(), buf_ring);

                                    Ok(())
                                }
                                Err(err) => Err(err),
                            });

                        let _ = result_sender.try_send(result);
                    }

                    TaskReceive::UnRegisterBufRing(bgid) => {
                        with_driver(|driver| driver.delete_fix_sized_buf_ring(bgid))
                    }
                }
            }

            with_driver(|driver| {
                driver.submit()?;
                driver.run_io()
            })
            .expect("driver submit failed");

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
    // for<'a> F: FnOnce(&'a mut PerThreadDriver) -> T + 'a,
    F: FnOnce(&mut PerThreadDriver) -> T,
{
    DRIVER.with(|driver| {
        let driver = driver.get().expect("PerThreadDriver not init");

        f(&mut driver.borrow_mut())
    })
}
