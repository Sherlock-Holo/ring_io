use std::future::Future;
use std::num::NonZeroUsize;
use std::task::{Context, Poll};
use std::thread::{available_parallelism, JoinHandle};
use std::{io, thread};

use async_task::{Runnable, Schedule};
use flume::Sender;
use futures_util::task::noop_waker_ref;
use futures_util::FutureExt;
use io_uring::squeue::Entry;
use io_uring::{Builder, IoUring};

use crate::driver::{Driver, WorkerDriver};
use crate::operation::Operation;

pub type Task<T> = async_task::Task<T>;

thread_local! {
    static CONTEXT: std::cell::RefCell<Option<RuntimeContext>> = std::cell::RefCell::new(None);
}

pub struct Runtime {
    driver: Driver,
    threads: Vec<JoinHandle<()>>,
    close_notify: Option<Sender<()>>,
}

pub(crate) struct RuntimeContext {
    worker_driver: WorkerDriver,
    task_sender: Sender<Runnable>,
}

impl RuntimeContext {
    pub(crate) fn submit(&self, entry: Entry, operation: Operation) -> io::Result<u64> {
        Ok(self.worker_driver.submit(entry, operation))
    }
}

impl Runtime {
    pub fn new() -> io::Result<Self> {
        let threads = available_parallelism()
            .unwrap_or(NonZeroUsize::new(4).unwrap())
            .get();
        Self::new_with_io_uring_builder(&IoUring::builder(), threads)
    }

    pub fn new_with_io_uring_builder(builder: &Builder, threads: usize) -> io::Result<Self> {
        let io_uring = builder.build(1024)?;
        let (task_sender, task_receiver) = flume::unbounded();
        let (close_notify, close_notified) = flume::bounded(0);
        let driver = Driver::new(io_uring, task_receiver);

        // create worker threads
        let threads = (0..threads - 1)
            .map(|_| {
                let worker_driver = driver.worker_driver(close_notified.clone());
                let runtime_context = RuntimeContext {
                    worker_driver: worker_driver.clone(),
                    task_sender: task_sender.clone(),
                };

                thread::spawn(move || {
                    CONTEXT.with(|context| context.borrow_mut().replace(runtime_context));

                    // worker run task
                    worker_driver.worker_thread_run();
                })
            })
            .collect::<Vec<_>>();

        let runtime_context = RuntimeContext {
            worker_driver: driver.worker_driver(close_notified),
            task_sender,
        };

        // let main worker thread, also the IO thread can spawn thread too
        CONTEXT.with(|context| context.borrow_mut().replace(runtime_context));

        Ok(Self {
            driver,
            threads,
            close_notify: Some(close_notify),
        })
    }

    pub fn block_on<F>(&mut self, fut: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (runnable, mut task) = async_task::spawn(fut, get_scheduler());
        runnable.schedule();

        loop {
            self.driver
                .main_thread_run()
                .unwrap_or_else(|err| panic!("driver panic {err}"));

            if task.is_finished() {
                match task.poll_unpin(&mut Context::from_waker(noop_waker_ref())) {
                    Poll::Ready(output) => return output,
                    Poll::Pending => unreachable!("finished task return pending"),
                }
            }
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        CONTEXT.with(|context| context.borrow_mut().take());
        self.close_notify.take();

        for join_handle in self.threads.drain(..) {
            let _ = join_handle.join();
        }
    }
}

pub fn block_on<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    Runtime::new().unwrap().block_on(fut)
}

pub fn spawn<F>(fut: F) -> Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (runnable, task) = async_task::spawn(fut, get_scheduler());
    runnable.schedule();

    task
}

pub fn create_io_uring_builder() -> Builder {
    IoUring::builder()
}

fn get_scheduler() -> impl Schedule + Send + Sync + 'static {
    let task_sender = CONTEXT.with(|context| {
        context
            .borrow()
            .as_ref()
            .expect("not in ring io context")
            .task_sender
            .clone()
    });

    move |runnable| task_sender.send(runnable).unwrap()
}

pub(crate) fn with_runtime_context<T, F: FnOnce(&RuntimeContext) -> T>(f: F) -> T {
    CONTEXT.with(|context| {
        let context = context.borrow();
        let context = context.as_ref().expect("not in ring io context");

        f(context)
    })
}

pub(crate) fn in_ring_io_context() -> bool {
    CONTEXT.with(|context| context.borrow().as_ref().is_some())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use futures_timer::Delay;

    use super::*;

    #[test]
    fn test_block_on() {
        let n = block_on(async move { 1 });

        assert_eq!(n, 1);
    }

    #[test]
    fn test_block_on_with_timer() {
        let start = Instant::now();

        block_on(async move {
            Delay::new(Duration::from_secs(1)).await;
        });

        let duration = start.elapsed();
        assert_eq!(duration.as_secs(), 1);
    }

    #[test]
    fn test_spawn() {
        let n = block_on(async move { spawn(async move { 1 }).await });

        assert_eq!(n, 1);
    }

    #[test]
    fn test_spawn_with_timer() {
        let start = Instant::now();

        block_on(
            async move { spawn(async move { Delay::new(Duration::from_secs(1)).await }).await },
        );

        let duration = start.elapsed();
        assert_eq!(duration.as_secs(), 1);
    }
}
