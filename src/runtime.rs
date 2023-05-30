use std::future::Future;
use std::io;
use std::rc::Rc;
use std::task::{Context, Poll};

use async_task::Runnable;
use flume::{Receiver, Sender};
use futures_util::task::noop_waker_ref;
use futures_util::FutureExt;
use io_uring::squeue::Entry;
pub use io_uring::Builder;
use io_uring::IoUring;
use once_cell::sync::Lazy;

use crate::driver::Driver;
use crate::operation::Operation;

static TX_RX: Lazy<(Sender<Runnable>, Receiver<Runnable>)> = Lazy::new(flume::unbounded);

pub type Task<T> = async_task::Task<T>;

thread_local! {
    static RUNTIME: std::cell::RefCell<Option<Runtime>> = std::cell::RefCell::new(None);
}

pub struct Runtime {
    driver: Rc<Driver>,
}

impl Runtime {
    pub(crate) fn submit(&self, entry: Entry, operation: Operation) -> io::Result<u64> {
        self.driver.submit(entry, operation)
    }
}

pub fn block_on<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    block_on_with_io_uring_builder(fut, &create_io_uring_builder())
}

pub fn block_on_with_io_uring_builder<F>(fut: F, builder: &Builder) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let driver = RUNTIME.with(|runtime| {
        let mut runtime = runtime.borrow_mut();
        if runtime.is_some() {
            panic!("can't call block_on nested");
        }

        let task_receiver = TX_RX.1.clone();
        let driver = Driver::new_with_io_uring_builder(task_receiver, builder).unwrap();

        let driver = Rc::new(driver);

        *runtime = Some(Runtime {
            driver: driver.clone(),
        });

        driver
    });

    let (runnable, mut task) = async_task::spawn(fut, schedule);
    runnable.schedule();

    let output = loop {
        driver
            .run()
            .unwrap_or_else(|err| panic!("driver panic {err}"));

        if task.is_finished() {
            match task.poll_unpin(&mut Context::from_waker(noop_waker_ref())) {
                Poll::Ready(output) => break output,
                Poll::Pending => unreachable!("finished task return pending"),
            }
        }
    };

    RUNTIME.with(|runtime| {
        runtime.borrow_mut().take();
    });

    output
}

pub fn spawn<F>(fut: F) -> Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (runnable, task) = async_task::spawn(fut, schedule);
    runnable.schedule();

    task
}

pub fn create_io_uring_builder() -> Builder {
    IoUring::builder()
}

pub(crate) fn in_ring_io_context() -> bool {
    RUNTIME.with(|runtime| runtime.borrow().is_some())
}

pub(crate) fn with_runtime<T, F: FnOnce(&Runtime) -> T>(f: F) -> T {
    RUNTIME.with(|runtime| {
        let runtime = runtime.borrow();
        let runtime = runtime.as_ref().expect("runtime not init");

        f(runtime)
    })
}

fn schedule(runnable: Runnable) {
    TX_RX.0.send(runnable).unwrap();
}

// all test must create a new thread, because block_on can't call nested
#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::thread;
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

    #[test]
    fn multi_thread() {
        thread::spawn(|| block_on(pending::<()>()));

        block_on(async move {
            let tasks = (0..10)
                .map(|_| spawn(async move { Delay::new(Duration::from_secs(1)).await }))
                .collect::<Vec<_>>();

            for task in tasks {
                task.await;
            }
        })
    }
}
