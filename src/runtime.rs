use std::future::Future;
use std::io;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_task::{Runnable, Task};
use flume::{Receiver, Sender};
use futures_util::task::noop_waker_ref;
use futures_util::FutureExt;
use io_uring::squeue::Entry;
use once_cell::sync::Lazy;

use crate::driver::Driver;
use crate::operation::Operation;

static TX_RX: Lazy<(Sender<Runnable>, Receiver<Runnable>)> = Lazy::new(flume::unbounded);

thread_local! {
    static RUNTIME: std::cell::RefCell<Option<Runtime>> = std::cell::RefCell::new(None);
}

pub struct Runtime {
    driver: Arc<Driver>,
}

impl Runtime {
    pub fn submit(&self, entry: Entry, operation: Operation) -> io::Result<u64> {
        self.driver.submit(entry, operation)
    }
}

pub fn block_on<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let task_receiver = TX_RX.1.clone();
    let driver = Driver::new(task_receiver).unwrap();

    let driver = RUNTIME.with(|runtime| {
        let driver = Arc::new(driver);

        *runtime.borrow_mut() = Some(Runtime {
            driver: driver.clone(),
        });

        driver
    });

    let (runnable, mut task) = async_task::spawn(fut, schedule);
    runnable.schedule();

    let output = loop {
        driver.run().unwrap();

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
