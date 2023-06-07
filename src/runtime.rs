use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::{available_parallelism, JoinHandle};
use std::{io, thread};

use flume::Sender;
pub use io_uring::Builder;
use io_uring::IoUring;
use rand::prelude::{thread_rng, SliceRandom};

use crate::buf;
use crate::per_thread::runtime::{PerThreadRuntime, TaskReceive};

const ENTRIES: u32 = 1024;

pub type Task<T> = async_task::Task<T>;

thread_local! {
    static WORKER_SENDERS: OnceLock<Arc<[Sender<TaskReceive>]>> = OnceLock::new();
}

pub struct Runtime {
    worker_senders: Arc<[Sender<TaskReceive>]>,
    shutdown: Arc<AtomicBool>,
    worker_threads: Vec<JoinHandle<()>>,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime {
    pub fn new() -> Self {
        let threads = available_parallelism()
            .unwrap_or(NonZeroUsize::new(4).unwrap())
            .get();

        let mut builder = IoUring::builder();
        builder.dontfork();

        Self::new_with(builder, threads)
    }

    pub fn new_with(builder: Builder, threads: usize) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut worker_threads = Vec::with_capacity(threads);

        let mut worker_senders = Vec::with_capacity(threads);
        let mut task_receivers = Vec::with_capacity(threads);
        for _ in 0..threads {
            let (sender, receiver) = flume::unbounded();
            worker_senders.push(sender);
            task_receivers.push(receiver);
        }

        let worker_senders: Arc<[Sender<TaskReceive>]> = worker_senders.into();

        for task_receiver in task_receivers {
            let mut per_thread_runtime =
                PerThreadRuntime::new(builder.clone(), ENTRIES, task_receiver);
            let shutdown = shutdown.clone();
            let worker_senders = worker_senders.clone();

            let join_handle = thread::spawn(move || {
                WORKER_SENDERS.with(|senders| {
                    let _ = senders.set(worker_senders);
                });

                per_thread_runtime.run(shutdown)
            });
            worker_threads.push(join_handle);
        }

        Self {
            worker_senders,
            shutdown,
            worker_threads,
        }
    }

    pub fn block_on<F>(&mut self, fut: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let sender = self
            .worker_senders
            .choose(&mut thread_rng())
            .unwrap()
            .clone();

        let (output_sender, output_receiver) = flume::bounded(1);
        inner_spawn(
            async move {
                let output = fut.await;

                let _ = output_sender.send(output);
            },
            sender,
        )
        .detach();

        output_receiver.recv().unwrap()
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);

        for join_handle in self.worker_threads.drain(..) {
            let _ = join_handle.join();
        }
    }
}

pub fn block_on<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    Runtime::new().block_on(fut)
}

pub async fn register_buf_ring(builder: buf::Builder) -> io::Result<()> {
    let result_receivers = WORKER_SENDERS.with(|senders| {
        let senders = senders.get().expect("not in ring_io context");
        let mut result_receivers = Vec::with_capacity(senders.len());
        for sender in senders.iter() {
            let (result_sender, result_receiver) = flume::bounded(1);
            sender
                .send(TaskReceive::RegisterBufRing(builder, result_sender))
                .unwrap();
            result_receivers.push(result_receiver);
        }

        result_receivers
    });

    for receiver in result_receivers {
        receiver.recv_async().await.unwrap()?;
    }

    Ok(())
}

pub fn unregister_buf_ring(bgid: u16) {
    WORKER_SENDERS.with(|senders| {
        let senders = senders.get().expect("not in ring_io context");
        for sender in senders.iter() {
            sender.send(TaskReceive::UnRegisterBufRing(bgid)).unwrap();
        }
    });
}

pub fn spawn<F>(fut: F) -> Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let sender = WORKER_SENDERS.with(|senders| {
        let senders = senders.get().expect("not in ring_io context");
        senders.choose(&mut thread_rng()).unwrap().clone()
    });

    inner_spawn(fut, sender)
}

fn inner_spawn<F>(fut: F, sender: Sender<TaskReceive>) -> Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (runnable, task) = async_task::spawn(fut, move |runnable| {
        sender.send(TaskReceive::Runnable(runnable)).unwrap();
    });
    runnable.schedule();

    task
}

pub fn create_io_uring_builder() -> Builder {
    IoUring::builder()
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
