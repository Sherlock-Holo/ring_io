use std::future::Future;
use std::mem::ManuallyDrop;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::{available_parallelism, JoinHandle};
use std::{io, thread};

use async_task::Runnable;
use flume::Sender;
pub use io_uring::Builder;
use io_uring::IoUring;

use crate::buf;
use crate::per_thread::runtime::PerThreadRuntime;
use crate::per_thread::IoUringModify;
pub use crate::thread_pool::spawn_blocking;
use crate::thread_pool::{set_thread_pool, ThreadPool};

const ENTRIES: u32 = 1024;

pub type Task<T> = async_task::Task<T>;

thread_local! {
    static IO_URING_MODIFY_SENDERS: OnceLock<Arc<[Sender<IoUringModify>]>> = OnceLock::new();
    static TASK_SENDER: OnceLock<Sender<Runnable>> = OnceLock::new();
}

pub struct Runtime {
    task_sender: Sender<Runnable>,
    shutdown: Arc<AtomicBool>,
    worker_threads: Vec<JoinHandle<()>>,
    thread_pool: ManuallyDrop<ThreadPool>,
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

        let mut io_uring_modify_senders = Vec::with_capacity(threads);
        let mut io_uring_modify_receivers = Vec::with_capacity(threads);
        for _ in 0..threads {
            let (sender, receiver) = flume::unbounded();
            io_uring_modify_senders.push(sender);
            io_uring_modify_receivers.push(receiver);
        }

        let io_uring_modify_senders: Arc<[Sender<_>]> = io_uring_modify_senders.into();
        let (task_sender, task_receiver) = flume::unbounded();

        let thread_pool = {
            let io_uring_modify_senders = io_uring_modify_senders.clone();
            let task_sender = task_sender.clone();

            ThreadPool::new(move || {
                let io_uring_modify_senders = io_uring_modify_senders.clone();
                let task_sender = task_sender.clone();

                IO_URING_MODIFY_SENDERS.with(|senders| {
                    senders.set(io_uring_modify_senders).unwrap();
                });
                TASK_SENDER.with(|sender| {
                    sender.set(task_sender).unwrap();
                });
            })
        };

        for io_uring_modify_receiver in io_uring_modify_receivers {
            let thread_pool = thread_pool.clone();

            let mut per_thread_runtime = PerThreadRuntime::new(
                builder.clone(),
                ENTRIES,
                task_receiver.clone(),
                io_uring_modify_receiver,
            );
            let shutdown = shutdown.clone();
            let io_uring_modify_senders = io_uring_modify_senders.clone();
            let task_sender = task_sender.clone();

            let join_handle = thread::spawn(move || {
                // set io related TLS
                IO_URING_MODIFY_SENDERS.with(|senders| {
                    senders.set(io_uring_modify_senders).unwrap();
                });
                TASK_SENDER.with(|sender| {
                    sender.set(task_sender).unwrap();
                });

                // set spawn_blocking related TLS
                set_thread_pool(thread_pool);

                per_thread_runtime.run(shutdown)
            });
            worker_threads.push(join_handle);
        }

        Self {
            task_sender,
            shutdown,
            worker_threads,
            thread_pool: ManuallyDrop::new(thread_pool),
        }
    }

    pub fn block_on<F>(&mut self, fut: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let sender = self.task_sender.clone();
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

        // Safety: only drop once
        // this is used to stop all spawn_blocking task
        unsafe {
            ManuallyDrop::drop(&mut self.thread_pool);
        }

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
    let result_receivers = IO_URING_MODIFY_SENDERS.with(|senders| {
        let senders = senders.get().expect("not in ring_io context");
        let mut result_receivers = Vec::with_capacity(senders.len());
        for sender in senders.iter() {
            let (result_sender, result_receiver) = flume::bounded(1);
            sender
                .send(IoUringModify::RegisterBufRing(builder, result_sender))
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

pub async fn unregister_buf_ring(bgid: u16) -> io::Result<()> {
    let result_receivers = IO_URING_MODIFY_SENDERS.with(|senders| {
        let senders = senders.get().expect("not in ring_io context");
        let mut result_receivers = Vec::with_capacity(senders.len());
        for sender in senders.iter() {
            let (result_sender, result_receiver) = flume::bounded(1);
            sender
                .send(IoUringModify::UnRegisterBufRing(bgid, result_sender))
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

pub fn spawn<F>(fut: F) -> Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let sender = TASK_SENDER.with(|sender| sender.get().expect("not in ring_io context").clone());

    inner_spawn(fut, sender)
}

fn inner_spawn<F>(fut: F, sender: Sender<Runnable>) -> Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (runnable, task) = async_task::spawn(fut, move |runnable| {
        sender.send(runnable).unwrap();
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

    #[test]
    fn test_blocking() {
        block_on(async move {
            let start = Instant::now();

            spawn_blocking(|| thread::sleep(Duration::from_secs(1))).await;

            let duration = start.elapsed();
            assert_eq!(duration.as_secs(), 1);
        })
    }

    #[test]
    fn test_blocking_inner_spawn() {
        block_on(async move {
            let start = Instant::now();

            spawn_blocking(|| spawn(async move { Delay::new(Duration::from_secs(1)).await }))
                .await
                .await;

            let duration = start.elapsed();
            assert_eq!(duration.as_secs(), 1);
        })
    }

    #[test]
    fn test_blocking_inner_blocking() {
        block_on(async move {
            let start = Instant::now();

            spawn_blocking(|| spawn_blocking(|| thread::sleep(Duration::from_secs(1))))
                .await
                .await;

            let duration = start.elapsed();
            assert_eq!(duration.as_secs(), 1);
        })
    }
}
