use std::cell::RefCell;
use std::future::Future;
use std::io::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

use async_task::{Runnable, Task as AsyncTask};
use crossbeam_channel::{Receiver, Sender, TryRecvError};
use futures_util::task::AtomicWaker;
use futures_util::FutureExt;
use io_uring::cqueue::buffer_select;
use nix::unistd;

use crate::buffer::GroupBufferRegisterState;
use crate::cqe_ext::EntryExt;
use crate::driver::{Callback, Driver, DRIVER};

thread_local! {
    static TASK_SENDER: RefCell<Option<Sender<Runnable>>> = RefCell::new(None);
    static TASK_RECEIVER: RefCell<Option<Receiver<Runnable>>> = RefCell::new(None);
}

#[derive(Debug, Clone, Default)]
pub struct Builder {
    sq_poll: Option<Duration>,
    sq_poll_cpu: Option<u32>,
    io_poll: bool,
}

impl Builder {
    pub fn sq_poll<T: Into<Option<Duration>>>(self, sq_poll: T) -> Self {
        Self {
            sq_poll: sq_poll.into(),
            ..self
        }
    }

    pub fn sq_poll_cpu<T: Into<Option<u32>>>(self, sq_poll_cpu: T) -> Self {
        Self {
            sq_poll_cpu: sq_poll_cpu.into(),
            ..self
        }
    }

    pub fn io_poll(self, io_poll: bool) -> Self {
        Self { io_poll, ..self }
    }

    pub fn build(self) -> Result<Runtime, Error> {
        let driver = Driver::new(self.sq_poll, self.sq_poll_cpu, self.io_poll)?;

        Ok(Runtime {
            driver: Some(driver),
        })
    }
}

pub struct Runtime {
    driver: Option<Driver>,
}

impl Runtime {
    pub fn builder() -> Builder {
        Default::default()
    }

    pub fn block_on<F>(&mut self, fut: F) -> F::Output
    where
        F: Future + 'static + Send,
        F::Output: 'static + Send,
    {
        const MAX_RUN_TASK_COUNT: u8 = 64;

        self.init_driver();
        self.init_task_sender_and_receiver();

        let (sender, receiver) = crossbeam_channel::bounded(1);

        spawn(async move {
            let result = fut.await;
            sender.send(result).unwrap();
        })
        .detach();

        loop {
            // if the main future is ready, return the result immediately
            if let Ok(result) = receiver.try_recv() {
                return result;
            }

            // if no task can run, it may need yield this thread
            let need_yield = TASK_RECEIVER.with(|task_receiver| {
                let mut task_receiver = task_receiver.borrow_mut();
                let task_receiver = task_receiver.as_mut().unwrap();

                // we can't run too many task in a round, it may starve the IO
                let available_run_task_count = MAX_RUN_TASK_COUNT;

                for i in 0..available_run_task_count {
                    match task_receiver.try_recv() {
                        Ok(runnable) => {
                            runnable.run();
                        }

                        Err(TryRecvError::Disconnected) => unreachable!(),
                        Err(TryRecvError::Empty) => {
                            return i == 0;
                        }
                    }
                }

                false
            });

            let empty_cq_and_no_wait_wakers = DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().unwrap();

                let cq = driver.ring.completion();

                if cq.is_empty() {
                    // let driver.try_run_all_wait_sqes works
                    drop(cq);

                    driver.try_run_all_wait_sqes();

                    if driver.wait_for_push_wakers.is_empty() {
                        return true;
                    }

                    for waker in driver.wait_for_push_wakers.drain(..) {
                        waker.wake();
                    }

                    return false;
                }

                // if we don't collect to a Vec, we can't use the driver again
                let cq = cq.collect::<Vec<_>>();

                for cqe in cq {
                    let user_data = cqe.user_data();

                    // means this cqe is a cancel cqe
                    if user_data == 0 {
                        continue;
                    }

                    let result = cqe.result();

                    // this cqe is canceled
                    if result == -libc::ECANCELED {
                        if let Some(callback) = driver.take_callback(cqe.user_data()) {
                            match callback {
                                Callback::ProvideBuffer { .. } => {
                                    unreachable!("ProvideBuffers event won't be canceled")
                                }

                                // at normal, a event with wakeup callback won't be canceled without
                                // runtime help, but wake up the waker may be a good idea
                                Callback::Wakeup { waker } => {
                                    driver.completion_queue_entries.insert(cqe.user_data(), cqe);

                                    waker.wake();
                                }

                                // a read event is canceled
                                Callback::CancelRead { group_id } => {
                                    if let Some(buffer_id) = buffer_select(cqe.flags()) {
                                        driver.give_back_buffer_with_id(group_id, buffer_id);
                                    }
                                }

                                Callback::CancelConnect { addr: _, fd } => {
                                    let _ = unistd::close(fd);
                                }

                                // no need to do anything
                                Callback::CancelOpenAt { .. }
                                | Callback::CancelStatx { .. }
                                | Callback::CancelRenameAt { .. }
                                | Callback::CancelUnlinkAt { .. }
                                | Callback::CancelTimeout { .. } => {}
                            }

                            continue;
                        }
                    }

                    if let Some(callback) = driver.callbacks.remove(&user_data) {
                        match callback {
                            Callback::ProvideBuffer { group_id } => {
                                if cqe.is_err() {
                                    panic!(
                                        "unexpect error for ProvideBuffers {}, user_data {}",
                                        Error::from_raw_os_error(-cqe.result()),
                                        user_data
                                    );
                                }

                                let group_buffer = driver
                                    .buffer_manager
                                    .group_buffer_mut(group_id)
                                    .unwrap_or_else(|| {
                                        panic!("group buffer {} not exist", group_id)
                                    });

                                if group_buffer.register_state()
                                    == GroupBufferRegisterState::Registering
                                {
                                    group_buffer.set_can_be_selected();
                                } else {
                                    group_buffer.increase_available();
                                }
                            }

                            Callback::Wakeup { waker } => {
                                driver.completion_queue_entries.insert(cqe.user_data(), cqe);

                                waker.wake();
                            }

                            // an ready read event is canceled
                            Callback::CancelRead { group_id } => {
                                let buffer_id =
                                    buffer_select(cqe.flags()).expect("buffer id not found");

                                driver.give_back_buffer_with_id(group_id, buffer_id);
                            }

                            Callback::CancelConnect { addr: _, fd } => {
                                let _ = unistd::close(fd);
                            }

                            // no need to do
                            Callback::CancelOpenAt { .. }
                            | Callback::CancelStatx { .. }
                            | Callback::CancelRenameAt { .. }
                            | Callback::CancelUnlinkAt { .. }
                            | Callback::CancelTimeout { .. } => {}
                        }
                    }
                }

                // wake up all waiting tasks
                for waker in driver.wait_for_push_wakers.drain(..) {
                    waker.wake();
                }

                // try to push all background sqes
                driver.try_run_all_wait_sqes();

                false
            });

            if need_yield && empty_cq_and_no_wait_wakers {
                thread::yield_now();
            }
        }
    }

    fn init_driver(&mut self) {
        DRIVER.with(|driver| {
            let runtime_driver = self.driver.take().expect("Driver is not created");

            driver.borrow_mut().replace(runtime_driver);
        })
    }

    fn init_task_sender_and_receiver(&mut self) -> bool {
        let (sender, receiver) = crossbeam_channel::unbounded();

        TASK_SENDER
            .with(|task_sender| {
                let mut task_sender = task_sender.borrow_mut();

                if task_sender.is_some() {
                    false
                } else {
                    task_sender.replace(sender);

                    true
                }
            })
            .then(|| {
                TASK_RECEIVER.with(|task_receiver| {
                    task_receiver.borrow_mut().replace(receiver);
                })
            })
            .is_some()
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        TASK_SENDER.with(|task_sender| {
            task_sender.borrow_mut().take();
        });

        TASK_RECEIVER.with(|task_receiver| {
            task_receiver.borrow_mut().take();
        });

        DRIVER.with(|driver| {
            driver.borrow_mut().take();
        });
    }
}

enum InnerTask<O> {
    AsyncTask(AsyncTask<O>),
    BlockTask(Receiver<O>, Arc<AtomicWaker>),
}

pub struct Task<O> {
    inner: InnerTask<O>,
}

impl<O> Task<O> {
    pub async fn cancel(self) -> Option<O> {
        match self.inner {
            InnerTask::AsyncTask(task) => task.cancel().await,
            InnerTask::BlockTask(receiver, _) => receiver.try_recv().ok(),
        }
    }

    pub fn detach(self) {
        if let InnerTask::AsyncTask(task) = self.inner {
            task.detach();
        }
        // when receiver drop, the sender will send failed, but who care
    }
}

impl<O> Future for Task<O>
where
    O: 'static + Unpin,
{
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut self.get_mut().inner;

        match this {
            InnerTask::AsyncTask(task) => task.poll_unpin(cx),
            InnerTask::BlockTask(receiver, atomic_waker) => match receiver.try_recv() {
                Err(TryRecvError::Empty) => {
                    atomic_waker.register(cx.waker());

                    Poll::Pending
                }

                Err(TryRecvError::Disconnected) => panic!("blocking task is interrupt"),

                Ok(result) => Poll::Ready(result),
            },
        }
    }
}

pub fn spawn<F>(fut: F) -> Task<F::Output>
where
    F: Future + 'static + Send,
    F::Output: 'static + Send,
{
    let task_sender = TASK_SENDER.with(|task_sender| {
        let mut task_sender = task_sender.borrow_mut();
        task_sender.as_mut().expect("task sender not init").clone()
    });

    let schedule = move |runnable| {
        task_sender.send(runnable).unwrap();
    };

    let (runnable, task) = async_task::spawn(fut, schedule);

    runnable.schedule();

    Task {
        inner: InnerTask::AsyncTask(task),
    }
}

pub fn spawn_blocking<F, O>(f: F) -> Task<O>
where
    F: FnOnce() -> O + 'static + Send + Sync,
    O: 'static + Send + Sync,
{
    let (tx, rx) = crossbeam_channel::bounded(1);

    // allow the blocking thread send result back to the main thread
    let main_thread_task_sender = TASK_SENDER.with(|task_sender| {
        let mut task_sender = task_sender.borrow_mut();
        let task_sender = task_sender.as_mut().expect("task sender not init");

        task_sender.clone()
    });

    let waker = Arc::new(AtomicWaker::new());

    {
        let waker = waker.clone();

        thread::spawn(move || {
            TASK_SENDER.with(|task_sender| {
                let mut task_sender = task_sender.borrow_mut();
                if task_sender.is_none() {
                    task_sender.replace(main_thread_task_sender);
                }
            });

            if tx.send(f()).is_ok() {
                waker.wake();
            }
        });
    }

    Task {
        inner: InnerTask::BlockTask(rx, waker),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_task() {
        let n = Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async { 1 });

        assert_eq!(n, 1);
    }

    async fn test_func1(n: i32) -> i32 {
        2 * n
    }

    #[test]
    fn test_call_func() {
        let n = Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async { test_func1(10).await });

        assert_eq!(n, 20);
    }

    #[test]
    fn test_simple_spawn() {
        let n = Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async {
                let task = spawn(async { 1 });

                task.await
            });

        assert_eq!(n, 1);
    }

    #[test]
    fn test_multi_spawn() {
        let (n1, n2) = Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async {
                let task1 = spawn(async { 1 });
                let task2 = spawn(async { 2 });

                (task1.await, task2.await)
            });

        assert_eq!(n1, 1);
        assert_eq!(n2, 2);
    }

    #[test]
    fn test_spawn_blocking() {
        let (n1, n2, n3) = Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async {
                let task3 = spawn_blocking(|| {
                    thread::sleep(Duration::from_secs(1));

                    3
                });

                let task1 = spawn(async { 1 });
                let task2 = spawn(async { 2 });

                let n1 = task1.await;
                dbg!(n1);

                let n2 = task2.await;
                dbg!(n2);

                let n3 = task3.await;
                dbg!(n3);

                (n1, n2, n3)
            });

        assert_eq!(n1, 1);
        assert_eq!(n2, 2);
        assert_eq!(n3, 3);
    }

    #[test]
    fn run_with_futures_timer() {
        Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async {
                futures_timer::Delay::new(Duration::from_secs(1)).await;
            })
    }
}
