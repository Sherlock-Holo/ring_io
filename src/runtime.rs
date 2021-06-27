use std::cell::RefCell;
use std::future::Future;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::Duration;

use async_task::{Runnable, Task};

use crate::driver::{CancelCallback, Driver, DRIVER};

thread_local! {
    static TASK_SENDER: RefCell<Option<Sender<Runnable>>> = RefCell::new(None);
    static TASK_RECEIVER: RefCell<Option<Receiver<Runnable>>> = RefCell::new(None);
}

/// return false if driver is init before
fn init_driver(sq_poll: Option<Duration>) -> bool {
    DRIVER.with(|driver| {
        let mut driver = driver.borrow_mut();
        if driver.is_some() {
            false
        } else {
            let ring_driver =
                Driver::new(sq_poll).unwrap_or_else(|err| panic!("init Driver failed: {}", err));

            driver.replace(ring_driver);

            true
        }
    })
}

/// return false if task sender and receiver is init before
fn init_task_sender_and_receiver() -> bool {
    let (sender, receiver) = mpsc::channel();

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

pub fn block_on<F, O>(fut: F) -> O
where
    F: Future<Output = O> + 'static,
    O: 'static,
{
    const MAX_RUN_TASK_COUNT: u8 = 64;

    init_driver(None);
    init_task_sender_and_receiver();

    let (sender, receiver) = mpsc::sync_channel(1);

    spawn_local(async move {
        let result = fut.await;
        sender.send(result).unwrap();
    })
    .detach();

    loop {
        match receiver.try_recv() {
            Ok(result) => return result,
            Err(TryRecvError::Disconnected) => unreachable!(),
            Err(TryRecvError::Empty) => {}
        }

        TASK_RECEIVER.with(|task_receiver| {
            let mut task_receiver = task_receiver.borrow_mut();
            let task_receiver = task_receiver.as_mut().unwrap();

            let available_run_task_count = MAX_RUN_TASK_COUNT;

            for i in 0..available_run_task_count {
                match task_receiver.try_recv() {
                    Ok(runnable) => {
                        runnable.run();

                        continue;
                    }

                    Err(TryRecvError::Disconnected) => unreachable!(),
                    Err(TryRecvError::Empty) => {}
                }

                // in this round, no task is available, yield out
                if i == 0 {
                    thread::yield_now();
                }

                return;
            }
        });

        DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().unwrap();

            for cqe in driver.ring.completion() {
                let user_data = cqe.user_data();

                // means this cqe is a cancel cqe
                if user_data == 0 {
                    continue;
                }

                let result = cqe.result();

                // this cqe is canceled
                if result == -libc::ECANCELED {
                    if let Some(cancel_callback) = driver.cancel_user_data.remove(&user_data) {
                        match cancel_callback {
                            CancelCallback::CancelWithoutCallback { .. } => {}
                            CancelCallback::CancelAndRegisterBuffer { .. } => {}
                        }
                    }

                    // canceled cqe doesn't need wake up
                    driver.wakers.remove(&user_data);
                }

                // although this cqe is done, but user want to cancel
                if let Some(cancel_callback) = driver.cancel_user_data.remove(&user_data) {
                    match cancel_callback {
                        CancelCallback::CancelWithoutCallback { .. } => {}
                        CancelCallback::CancelAndRegisterBuffer { .. } => {}
                    }

                    // canceled cqe doesn't need wake up
                    driver.wakers.remove(&user_data);
                } else {
                    // store the cqe and wake up task
                    driver.available_entries.insert(user_data, cqe);
                    if let Some(waker) = driver.wakers.remove(&user_data) {
                        waker.wake();
                    }
                }
            }

            // wake up all waiting tasks
            for waker in driver.wait_for_push_wakers.drain(..) {
                waker.wake();
            }
        });
    }
}

pub fn spawn_local<F, O>(fut: F) -> Task<O>
where
    F: Future<Output = O> + 'static,
    O: 'static,
{
    let schedule = |runnable| {
        TASK_SENDER.with(|task_sender| {
            let mut task_sender = task_sender.borrow_mut();
            let task_sender = task_sender.as_mut().expect("task sender not init");

            task_sender.send(runnable).unwrap();
        })
    };

    let (runnable, task) = async_task::spawn_local(fut, schedule);

    runnable.schedule();

    task
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_task() {
        let n = block_on(async move { 1 });

        assert_eq!(n, 1);
    }

    async fn test_func1(n: i32) -> i32 {
        2 * n
    }

    #[test]
    fn test_call_func() {
        let n = block_on(async { test_func1(10).await });

        assert_eq!(n, 20);
    }
}
