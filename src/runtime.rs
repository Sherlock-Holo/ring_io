use std::cell::RefCell;
use std::future::Future;
use std::io::Error;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::Duration;

use async_task::{Runnable, Task};
use io_uring::cqueue::buffer_select;
use nix::unistd;

use crate::buffer::GroupBufferRegisterState;
use crate::cqe_ext::EntryExt;
use crate::driver::{Callback, Driver, DRIVER};

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
                        }

                        continue;
                    }
                }

                if let Some(callback) = driver.callbacks.remove(&user_data) {
                    match callback {
                        Callback::ProvideBuffer { group_id } => {
                            if cqe.is_err() {
                                panic!(
                                    "unexpect error for ProvideBuffers {}",
                                    Error::from_raw_os_error(-cqe.result())
                                );
                            }

                            let group_buffer = driver
                                .buffer_manager
                                .group_buffer_mut(group_id)
                                .unwrap_or_else(|| panic!("group buffer {} not exist", group_id));

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
        let n = block_on(async { 1 });

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

    #[test]
    fn test_simple_span() {
        let n = block_on(async {
            let task = spawn_local(async { 1 });

            task.await
        });

        assert_eq!(n, 1);
    }

    #[test]
    fn test_multi_span() {
        let (n1, n2) = block_on(async {
            let task1 = spawn_local(async { 1 });
            let task2 = spawn_local(async { 2 });

            (task1.await, task2.await)
        });

        assert_eq!(n1, 1);
        assert_eq!(n2, 2);
    }
}
