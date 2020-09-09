use std::io::Result;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use std::{ptr, thread};

use futures_util::task::AtomicWaker;
use iou::{CompletionQueueEvent, Registrar, SubmissionQueue, SubmissionQueueEvent};
use once_cell::sync::Lazy;
use slab::Slab;

use crate::drive::event::Event;

use super::Drive;

pub fn get_default_driver() -> DemoDriver {
    const SQ_ENTRIES: u32 = 1024;

    static DRIVER: Lazy<DemoDriver> =
        Lazy::new(|| DemoDriver::new(SQ_ENTRIES, 64, Duration::from_micros(100)).unwrap());

    let driver = &*DRIVER;

    driver.clone()
}

#[derive(Clone)]
pub struct DemoDriver {
    inner: Arc<InnerDriver>,
    max_submit: usize,
}

impl DemoDriver {
    pub fn new(
        entries: u32,
        max_submit: usize,
        submit_interval: impl Into<Option<Duration>>,
    ) -> Result<Self> {
        let submit_interval = submit_interval.into();

        let inner = Arc::new(InnerDriver::new(entries, submit_interval)?);

        if let Some(submit_interval) = submit_interval {
            let inner = inner.clone();

            thread::spawn(move || loop {
                thread::sleep(submit_interval);

                let mut sq = inner.sq.lock().unwrap();

                let (sq, prepare_count) = &mut *sq;

                let prepare_count = prepare_count.as_mut().expect("submit count is not set");

                if *prepare_count == 0 {
                    continue;
                }

                *prepare_count = 0;

                let _ = sq.submit();
            });
        }

        Ok(DemoDriver { inner, max_submit })
    }
}

impl Drive for DemoDriver {
    fn poll_prepare<'cx>(
        self: Pin<&mut Self>,
        cx: &mut Context<'cx>,
        prepare: impl FnOnce(&mut SubmissionQueueEvent<'_>, &mut Context<'cx>) -> Arc<Event>,
    ) -> Poll<Result<Arc<Event>>> {
        let mut sq = self.inner.sq.lock().unwrap();
        let (sq, prepare_count) = &mut *sq;

        match sq.next_sqe() {
            None => {
                self.inner.waker.register(cx.waker());

                Poll::Pending
            }

            Some(mut sqe) => {
                let event = prepare(&mut sqe, cx);

                let user_data = self.inner.slab.lock().unwrap().insert(event.clone());

                sqe.set_user_data(user_data as _);

                if let Some(prepare_count) = prepare_count {
                    *prepare_count += 1;
                }

                Poll::Ready(Ok(event))
            }
        }
    }

    fn poll_submit(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        eager: bool,
    ) -> Poll<Result<usize>> {
        let mut sq = self.inner.sq.lock().unwrap();
        let (sq, prepare_count) = &mut *sq;

        if eager {
            if let Some(prepare_count) = prepare_count {
                *prepare_count = 0;
            }

            return Poll::Ready(sq.submit());
        }

        if let Some(prepare_count) = prepare_count {
            if *prepare_count >= self.max_submit {
                *prepare_count = 0;

                Poll::Ready(sq.submit())
            } else {
                Poll::Ready(Ok(0))
            }
        } else {
            Poll::Ready(Ok(0))
        }
    }
}

struct InnerDriver {
    ring: *mut iou::IoUring,
    sq: Mutex<(SubmissionQueue<'static>, Option<usize>)>,
    _registrar: Registrar<'static>,
    waker: Arc<AtomicWaker>,
    slab: Arc<Mutex<Slab<Arc<Event>>>>,
}

impl InnerDriver {
    fn new(entries: u32, submit_interval: Option<Duration>) -> Result<Self> {
        let ring_pointer = Box::into_raw(Box::new(iou::IoUring::new(entries)?));

        // Safety: ring is allocated on heap
        let ring_ref: &'static mut _ = unsafe { &mut *ring_pointer };

        let (sq, cq, registrar) = ring_ref.queues();

        let prepare_count = if submit_interval.is_some() {
            Some(0)
        } else {
            None
        };

        let sq = Mutex::new((sq, prepare_count));

        let waker = Arc::new(AtomicWaker::new());
        let slab = Arc::new(Mutex::new(Slab::with_capacity(entries as _)));

        {
            let waker = waker.clone();
            let slab = slab.clone();

            thread::spawn(move || complete(cq, &waker, slab));
        }

        Ok(Self {
            ring: ring_pointer,
            sq,
            _registrar: registrar,
            waker,
            slab,
        })
    }
}

unsafe impl Send for InnerDriver {}

unsafe impl Sync for InnerDriver {}

impl Drop for InnerDriver {
    fn drop(&mut self) {
        unsafe {
            ptr::drop_in_place(self.ring);
        }
    }
}

fn complete(
    mut cq: iou::CompletionQueue<'static>,
    waker: &AtomicWaker,
    slab: Arc<Mutex<Slab<Arc<Event>>>>,
) {
    while let Ok(cqe) = cq.wait_for_cqe() {
        let mut slab = slab.lock().unwrap();

        waker.wake();

        consume_cqe(cqe, &mut slab);

        while let Some(cqe) = cq.peek_for_cqe() {
            waker.wake();

            consume_cqe(cqe, &mut slab);
        }
    }
}

fn consume_cqe(cqe: CompletionQueueEvent, slab: &mut Slab<Arc<Event>>) {
    let user_data = cqe.user_data() as usize;

    if !slab.contains(user_data) {
        return;
    }

    let event = slab.remove(user_data);

    if !cqe.is_timeout() && !event.is_cancel() {
        event.set_result(cqe.result());
    }
}
