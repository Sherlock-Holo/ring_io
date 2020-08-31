use std::io::Result;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;

use futures_util::task::AtomicWaker;
use iou::{CompletionQueueEvent, SubmissionQueueEvent};
use once_cell::sync::Lazy;
use slab::Slab;

use crate::drive::event::Event;

use super::Drive;

const SQ_ENTRIES: u32 = 1024;

static SQ: Lazy<Mutex<iou::SubmissionQueue<'static>>> = Lazy::new(init_sq);

#[derive(Clone)]
pub struct DemoDriver {
    sq: &'static Mutex<iou::SubmissionQueue<'static>>,
    waker: &'static AtomicWaker,
}

impl DemoDriver {
    pub fn new() -> Self {
        Self {
            sq: &SQ,
            waker: get_waker(),
        }
    }
}

impl Drive for DemoDriver {
    fn poll_prepare<'cx>(
        self: Pin<&mut Self>,
        cx: &mut Context<'cx>,
        prepare: impl FnOnce(&mut SubmissionQueueEvent<'_>, &mut Context<'cx>) -> Arc<Event>,
    ) -> Poll<Result<Arc<Event>>> {
        let mut sq = self.sq.lock().unwrap();

        match sq.next_sqe() {
            None => {
                self.waker.register(cx.waker());

                Poll::Pending
            }

            Some(mut sqe) => {
                let event = prepare(&mut sqe, cx);

                let user_data = get_slab().lock().unwrap().insert(event.clone());

                sqe.set_user_data(user_data as _);

                Poll::Ready(Ok(event))
            }
        }
    }

    fn poll_submit(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _eager: bool,
    ) -> Poll<Result<usize>> {
        let mut sq = self.sq.lock().unwrap();

        // for now ignore eager
        Poll::Ready(sq.submit())
    }
}

impl Default for DemoDriver {
    fn default() -> Self {
        Self::new()
    }
}

fn init_sq() -> Mutex<iou::SubmissionQueue<'static>> {
    // Safety: won't call this function twice
    unsafe {
        static mut RING: Option<iou::IoUring> = None;

        RING = Some(iou::IoUring::new(SQ_ENTRIES).expect("TODO handle io_uring_init failure"));

        let (sq, cq, _) = RING.as_mut().unwrap().queues();

        thread::spawn(move || complete(cq));

        Mutex::new(sq)
    }
}

fn get_waker() -> &'static AtomicWaker {
    static WAKER: Lazy<AtomicWaker> = Lazy::new(AtomicWaker::new);

    &WAKER
}

fn get_slab() -> &'static Mutex<Slab<Arc<Event>>> {
    static SLAB: Lazy<Mutex<Slab<Arc<Event>>>> = Lazy::new(|| Mutex::new(Slab::new()));

    &SLAB
}

unsafe fn complete(mut cq: iou::CompletionQueue<'static>) {
    while let Ok(cqe) = cq.wait_for_cqe() {
        get_waker().wake();

        let mut ready = cq.ready() as usize + 1;

        consume_cqe(cqe);

        ready -= 1;

        while let Some(cqe) = cq.peek_for_cqe() {
            get_waker().wake();

            if ready == 0 {
                ready = cq.ready() as usize + 1;
            }

            consume_cqe(cqe);

            ready -= 1;
        }

        debug_assert!(ready == 0);
    }
}

fn consume_cqe(cqe: CompletionQueueEvent) {
    let user_data = cqe.user_data() as usize;

    let mut slab = get_slab().lock().unwrap();

    if !slab.contains(user_data) {
        return;
    }

    let event = slab.remove(user_data);

    if !cqe.is_timeout() && !event.is_cancel() {
        event.set_result(cqe.result());
    }
}
