use std::fmt::Debug;
use std::io;
use std::sync::{Arc, Mutex, Weak};

use flume::{Receiver, Sender};
use futures_util::task::AtomicWaker;
use io_uring::cqueue::Entry;

#[derive(Debug)]
pub struct OperationResult {
    pub result: io::Result<i32>,
    pub flags: u32,
}

impl OperationResult {
    pub fn new(cqe: &Entry) -> Self {
        let flags = cqe.flags();
        let result = cqe.result();
        let result = if result < 0 {
            Err(io::Error::from_raw_os_error(-result))
        } else {
            Ok(result)
        };

        Self { result, flags }
    }
}

pub struct Operation {
    result_sender: Sender<OperationResult>,
    waker: Arc<AtomicWaker>,
    data: Arc<Mutex<Option<Box<dyn Droppable>>>>,
}

impl Operation {
    pub fn new() -> (
        Self,
        Receiver<OperationResult>,
        Arc<AtomicWaker>,
        Weak<Mutex<Option<Box<dyn Droppable>>>>,
    ) {
        let (result_sender, result_receiver) = flume::bounded(1);
        let waker = Arc::new(AtomicWaker::new());
        let data = Arc::new(Mutex::new(None));
        let weak = Arc::downgrade(&data);

        (
            Self {
                result_sender,
                waker: waker.clone(),
                data,
            },
            result_receiver,
            waker,
            weak,
        )
    }

    pub fn send_result(&self, result: OperationResult) {
        if self.result_sender.try_send(result).is_ok() {
            self.waker.wake();
        }
    }
}

/*enum OperationState {
    Submitted {
        result_sender: Sender<OperationResult>,
        waker: Arc<AtomicWaker>,
    },

    Ignored,
    Completed,
}

impl Debug for OperationState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationState::Submitted { .. } => f
                .debug_struct("OperationState::Submitted")
                .finish_non_exhaustive(),
            OperationState::Ignored { .. } => f
                .debug_struct("OperationState::Ignored")
                .finish_non_exhaustive(),
            OperationState::Completed => f.debug_struct("OperationState::Completed").finish(),
        }
    }
}*/

pub trait Droppable: Send + 'static {}

impl<T: Send + 'static> Droppable for T {}
