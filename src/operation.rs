use std::fmt::Debug;
use std::io;
use std::sync::{Arc, Mutex, Weak};

use flume::{Receiver, Sender};
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
    data: Arc<Mutex<Option<Box<dyn Droppable>>>>,
}

impl Operation {
    pub fn new() -> (
        Self,
        Receiver<OperationResult>,
        Weak<Mutex<Option<Box<dyn Droppable>>>>,
    ) {
        let (result_sender, result_receiver) = flume::bounded(1);
        let data = Arc::new(Mutex::new(None));
        let weak = Arc::downgrade(&data);

        (
            Self {
                result_sender,
                data,
            },
            result_receiver,
            weak,
        )
    }

    pub fn send_result(&self, result: OperationResult) {
        let _ = self.result_sender.try_send(result);
    }
}

pub trait Droppable: Send + 'static {}

impl<T: Send + 'static> Droppable for T {}
