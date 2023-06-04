use std::fmt::Debug;
use std::io;
use std::sync::{Arc, Mutex, Weak};

use flume::{Receiver, Sender};
use io_uring::cqueue::Entry;

use crate::buf::{FixedSizeBufRing, GBuf};

#[derive(Debug)]
pub struct OperationResult {
    pub result: io::Result<i32>,
    pub flags: u32,
    pub g_buf: Option<GBuf>,
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

        Self {
            result,
            flags,
            g_buf: None,
        }
    }
}

pub struct Operation {
    buf_ring: Option<FixedSizeBufRing>,
    result_sender: Sender<OperationResult>,
    _data: Arc<Mutex<Option<Box<dyn Droppable>>>>,
}

pub type OperationNew = (
    Operation,
    Receiver<OperationResult>,
    Weak<Mutex<Option<Box<dyn Droppable>>>>,
);

impl Operation {
    pub fn new() -> OperationNew {
        let (result_sender, result_receiver) = flume::bounded(1);
        let data = Arc::new(Mutex::new(None));
        let weak = Arc::downgrade(&data);

        (
            Self {
                buf_ring: None,
                result_sender,
                _data: data,
            },
            result_receiver,
            weak,
        )
    }

    pub fn new_with_buf_ring(buf_ring: FixedSizeBufRing, multi_shot: bool) -> OperationNew {
        let (result_sender, result_receiver) = if multi_shot {
            flume::unbounded()
        } else {
            flume::bounded(1)
        };
        let data = Arc::new(Mutex::new(None));
        let weak = Arc::downgrade(&data);

        (
            Self {
                buf_ring: Some(buf_ring),
                result_sender,
                _data: data,
            },
            result_receiver,
            weak,
        )
    }

    pub fn send_result(&self, mut result: OperationResult) {
        if let Some(buf_ring) = &self.buf_ring {
            if let Ok(res) = &result.result {
                let g_buf = buf_ring.get_buf(*res as _, result.flags);
                result.g_buf.replace(g_buf);
            }
        }

        let _ = self.result_sender.try_send(result);
    }
}

pub trait Droppable: Send + 'static {}

impl<T: Send + 'static> Droppable for T {}
