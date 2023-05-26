use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};

use flume::{Receiver, TryRecvError};
use futures_util::task::AtomicWaker;

use crate::operation::{Droppable, OperationResult};

pub struct Op<T: Completable> {
    data: Option<T>,
    result_receiver: Receiver<OperationResult>,
    waker: Arc<AtomicWaker>,
    data_drop: Weak<Mutex<Option<Box<dyn Droppable>>>>,
}

impl<T: Completable> Op<T> {
    pub(crate) fn new(
        data: T,
        result_receiver: Receiver<OperationResult>,
        waker: Arc<AtomicWaker>,
        data_drop: Weak<Mutex<Option<Box<dyn Droppable>>>>,
    ) -> Self {
        Self {
            data: Some(data),
            result_receiver,
            waker,
            data_drop,
        }
    }
}

impl<T: Completable> Future for Op<T> {
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.result_receiver.try_recv() {
            Err(TryRecvError::Empty) => {
                self.waker.register(cx.waker());

                // make sure when we register a new waker, we won't miss the ready event because
                // runtime use the old waker to wake us
                match self.result_receiver.try_recv() {
                    Err(TryRecvError::Empty) => Poll::Pending,
                    Err(TryRecvError::Disconnected) => panic!("runtime exit"),
                    Ok(result) => Poll::Ready(self.data.take().unwrap().complete(result)),
                }
            }

            Err(TryRecvError::Disconnected) => panic!("runtime exit"),
            Ok(result) => Poll::Ready(self.data.take().unwrap().complete(result)),
        }
    }
}

impl<T: Completable> Drop for Op<T> {
    fn drop(&mut self) {
        if let Some(data) = self.data.take() {
            if let Some(data_drop) = self.data_drop.upgrade() {
                data_drop.lock().unwrap().replace(data.data_drop());
            }
        }
    }
}

pub trait Completable: Send + Unpin + 'static {
    type Output;

    fn complete(self, result: OperationResult) -> Self::Output;

    fn data_drop(self) -> Box<dyn Droppable>;
}
