use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, Weak};
use std::task::{ready, Context, Poll};

use flume::r#async::{RecvFut, RecvStream};
use flume::{Receiver, RecvError};
use futures_util::{FutureExt, Stream, StreamExt};

use crate::operation::{Droppable, OperationResult};

#[must_use = "io_uring operation has been submitted"]
pub struct Op<T: Completable> {
    data: Option<T>,
    result_receiver: RecvFut<'static, OperationResult>,
    data_drop: Weak<Mutex<Option<Box<dyn Droppable>>>>,
}

impl<T: Completable> Op<T> {
    pub(crate) fn new(
        data: T,
        result_receiver: Receiver<OperationResult>,
        data_drop: Weak<Mutex<Option<Box<dyn Droppable>>>>,
    ) -> Self {
        Self {
            data: Some(data),
            result_receiver: result_receiver.into_recv_async(),
            data_drop,
        }
    }
}

impl<T: Completable> Future for Op<T> {
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(self.result_receiver.poll_unpin(cx)) {
            Ok(result) => Poll::Ready(self.data.take().unwrap().complete(result)),
            Err(RecvError::Disconnected) => panic!("runtime exit"),
        }
    }
}

impl<T: Completable> Drop for Op<T> {
    fn drop(&mut self) {
        if let Some(data) = self.data.take() {
            if let Some(data) = data.data_drop() {
                if let Some(data_drop) = self.data_drop.upgrade() {
                    data_drop.lock().unwrap().replace(data);
                }
            }
        }
    }
}

pub trait Completable: Send + Unpin + 'static {
    type Output;

    fn complete(self, result: OperationResult) -> Self::Output;

    fn data_drop(self) -> Option<Box<dyn Droppable>>;
}

#[must_use = "io_uring operation has been submitted"]
pub struct MultiOp<T: MultiCompletable> {
    data: Option<T>,
    result_receiver: RecvStream<'static, OperationResult>,
    data_drop: Weak<Mutex<Option<Box<dyn Droppable>>>>,
}

impl<T: MultiCompletable> MultiOp<T> {
    pub(crate) fn new(
        data: T,
        result_receiver: Receiver<OperationResult>,
        data_drop: Weak<Mutex<Option<Box<dyn Droppable>>>>,
    ) -> Self {
        Self {
            data: Some(data),
            result_receiver: result_receiver.into_stream(),
            data_drop,
        }
    }
}

impl<T: MultiCompletable> Stream for MultiOp<T> {
    type Item = T::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.result_receiver.poll_next_unpin(cx)) {
            None => Poll::Ready(None),
            Some(result) => Poll::Ready(self.data.as_mut().unwrap().complete(result)),
        }
    }
}

impl<T: MultiCompletable> Drop for MultiOp<T> {
    fn drop(&mut self) {
        if let Some(data) = self.data.take() {
            if let Some(data) = data.data_drop() {
                if let Some(data_drop) = self.data_drop.upgrade() {
                    data_drop.lock().unwrap().replace(data);
                }
            }
        }
    }
}

pub trait MultiCompletable: Send + Unpin + 'static {
    type Output;

    fn complete(&mut self, result: OperationResult) -> Option<Self::Output>;

    fn data_drop(self) -> Option<Box<dyn Droppable>>;
}
