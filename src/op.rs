use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, Weak};
use std::task::{ready, Context, Poll};

use flume::r#async::RecvFut;
use flume::{Receiver, RecvError};
use futures_util::FutureExt;

use crate::operation::{Droppable, OperationResult};

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
