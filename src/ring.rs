// use std::io;
// use std::os::unix::io::RawFd;
// use std::pin::Pin;
// use std::sync::{Arc, Mutex};
// use std::task::{Context, Poll};
//
// use futures_channel::oneshot::Receiver;
// use futures_util::task::AtomicWaker;
//
// use crate::drive::Drive;
// use crate::event::Event;
// use crate::state::State;
//
// pub struct Ring<D: Drive> {
//     event: Option<Arc<Event>>,
//     driver: D,
// }
//
// impl<D: Drive + Unpin> Ring<D> {
//     pub fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>, prepare: impl FnOnce(&mut iou::SubmissionQueueEvent) -> Arc<Event>) -> Poll<io::Result<usize>> {
//         let this = self.get_mut();
//
//         /*if this.event.is_some() {
//             this.waker.register(cx.waker());
//
//             return Poll::Pending;
//         }*/
//         if let Some(event) = this.event.take() {
//             match event.as_ref() {
//                 Event::Read { offset, buf, state, waker } => {
//                     let mut state = state.lock().unwrap();
//
//                     match &mut *state {
//                         State::Submitted => {
//                             this.waker.register(cx.waker());
//
//                             drop(state);
//
//                             this.event.replace(event);
//
//                             return Poll::Pending;
//                         }
//                         State::Completed(result) => {
//                             let result = result.take().unwrap();
//
//                             return Poll::Ready(result);
//                         }
//                         State::Canceled => unreachable!()
//                     }
//                 }
//             }
//         }
//
//         let event = futures_util::ready!(Pin::new(&mut this.driver).poll_prepare(cx, |mut sqe, cx| {
//             prepare(&mut sqe)
//         }))?;
//
//         todo!()
//     }
// }
