use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub use default_driver::get_default_driver;
pub use default_driver::DefaultDriver;
pub use event::*;

mod default_driver;
mod event;

pub type PollPrepareSubmit = (Poll<Result<Arc<Event>>>, Option<Poll<Result<usize>>>);

/// Implemented by drivers for io-uring.
///
/// The type that implements `Drive` is used to prepare and submit IO events to an io-uring
/// instance. Paired with a piece of code which processes completions, it can run IO on top of
/// io-uring.
pub trait Drive {
    /// Prepare an event on the submission queue.
    ///
    /// The implementer is responsible for provisioning an [`iou::SubmissionQueueEvent`] from the
    /// submission  queue. Once an SQE is available, the implementer should pass it to the
    /// `prepare` callback, which constructs a [`Completion`], and return that `Completion` to the
    /// caller.
    ///
    /// If the driver is not ready to recieve more events, it can return `Poll::Pending`. If it
    /// does, it must register a waker to wake the task when more events can be prepared, otherwise
    /// this method will not be called again. This allows the driver to implement backpressure.
    ///
    /// Drivers which call `prepare` but do not return the completion it gives are incorrectly
    /// implemented. This will lead ringbahn to panic.
    fn poll_prepare<'cx>(
        self: Pin<&mut Self>,
        cx: &mut Context<'cx>,
        prepare: impl FnOnce(&mut iou::SubmissionQueueEvent<'_>, &mut Context<'cx>) -> Arc<Event>,
    ) -> Poll<Result<Arc<Event>>>;

    /// Submit all of the events on the submission queue.
    ///
    /// The implementer is responsible for determining how and when these events are submitted to
    /// the kernel to complete. The `eager` argument is a hint indicating whether the caller would
    /// prefer to see events submitted eagerly, but the implementation is not obligated to follow
    /// this hint.
    ///
    /// If the implementation is not ready to submit, but wants to be called again to try later, it
    /// can return `Poll::Pending`. If it does, it must register a waker to wake the task when it
    /// would be appropriate to try submitting again.
    ///
    /// It is also valid not to submit an event but not to register a waker to try again, in which
    /// case the appropriate response would be to return `Ok(0)`. This indicates to the caller that
    /// the submission step is complete, whether or not actual IO was performed.
    fn poll_submit(self: Pin<&mut Self>, cx: &mut Context<'_>, eager: bool) -> Poll<Result<usize>>;

    fn poll_prepare_with_submit<'cx>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'cx>,
        prepare: impl FnOnce(&mut iou::SubmissionQueueEvent<'_>, &mut Context<'cx>) -> Arc<Event>,
        eager: Option<bool>,
    ) -> PollPrepareSubmit {
        let event = match self.as_mut().poll_prepare(cx, prepare) {
            Poll::Pending => return (Poll::Pending, None),
            Poll::Ready(result) => match result {
                Err(err) => return (Poll::Ready(Err(err)), None),
                Ok(event) => event,
            },
        };

        match eager {
            None => (Poll::Ready(Ok(event)), None),
            Some(eager) => match self.as_mut().poll_submit(cx, eager) {
                Poll::Pending => (Poll::Ready(Ok(event)), Some(Poll::Pending)),
                Poll::Ready(result) => (Poll::Ready(Ok(event)), Some(Poll::Ready(result))),
            },
        }
    }
}
