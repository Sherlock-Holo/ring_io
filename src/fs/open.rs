use std::ffi::CString;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::drive::Drive;
use crate::event::{Event, OpenEvent};
use crate::fs::File;

pub struct Open<D: Drive> {
    path: CString,
    event: Arc<Event>,
    driver: D,
}

impl<D: Drive> Open<D> {
    pub(crate) fn new(path: CString, driver: D) -> Self {
        Self {
            path,
            event: Arc::new(Event::Nothing),
            driver,
        }
    }
}

impl<D: Drive + Unpin + Clone> Future for Open<D> {
    type Output = io::Result<File<D>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match &*this.event {
            Event::Open(open_event) => {
                let mut open_event = open_event.lock().unwrap();

                if open_event.result.is_none() {
                    open_event.waker.register(cx.waker());

                    Poll::Pending
                } else {
                    let fd = open_event.result.take().unwrap()?;

                    Poll::Ready(Ok(File::new(fd as _, this.driver.clone())))
                }
            }

            Event::Nothing => {
                let path = this.path.as_ptr();

                let event = futures_util::ready!(Pin::new(&mut this.driver).poll_prepare(
                    cx,
                    |sqe, cx| {
                        unsafe {
                            uring_sys::io_uring_prep_openat(
                                sqe.raw_mut(),
                                libc::AT_FDCWD,
                                path,
                                libc::O_RDONLY,
                                0x644,
                            );
                        }

                        let open_event = OpenEvent::new();

                        open_event.waker.register(cx.waker());

                        Arc::new(Event::Open(Mutex::new(open_event)))
                    }
                ))?;

                this.event = event;

                Poll::Pending
            }

            _ => unreachable!(),
        }
    }
}

impl<D: Drive> Drop for Open<D> {
    fn drop(&mut self) {
        self.event.cancel();
    }
}
