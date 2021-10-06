use std::future::Future;
use std::io::Result;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::opcode::Fsync as RingFsync;
use io_uring::types::{Fd, FsyncFlags};

use crate::cqe_ext::EntryExt;
use crate::Driver;

#[must_use = "Future do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct Fsync {
    fd: RawFd,
    user_data: Option<u64>,
    driver: Driver,
    flags: Option<FsyncFlags>,
}

impl Fsync {
    pub unsafe fn new<FD: AsRawFd>(fd: &FD, driver: &Driver) -> Self {
        Self {
            fd: fd.as_raw_fd(),
            user_data: None,
            driver: driver.clone(),
            flags: None,
        }
    }

    pub fn flags(mut self, flags: FsyncFlags) -> Self {
        self.flags.replace(flags);

        self
    }
}

impl Future for Fsync {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let fsync_cqe = self
                .driver
                .take_cqe_with_waker(user_data, cx.waker())
                .map(|cqe| cqe.ok())
                .transpose()
                .map_err(|err| {
                    // drop won't send useless cancel when error happened
                    self.user_data.take();

                    err
                })?;

            return if fsync_cqe.is_some() {
                // drop won't send useless cancel
                self.user_data.take();

                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            };
        }

        let mut fsync_sqe = RingFsync::new(Fd(self.fd));
        if let Some(flags) = self.flags {
            fsync_sqe = fsync_sqe.flags(flags);
        }

        let fsync_sqe = fsync_sqe.build();

        let user_data = self
            .driver
            .push_sqe_with_waker(fsync_sqe, cx.waker().clone())?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Fsync {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            // clone the Bytes will still pointer to the same address
            let _ = self.driver.cancel_normal(user_data);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::sync::mpsc::SyncSender;
    use std::sync::{mpsc, Arc};

    use futures_task::ArcWake;

    use super::*;

    struct ArcWaker {
        tx: SyncSender<()>,
    }

    impl ArcWake for ArcWaker {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.tx.send(()).unwrap();
        }
    }

    #[test]
    fn test_fsync() {
        let file = File::open("testdata/book.txt").unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut fsync = unsafe { Fsync::new(&file, &driver) };

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        assert!(Pin::new(&mut fsync)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        if let Poll::Ready(result) = Pin::new(&mut fsync).poll(&mut Context::from_waker(&waker)) {
            result.unwrap()
        } else {
            panic!("fsync is still no ready");
        };
    }
}
