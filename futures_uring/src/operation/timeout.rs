use std::future::Future;
use std::io::{Error, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use io_uring::opcode::Timeout as RingTimeout;
use io_uring::types::Timespec;

use crate::Driver;

#[must_use = "Future do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct Timeout {
    timespec: Option<Box<Timespec>>,
    user_data: Option<u64>,
    driver: Driver,
}

impl Timeout {
    pub fn new(duration: Duration, driver: &Driver) -> Self {
        let second = duration.as_secs();
        let nano_second = duration.subsec_nanos();

        Self {
            timespec: Some(Box::new(Timespec::new().sec(second).nsec(nano_second))),
            user_data: None,
            driver: driver.clone(),
        }
    }
}

impl Future for Timeout {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            return match self.driver.take_cqe_with_waker(user_data, cx.waker()) {
                None => Poll::Pending,
                Some(timeout_cqe) => {
                    // drop won't send useless cancel
                    self.user_data.take();

                    // match can't use -libc::ETIME or NEGATIVE_ECANCELED, so we declare a new const
                    // value here
                    const NEGATIVE_ETIME: libc::c_int = -libc::ETIME;
                    const NEGATIVE_ECANCELED: libc::c_int = -libc::ECANCELED;

                    match timeout_cqe.result() {
                        NEGATIVE_ETIME => return Poll::Ready(Ok(())),
                        NEGATIVE_ECANCELED => {
                            return Poll::Ready(Err(Error::from_raw_os_error(libc::ETIME)))
                        }
                        0 => unreachable!("no count set for the Timeout op, should never return 0"),
                        result => panic!("unexpected other result: {}", result),
                    }
                }
            };
        }

        let timespec = self.timespec.as_ref().expect("timespec is not init");

        let timeout_sqe = RingTimeout::new(&**timespec).build();

        let user_data = self
            .driver
            .push_sqe_with_waker(timeout_sqe, cx.waker().clone())?;
        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Timeout {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            let _ = self.driver.cancel_timeout(
                user_data,
                self.timespec.take().expect("timespec is not set"),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{self, SyncSender};
    use std::sync::Arc;
    use std::time::Instant;

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
    fn test_timeout() {
        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut timeout = Timeout::new(Duration::from_secs(1), &driver);

        let start = Instant::now();

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        assert!(Pin::new(&mut timeout)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        if let Poll::Ready(result) =
            Pin::new(&mut timeout).poll(&mut Context::from_waker(futures_task::noop_waker_ref()))
        {
            result.unwrap();

            dbg!(start.elapsed());
        } else {
            panic!("timeout is still no ready");
        }
    }
}
