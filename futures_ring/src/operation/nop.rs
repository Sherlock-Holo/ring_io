use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::opcode::Nop as RingNop;

use crate::Driver;

#[must_use = "Future do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct Nop {
    driver: Driver,
    user_data: Option<u64>,
}

impl Nop {
    pub fn new(driver: &Driver) -> Self {
        Self {
            driver: driver.clone(),
            user_data: None,
        }
    }
}

impl Future for Nop {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let nop_cqe = self.driver.take_cqe_with_waker(user_data, cx.waker());

            return if nop_cqe.is_some() {
                // drop won't send useless cancel
                self.user_data.take();

                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            };
        }

        let sqe = RingNop::new().build();

        let user_data = self.driver.push_sqe_with_waker(sqe, cx.waker().clone())?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Nop {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            let _ = self.driver.cancel_normal(user_data);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{self, SyncSender};
    use std::sync::Arc;

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
    fn test_nop() {
        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut nop = Nop::new(&driver);

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        assert!(Pin::new(&mut nop)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert!(reactor.try_run_one().unwrap());

        rx.recv().unwrap();

        if let Poll::Ready(result) =
            Pin::new(&mut nop).poll(&mut Context::from_waker(futures_task::noop_waker_ref()))
        {
            result.unwrap();
        } else {
            panic!("nop is still no ready");
        }
    }
}
