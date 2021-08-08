use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use io_uring::opcode::Timeout;
use io_uring::types::Timespec;

use crate::driver::DRIVER;

pub fn sleep(duration: Duration) -> Sleep {
    let second = duration.as_secs();
    let nano_second = duration.subsec_nanos();

    Sleep {
        timespec: Some(Box::new(Timespec::new().sec(second).nsec(nano_second))),
        user_data: None,
    }
}

pub fn sleep_until(deadline: Instant) -> Sleep {
    let duration = deadline.saturating_duration_since(Instant::now());

    sleep(duration)
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct Sleep {
    timespec: Option<Box<Timespec>>,
    user_data: Option<u64>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let timeout_cqe = DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().expect("Driver is not running");

                driver.take_cqe_with_waker(user_data, cx.waker())
            });

            return match timeout_cqe {
                None => Poll::Pending,
                Some(timeout_cqe) => {
                    // drop won't send useless cancel
                    self.user_data.take();

                    // match can use -libc::ETIME or NEGATIVE_ECANCELED, so we declare a new const
                    // value here
                    const NEGATIVE_ETIME: libc::c_int = -libc::ETIME;
                    const NEGATIVE_ECANCELED: libc::c_int = -libc::ECANCELED;

                    match timeout_cqe.result() {
                        // even the sleep is cancel, but we don't care the result
                        NEGATIVE_ETIME | NEGATIVE_ECANCELED => return Poll::Ready(()),
                        0 => unreachable!("no count set for the Timeout op, should never return 0"),
                        result => panic!("unexpected other result: {}", result),
                    }
                }
            };
        }

        let timespec = self.timespec.as_ref().expect("timespec is not init");

        let timeout_sqe = Timeout::new(&**timespec).build();

        let user_data = DRIVER
            .with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().expect("Driver is not running");

                driver.push_sqe_with_waker(timeout_sqe, cx.waker().clone())
            })
            .expect("push Timeout sqe to io_uring sq failed, it should not happened");

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                match driver.as_mut() {
                    None => {}
                    Some(driver) => {
                        let _ = driver.cancel_timeout(
                            user_data,
                            self.timespec.take().expect("timespec is not set"),
                        );
                    }
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::block_on;

    #[test]
    fn test_sleep() {
        block_on(async {
            let start = Instant::now();

            sleep(Duration::from_secs(1)).await;

            assert_eq!(start.elapsed().as_secs(), 1);
        })
    }

    #[test]
    fn test_sleep_until() {
        block_on(async {
            let start = Instant::now();
            let deadline = start + Duration::from_secs(1);

            sleep_until(deadline).await;

            assert_eq!(start.elapsed().as_secs(), 1);
        })
    }
}
