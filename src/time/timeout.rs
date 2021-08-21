use std::future::Future;
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use pin_project_lite::pin_project;

use super::{sleep, sleep_until, Sleep};

pub fn timeout<F>(duration: Duration, fut: F) -> Timeout<F> {
    Timeout {
        sleep: sleep(duration),
        fut,
    }
}

pub fn timeout_at<F>(deadline: Instant, fut: F) -> Timeout<F> {
    Timeout {
        sleep: sleep_until(deadline),
        fut,
    }
}

pin_project! {
    #[must_use = "Future do nothing unless you `.await` or poll them"]
    #[derive(Debug)]
    pub struct Timeout<F> {
        sleep: Sleep,
        #[pin]
        fut: F,
    }
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        if let Poll::Ready(result) = Pin::new(&mut this.fut).poll(cx) {
            return Poll::Ready(Ok(result));
        }

        if Pin::new(&mut this.sleep).poll(cx).is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Err(Error::from(ErrorKind::TimedOut)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::Runtime;

    #[test]
    fn test_timeout_expired() {
        Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async {
                let err = timeout(
                    Duration::from_secs(1),
                    futures_util::future::pending::<()>(),
                )
                .await
                .unwrap_err();

                assert_eq!(err.kind(), ErrorKind::TimedOut)
            })
    }

    #[test]
    fn test_timeout_not_expired() {
        Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async {
                assert_eq!(
                    timeout(Duration::from_secs(1), async { 1 }).await.unwrap(),
                    1
                )
            })
    }

    #[test]
    fn test_timeout_not_expired_with_long_future() {
        Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async {
                assert_eq!(
                    timeout(Duration::from_secs(2), async {
                        sleep(Duration::from_secs(1)).await;

                        1
                    })
                    .await
                    .unwrap(),
                    1
                )
            })
    }
}
