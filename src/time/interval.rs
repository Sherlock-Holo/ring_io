use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::Stream;

use super::{sleep, Sleep};

pub fn interval(period: Duration) -> Interval {
    if period.is_zero() {
        panic!("period is zero");
    }

    Interval {
        period,
        sleep: None,
    }
}

#[must_use = "futures do nothing unless you `.next().await` or poll them"]
#[derive(Debug)]
pub struct Interval {
    period: Duration,
    sleep: Option<Sleep>,
}

impl Stream for Interval {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let sleep = match self.sleep.as_mut() {
                None => {
                    let sleep = sleep::sleep(self.period);

                    self.sleep.replace(sleep);

                    continue;
                }

                Some(sleep) => sleep,
            };

            return if Pin::new(sleep).poll(cx).is_ready() {
                self.sleep.take();

                Poll::Ready(Some(()))
            } else {
                Poll::Pending
            };
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (usize::MAX, None)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use futures_util::StreamExt;

    use super::*;
    use crate::runtime::Runtime;

    #[test]
    fn test_interval() {
        Runtime::builder()
            .build()
            .expect("build runtime failed")
            .block_on(async {
                let mut interval = interval(Duration::from_secs(1));

                let start = Instant::now();

                interval.next().await.unwrap();

                assert_eq!(start.elapsed().as_secs(), 1);

                interval.next().await.unwrap();

                assert_eq!(start.elapsed().as_secs(), 2);
            })
    }
}
