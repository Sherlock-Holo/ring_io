use std::future::Future;
use std::io::Result;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::opcode::Splice as RingSplice;
use io_uring::types::Fd;
use nix::fcntl::OFlag;
use nix::unistd;

use crate::cqe_ext::EntryExt;
use crate::driver::DRIVER;

#[derive(Debug)]
pub struct Splice {
    read_pipe: RawFd,
    write_pipe: RawFd,
}

impl Splice {
    pub fn new() -> Result<Self> {
        let (read_pipe, write_pipe) = unistd::pipe2(OFlag::O_CLOEXEC)?;

        Ok(Self {
            read_pipe,
            write_pipe,
        })
    }

    pub async fn copy<FD1: AsRawFd, FD2: AsRawFd>(&mut self, from: FD1, to: FD2) -> Result<u64> {
        self.copy_with_size(from, to, 128 * 1024).await
    }

    pub async fn copy_with_size<FD1: AsRawFd, FD2: AsRawFd>(
        &mut self,
        from: FD1,
        to: FD2,
        size: u32,
    ) -> Result<u64> {
        let from = from.as_raw_fd();
        let to = to.as_raw_fd();

        let mut copied = 0;

        loop {
            let mut n = SpliceCopy::new(from, self.write_pipe, size).await?;

            if n == 0 {
                return Ok(copied);
            }

            copied += n;

            while n > 0 {
                let written = SpliceCopy::new(self.read_pipe, to, n as _).await?;

                n -= written;
            }
        }
    }
}

impl Drop for Splice {
    fn drop(&mut self) {
        let _ = unistd::close(self.read_pipe);
        let _ = unistd::close(self.write_pipe);
    }
}

struct SpliceCopy<FD1, FD2> {
    from: FD1,
    to: FD2,
    user_data: Option<u64>,
    copy_size: u32,
}

impl<FD1, FD2> SpliceCopy<FD1, FD2> {
    fn new(from: FD1, to: FD2, copy_size: u32) -> Self {
        Self {
            from,
            to,
            user_data: None,
            copy_size,
        }
    }
}

impl<FD1: AsRawFd + Unpin, FD2: AsRawFd + Unpin> Future for SpliceCopy<FD1, FD2> {
    type Output = Result<u64>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(user_data) = this.user_data {
            let splice_cqe = DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().expect("Driver is not running");

                driver
                    .take_cqe_with_waker(user_data, cx.waker())
                    .map(|cqe| cqe.ok())
                    .transpose()
            })?;

            return match splice_cqe {
                None => Poll::Pending,
                Some(splice_cqe) => {
                    // drop won't send useless cancel
                    this.user_data.take();

                    Poll::Ready(Ok(splice_cqe.ok()?.result() as _))
                }
            };
        }

        let splice_sqe = RingSplice::new(
            Fd(this.from.as_raw_fd()),
            -1,
            Fd(this.to.as_raw_fd()),
            -1,
            this.copy_size,
        )
        .flags(libc::SPLICE_F_MOVE)
        .build();

        let user_data = DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.push_sqe_with_waker(splice_sqe, cx.waker().clone())
        })?;

        user_data.map(|user_data| this.user_data.replace(user_data));

        Poll::Pending
    }
}

impl<FD1, FD2> Drop for SpliceCopy<FD1, FD2> {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                match driver.as_mut() {
                    None => {}
                    Some(driver) => {
                        let _ = driver.cancel_normal(user_data);
                    }
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::AsyncReadExt;

    use super::*;
    use crate::fs::{self, File};
    use crate::net::{TcpListener, TcpStream};
    use crate::{block_on, spawn};

    #[test]
    fn create_splice() {
        let _splice = Splice::new().unwrap();
    }

    #[test]
    fn test_file_to_tcp() {
        block_on(async {
            let listener = TcpListener::bind("0.0.0.0:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let task = spawn(async move { TcpStream::connect(addr).await.unwrap() });

            let accept_stream = listener.accept().await.unwrap();
            let mut conn_stream = task.await;

            let file = File::open("testdata/book.txt").await.unwrap();

            let mut splice = Splice::new().unwrap();

            let copied = splice.copy(file, accept_stream.as_raw_fd()).await.unwrap();

            dbg!(copied);

            drop(accept_stream);

            let mut buf = vec![];

            let n = conn_stream.read_to_end(&mut buf).await.unwrap();

            dbg!(n);

            buf.truncate(n);

            let data = fs::read("testdata/book.txt").await.unwrap();

            assert_eq!(data, buf);
            assert_eq!(copied, n as _);
        })
    }
}
