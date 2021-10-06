use std::ffi::CString;
use std::future::Future;
use std::io::Result;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::opcode::OpenAt;
use io_uring::types::Fd;
use libc::{c_int, mode_t};

use crate::cqe_ext::EntryExt;
use crate::helper::path_to_c_string;
use crate::Driver;

#[derive(Debug)]
pub struct Open {
    path: Option<CString>,
    flags: c_int,
    mode: mode_t,
    user_data: Option<u64>,
    driver: Driver,
}

impl Open {
    pub fn new<P: AsRef<Path>>(
        path: P,
        flags: c_int,
        mode: mode_t,
        driver: &Driver,
    ) -> Result<Self> {
        let path = path_to_c_string(path.as_ref())?;

        Ok(Self::new_cstring(path, flags, mode, driver))
    }

    pub fn new_cstring(path: CString, flags: c_int, mode: mode_t, driver: &Driver) -> Self {
        Self {
            path: Some(path),
            flags,
            mode,
            user_data: None,
            driver: driver.clone(),
        }
    }
}

impl Future for Open {
    type Output = Result<RawFd>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let open_at_cqe = self
                .driver
                .take_cqe_with_waker(user_data, cx.waker())
                .map(|cqe| cqe.ok())
                .transpose()
                .map_err(|err| {
                    // drop won't send useless cancel when error happened
                    self.user_data.take();

                    err
                })?;

            return match open_at_cqe {
                None => Poll::Pending,
                Some(open_at_cqe) => {
                    // drop won't send useless cancel
                    self.user_data.take();

                    let fd = open_at_cqe.result();

                    // Safety: fd is valid
                    Poll::Ready(Ok(fd))
                }
            };
        }

        let path = &*self.path.as_ref().expect("path is not init");
        let open_at_sqe = OpenAt::new(Fd(libc::AT_FDCWD), path.as_ptr())
            .flags(self.flags)
            .mode(self.mode)
            .build();

        let user_data = self
            .driver
            .push_sqe_with_waker(open_at_sqe, cx.waker().clone())?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Open {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            let _ = self
                .driver
                .cancel_open_at(user_data, self.path.take().expect("path is not init"));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::File;
    use std::io::Read;
    use std::os::unix::io::FromRawFd;
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
    fn test_open() {
        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut open = Open::new("testdata/book.txt", libc::O_RDONLY, 0, &driver).unwrap();

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        assert!(Pin::new(&mut open)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        let fd = if let Poll::Ready(result) =
            Pin::new(&mut open).poll(&mut Context::from_waker(&waker))
        {
            result.unwrap()
        } else {
            panic!("open is still no ready");
        };

        let mut file = unsafe { File::from_raw_fd(fd) };

        let content = fs::read("testdata/book.txt").unwrap();

        let mut buf = vec![];

        file.read_to_end(&mut buf).unwrap();

        assert_eq!(content, buf);
    }
}
