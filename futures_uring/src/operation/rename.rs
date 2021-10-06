use std::ffi::CString;
use std::future::Future;
use std::io::Result;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::opcode::RenameAt;
use io_uring::types::Fd;

use crate::cqe_ext::EntryExt;
use crate::helper::path_to_c_string;
use crate::Driver;

#[derive(Debug)]
pub struct RenameAt2 {
    old: Option<CString>,
    new: Option<CString>,
    flags: Option<libc::c_uint>,
    user_data: Option<u64>,
    driver: Driver,
}

impl RenameAt2 {
    pub fn new<P1: AsRef<Path>, P2: AsRef<Path>>(
        old: P1,
        new: P2,
        driver: &Driver,
    ) -> Result<Self> {
        let old = path_to_c_string(old.as_ref())?;
        let new = path_to_c_string(new.as_ref())?;

        Ok(Self::new_c_string(old, new, driver))
    }

    pub fn new_c_string(old: CString, new: CString, driver: &Driver) -> Self {
        Self {
            old: Some(old),
            new: Some(new),
            flags: None,
            user_data: None,
            driver: driver.clone(),
        }
    }

    pub fn flags(mut self, flags: libc::c_uint) -> Self {
        self.flags.replace(flags);

        self
    }
}

impl Future for RenameAt2 {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let rename_at_cqe = self
                .driver
                .take_cqe_with_waker(user_data, cx.waker())
                .map(|cqe| cqe.ok())
                .transpose()
                .map_err(|err| {
                    // drop won't send useless cancel when error happened
                    self.user_data.take();

                    err
                })?;

            return if rename_at_cqe.is_some() {
                // drop won't send useless cancel
                self.user_data.take();

                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            };
        }

        let old = &*self.old.as_ref().expect("old path is not init");
        let new = &*self.new.as_ref().expect("new path is not init");

        let mut rename_at_sqe = RenameAt::new(
            Fd(libc::AT_FDCWD),
            old.as_ptr(),
            Fd(libc::AT_FDCWD),
            new.as_ptr(),
        );

        if let Some(flags) = self.flags {
            rename_at_sqe = rename_at_sqe.flags(flags);
        }

        let rename_at_sqe = rename_at_sqe.build();

        let user_data = self
            .driver
            .push_sqe_with_waker(rename_at_sqe, cx.waker().clone())?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for RenameAt2 {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            let _ = self.driver.cancel_rename_at(
                user_data,
                self.old.take().expect("old path is not init"),
                self.old.take().expect("new path is not init"),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::ErrorKind;
    use std::sync::mpsc::SyncSender;
    use std::sync::{mpsc, Arc};

    use futures_task::ArcWake;
    use tempfile::TempDir;

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
    fn test_rename_dir() {
        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

        let old = tmp_dir.path().join("a");
        let new = tmp_dir.path().join("b");

        fs::create_dir(&old).unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut rename_at2 = RenameAt2::new(&old, &new, &driver).unwrap();

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        assert!(Pin::new(&mut rename_at2)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        if let Poll::Ready(result) =
            Pin::new(&mut rename_at2).poll(&mut Context::from_waker(&waker))
        {
            result.unwrap()
        } else {
            panic!("rename_at2 is still no ready");
        };

        assert!(fs::metadata(new).unwrap().is_dir());

        assert_eq!(fs::metadata(old).unwrap_err().kind(), ErrorKind::NotFound);
    }

    #[test]
    fn test_rename_file() {
        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

        let old = tmp_dir.path().join("a");
        let new = tmp_dir.path().join("b");

        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&old)
            .unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut rename_at2 = RenameAt2::new(&old, &new, &driver).unwrap();

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        assert!(Pin::new(&mut rename_at2)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        if let Poll::Ready(result) =
            Pin::new(&mut rename_at2).poll(&mut Context::from_waker(&waker))
        {
            result.unwrap()
        } else {
            panic!("rename_at2 is still no ready");
        };

        assert!(fs::metadata(new).unwrap().is_file());

        assert_eq!(fs::metadata(old).unwrap_err().kind(), ErrorKind::NotFound);
    }
}
