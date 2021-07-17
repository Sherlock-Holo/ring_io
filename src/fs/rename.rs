use std::ffi::CString;
use std::future::Future;
use std::io::Result;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::opcode::RenameAt;
use io_uring::types::Fd;

use crate::cqe_ext::EntryExt;
use crate::driver::DRIVER;
use crate::fs::path_to_cstring;

pub async fn rename<P1: AsRef<Path>, P2: AsRef<Path>>(from: P1, to: P2) -> Result<()> {
    let from = path_to_cstring(from)?;
    let to = path_to_cstring(to)?;

    Rename {
        old: Some(from),
        new: Some(to),
        user_data: None,
    }
    .await
}

struct Rename {
    old: Option<CString>,
    new: Option<CString>,
    user_data: Option<u64>,
}

impl Future for Rename {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let rename_at_cqe = DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().expect("Driver is not running");

                driver
                    .take_cqe_with_waker(user_data, cx.waker())
                    .map(|cqe| cqe.ok())
                    .transpose()
            })?;

            return match rename_at_cqe {
                None => Poll::Pending,
                Some(rename_at_cqe) => {
                    // drop won't send useless cancel
                    self.user_data.take();

                    rename_at_cqe.ok()?;

                    // Safety: fd is valid
                    Poll::Ready(Ok(()))
                }
            };
        }

        let old_path = &*self.old.as_ref().expect("old path is not set");
        let new_path = &*self.new.as_ref().expect("new path is not set");
        let rename_at_sqe = RenameAt::new(
            Fd(libc::AT_FDCWD),
            old_path.as_ptr(),
            Fd(libc::AT_FDCWD),
            new_path.as_ptr(),
        )
        .build();

        let user_data = DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.push_sqe_with_waker(rename_at_sqe, cx.waker().clone())
        })?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Rename {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                match driver.as_mut() {
                    None => {}
                    Some(driver) => {
                        let _ = driver.cancel_rename_at(
                            user_data,
                            self.old.take().expect("old path is not set"),
                            self.new.take().expect("new path is not set"),
                        );
                    }
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use tempfile::{NamedTempFile, TempDir};

    use super::*;
    use crate::block_on;

    #[test]
    fn test_rename() {
        block_on(async {
            let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
            let mut new_path = tmp_dir.path().to_path_buf();
            new_path.push("new-file");

            let tmp_file = NamedTempFile::new_in(tmp_dir.path()).unwrap();

            rename(tmp_file.path(), &new_path).await.unwrap();

            let metadata = std::fs::metadata(new_path).unwrap();
            assert!(metadata.is_file());
        })
    }
}
