use std::ffi::CString;
use std::future::Future;
use std::io::Result;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::opcode::UnlinkAt;
use io_uring::types::Fd;

use crate::cqe_ext::EntryExt;
use crate::driver::DRIVER;
use crate::fs::path_to_cstring;

pub async fn remove_file<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path_to_cstring(path)?;

    Remove {
        path: Some(path),
        user_data: None,
        remove_dir: false,
    }
    .await
}

pub async fn remove_dir<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path_to_cstring(path)?;

    Remove {
        path: Some(path),
        user_data: None,
        remove_dir: true,
    }
    .await
}

// wait for io_uring readdir and opendir support
/*pub async fn remove_dir_all<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref();

    let filetype = symlink_metadata(path).await?.file_type();

    if filetype.is_symlink() {
        remove_file(path).await
    } else {
        todo!()
    }
}*/

struct Remove {
    path: Option<CString>,
    user_data: Option<u64>,
    remove_dir: bool,
}

impl Future for Remove {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let remove_sqe = DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().expect("Driver is not running");

                driver
                    .take_cqe_with_waker(user_data, cx.waker())
                    .map(|cqe| cqe.ok())
                    .transpose()
            })?;

            return match remove_sqe {
                None => Poll::Pending,
                Some(remove_sqe) => {
                    // drop won't send useless cancel
                    self.user_data.take();

                    remove_sqe.ok()?;

                    // Safety: fd is valid
                    Poll::Ready(Ok(()))
                }
            };
        }

        let path = &*self.path.as_ref().expect("path is not set");

        let mut unlink_at_sqe = UnlinkAt::new(Fd(libc::AT_FDCWD), path.as_ptr());
        if self.remove_dir {
            unlink_at_sqe = unlink_at_sqe.flags(libc::AT_REMOVEDIR);
        }

        let unlink_at_sqe = unlink_at_sqe.build();

        let user_data = DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.push_sqe_with_waker(unlink_at_sqe, cx.waker().clone())
        })?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Remove {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                match driver.as_mut() {
                    None => {}
                    Some(driver) => {
                        let _ = driver.cancel_unlink_at(
                            user_data,
                            self.path.take().expect("path is not set"),
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
    use std::io::ErrorKind;

    use tempfile::{NamedTempFile, TempDir};

    use super::*;
    use crate::block_on;

    #[test]
    fn test_remove_dir() {
        block_on(async {
            let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

            remove_dir(tmp_dir.path()).await.unwrap();

            let error = std::fs::metadata(tmp_dir.path()).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::NotFound);
        })
    }

    #[test]
    fn test_remove_file() {
        block_on(async {
            let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

            let tmp_file = NamedTempFile::new_in(tmp_dir.path()).unwrap();

            remove_file(tmp_file.path()).await.unwrap();

            let error = std::fs::metadata(tmp_file.path()).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::NotFound);
        })
    }

    #[test]
    fn test_remove_file_should_failed() {
        block_on(async {
            let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

            // the IsADirectory is not stable
            // assert_eq!(remove_file(tmp_dir.path()).await.unwrap_err().kind(), ErrorKind::IsADirectory);
            dbg!(remove_file(tmp_dir.path()).await.unwrap_err().kind());
        })
    }

    #[test]
    fn test_remove_dir_should_failed() {
        block_on(async {
            let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

            let tmp_file = NamedTempFile::new_in(tmp_dir.path()).unwrap();

            // the NotADirectory is not stable
            // assert_eq!(remove_dir(tmp_file.path()).await.unwrap_err().kind(), ErrorKind::NotADirectory);
            dbg!(remove_dir(tmp_file.path()).await.unwrap_err().kind());
        })
    }
}
