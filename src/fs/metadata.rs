use std::ffi::CString;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::io::Result;
use std::mem::MaybeUninit;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use io_uring::opcode::Statx;
use io_uring::types::Fd;
use libc::mode_t;

use crate::cqe_ext::EntryExt;
use crate::driver::DRIVER;
use crate::fs::path_to_cstring;

#[derive(Debug, Copy, Clone)]
pub struct FileType {
    stx_mode: mode_t,
}

impl Display for FileType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.is_dir() {
            f.write_str("Dir")
        } else if self.is_file() {
            f.write_str("RegularFile")
        } else if self.is_symlink() {
            f.write_str("Symlink")
        } else {
            f.write_str("Unknown")
        }
    }
}

impl From<Metadata> for FileType {
    fn from(metadata: Metadata) -> Self {
        Self {
            stx_mode: metadata.statx.stx_mode as _,
        }
    }
}

impl PartialEq for FileType {
    fn eq(&self, other: &Self) -> bool {
        (self.is_dir() && other.is_dir())
            && (self.is_file() && other.is_file())
            && (self.is_symlink() && other.is_symlink())
    }
}

impl Eq for FileType {}

impl FileType {
    pub fn is_dir(&self) -> bool {
        self.stx_mode & libc::S_IFMT == libc::S_IFDIR
    }

    pub fn is_file(&self) -> bool {
        self.stx_mode & libc::S_IFMT == libc::S_IFREG
    }

    pub fn is_symlink(&self) -> bool {
        self.stx_mode & libc::S_IFMT == libc::S_IFLNK
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Permissions {
    stx_mode: mode_t,
}

impl Display for Permissions {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.readonly() {
            f.write_str("Readonly")
        } else {
            f.write_str("NotReadonly")
        }
    }
}

impl From<Metadata> for Permissions {
    fn from(metadata: Metadata) -> Self {
        Self {
            stx_mode: metadata.statx.stx_mask,
        }
    }
}

impl Eq for Permissions {}

impl PartialEq for Permissions {
    fn eq(&self, other: &Self) -> bool {
        self.readonly() && other.readonly()
    }
}

impl Permissions {
    pub fn readonly(&self) -> bool {
        // check if any class (owner, group, others) has write permission
        self.stx_mode & 0o222 == 0
    }

    pub fn set_readonly(&mut self, readonly: bool) {
        if readonly {
            // remove write permission for all classes; equivalent to `chmod a-w <file>`
            self.stx_mode &= !0o222;
        } else {
            // add write permission for all classes; equivalent to `chmod a+w <file>`
            self.stx_mode |= 0o222;
        }
    }

    pub fn mode(&self) -> u32 {
        self.stx_mode as u32
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Metadata {
    statx: libc::statx,
}

impl From<libc::statx> for Metadata {
    fn from(statx: libc::statx) -> Self {
        Self { statx }
    }
}

impl Metadata {
    pub fn file_type(&self) -> FileType {
        FileType::from(*self)
    }

    #[inline]
    pub fn is_dir(&self) -> bool {
        FileType::from(*self).is_dir()
    }

    #[inline]
    pub fn is_file(&self) -> bool {
        FileType::from(*self).is_file()
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.statx.stx_size
    }

    pub fn permissions(&self) -> Permissions {
        Permissions::from(*self)
    }

    pub fn modified(&self) -> Result<SystemTime> {
        Ok(SystemTime::UNIX_EPOCH
            + Duration::new(
                self.statx.stx_mtime.tv_sec as _,
                self.statx.stx_mtime.tv_nsec,
            ))
    }

    pub fn accessed(&self) -> Result<SystemTime> {
        Ok(SystemTime::UNIX_EPOCH
            + Duration::new(
                self.statx.stx_atime.tv_sec as _,
                self.statx.stx_atime.tv_nsec,
            ))
    }

    pub fn created(&self) -> Result<SystemTime> {
        Ok(SystemTime::UNIX_EPOCH
            + Duration::new(
                self.statx.stx_ctime.tv_sec as _,
                self.statx.stx_ctime.tv_nsec,
            ))
    }
}

pub async fn metadata<P: AsRef<Path>>(path: P) -> Result<Metadata> {
    let path = path_to_cstring(path)?;

    MetadataFuture {
        path: Some(path),
        follow_symlink: false,
        statx: None,
        user_data: None,
    }
    .await
}

pub async fn symlink_metadata<P: AsRef<Path>>(path: P) -> Result<Metadata> {
    let path = path_to_cstring(path)?;

    MetadataFuture {
        path: Some(path),
        follow_symlink: true,
        statx: None,
        user_data: None,
    }
    .await
}

struct MetadataFuture {
    path: Option<CString>,
    follow_symlink: bool,
    statx: Option<Box<MaybeUninit<libc::statx>>>,
    user_data: Option<u64>,
}

impl Future for MetadataFuture {
    type Output = Result<Metadata>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let connect_cqe = DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().expect("Driver is not running");

                driver
                    .take_cqe_with_waker(user_data, cx.waker())
                    .map(|cqe| cqe.ok())
                    .transpose()
            })?;

            return match connect_cqe {
                None => Poll::Pending,
                Some(connect_cqe) => {
                    // drop won't send useless cancel
                    self.user_data.take();

                    connect_cqe.ok()?;

                    let statx = self.statx.take().expect("statx is not init");

                    // Safety: io_uring will fill the data
                    let statx = unsafe { (*statx).assume_init() };

                    Poll::Ready(Ok(statx.into()))
                }
            };
        }

        self.statx.replace(Box::new(MaybeUninit::zeroed()));
        let statx: *mut _ = self.statx.as_mut().unwrap().as_mut_ptr();

        let mut flags = libc::AT_STATX_SYNC_AS_STAT;
        if !self.follow_symlink {
            flags |= libc::AT_SYMLINK_NOFOLLOW;
        }

        let path = &*self.path.as_ref().expect("path is not set");
        let statx_sqe = Statx::new(Fd(libc::AT_FDCWD), path.as_ptr(), statx as *mut _)
            .flags(flags)
            .mask(libc::STATX_ALL)
            .build();

        let user_data = DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.push_sqe_with_waker(statx_sqe, cx.waker().clone())
        })?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for MetadataFuture {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                match driver.as_mut() {
                    None => {}
                    Some(driver) => {
                        let _ = driver.cancel_statx(
                            user_data,
                            self.path.take().expect("path is not set"),
                            self.statx.take().expect("statx is not set"),
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
    fn test_get_file_metadata() {
        block_on(async {
            let tmp_file = NamedTempFile::new_in(env::temp_dir()).unwrap();

            let metadata = metadata(tmp_file.path()).await.unwrap();

            assert!(metadata.file_type().is_file());
            assert!(metadata.is_file());

            dbg!(metadata.accessed().unwrap());
            dbg!(metadata.modified().unwrap());
            dbg!(metadata.created().unwrap());
            dbg!(metadata.permissions().readonly());
        })
    }

    #[test]
    fn test_get_dir_metadata() {
        block_on(async {
            let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

            let metadata = metadata(tmp_dir.path()).await.unwrap();

            assert!(metadata.file_type().is_dir());
            assert!(metadata.is_dir());

            dbg!(metadata.accessed().unwrap());
            dbg!(metadata.modified().unwrap());
            dbg!(metadata.created().unwrap());
            dbg!(metadata.permissions().readonly());
        })
    }

    #[test]
    fn test_get_symlink_metadata() {
        block_on(async {
            let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
            let tmp_file = NamedTempFile::new_in(tmp_dir.path()).unwrap();

            let mut symlink_path = tmp_dir.path().to_path_buf();
            symlink_path.push("symlink");

            std::os::unix::fs::symlink(tmp_file.path(), &symlink_path).unwrap();

            let metadata = metadata(symlink_path).await.unwrap();

            assert!(metadata.file_type().is_symlink());

            dbg!(metadata.accessed().unwrap());
            dbg!(metadata.modified().unwrap());
            dbg!(metadata.created().unwrap());
            dbg!(metadata.permissions().readonly());
        })
    }

    #[test]
    fn test_get_follow_symlink_metadata() {
        block_on(async {
            let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
            let tmp_file = NamedTempFile::new_in(tmp_dir.path()).unwrap();

            let mut symlink_path = tmp_dir.path().to_path_buf();
            symlink_path.push("symlink");

            std::os::unix::fs::symlink(tmp_file.path(), &symlink_path).unwrap();

            let metadata = symlink_metadata(symlink_path).await.unwrap();

            assert!(metadata.file_type().is_file());
            assert!(metadata.is_file());

            dbg!(metadata.accessed().unwrap());
            dbg!(metadata.modified().unwrap());
            dbg!(metadata.created().unwrap());
            dbg!(metadata.permissions().readonly());
        })
    }
}
