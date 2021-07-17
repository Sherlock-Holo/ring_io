use std::ffi::CString;
use std::future::Future;
use std::io::{Error, Result, SeekFrom};
use std::io::{IoSlice, IoSliceMut};
use std::os::raw::c_int;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_io::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWrite};
use futures_util::{AsyncReadExt, AsyncWriteExt, FutureExt};
use io_uring::opcode::{Fsync, OpenAt};
use io_uring::types::{Fd, FsyncFlags};
use libc::mode_t;
use nix::unistd;
use nix::unistd::Whence;

use crate::cqe_ext::EntryExt;
use crate::driver::DRIVER;
use crate::fs::path_to_cstring;
use crate::fs::{metadata, Metadata};
use crate::io::ring_fd::RingFd;
use crate::runtime::Task;
use crate::spawn_blocking;

pub struct File {
    fd: RingFd,
    seek_task: Option<Task<Result<u64>>>,
}

impl File {
    fn new(ring_fd: RingFd) -> Self {
        Self {
            fd: ring_fd,
            seek_task: None,
        }
    }

    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        OpenOptions::new().read(true).open(path).await
    }

    pub async fn metadata(&self) -> Result<Metadata> {
        const FD_PATH: &str = "/proc/self/fd";

        let mut path = Path::new(FD_PATH).to_path_buf();
        path.push(self.fd.as_raw_fd().to_string());

        metadata(path).await
    }

    pub fn sync_all(&self) -> Sync {
        Sync {
            fd: &self.fd,
            only_data: false,
            user_data: None,
        }
    }

    pub fn sync_data(&self) -> Sync {
        Sync {
            fd: &self.fd,
            only_data: true,
            user_data: None,
        }
    }
}

impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl IntoRawFd for File {
    fn into_raw_fd(self) -> RawFd {
        self.fd.into_raw_fd()
    }
}

impl FromRawFd for File {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self::new(RingFd::new_file(fd))
    }
}

impl AsyncRead for File {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.fd).poll_read(cx, buf)
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.fd).poll_read_vectored(cx, bufs)
    }
}

impl AsyncBufRead for File {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        Pin::new(&mut self.get_mut().fd).poll_fill_buf(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.fd).consume(amt)
    }
}

impl AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.fd).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.fd).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.fd).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.fd).poll_close(cx)
    }
}

impl AsyncSeek for File {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<Result<u64>> {
        match &mut self.seek_task {
            Some(task) => task.poll_unpin(cx),
            None => {
                let fd = self.fd.as_raw_fd();

                let mut task = spawn_blocking(move || {
                    let (offset, whence) = match pos {
                        SeekFrom::Start(offset) => (offset as i64, Whence::SeekSet),
                        SeekFrom::End(offset) => (offset, Whence::SeekEnd),
                        SeekFrom::Current(offset) => (offset, Whence::SeekCur),
                    };

                    unistd::lseek64(fd, offset, whence)
                        .map(|new_pos| new_pos as u64)
                        .map_err(Error::from)
                });

                if let Poll::Ready(result) = task.poll_unpin(cx) {
                    Poll::Ready(result)
                } else {
                    self.seek_task.replace(task);

                    Poll::Pending
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct OpenOptions {
    // generic
    read: bool,
    write: bool,
    append: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
    // system-specific
    custom_flags: i32,
    mode: mode_t,
}

impl OpenOptions {
    pub fn new() -> OpenOptions {
        OpenOptions {
            // generic
            read: false,
            write: false,
            append: false,
            truncate: false,
            create: false,
            create_new: false,
            // system-specific
            custom_flags: 0,
            mode: 0o666,
        }
    }

    pub async fn open<P: AsRef<Path>>(&self, path: P) -> Result<File> {
        let flags = libc::O_CLOEXEC
            | self.get_access_mode()?
            | self.get_creation_mode()?
            | (self.custom_flags as c_int & !libc::O_ACCMODE);

        let path = path_to_cstring(path)?;

        Open {
            path: Some(path),
            flags,
            mode: self.mode,
            user_data: None,
        }
        .await
    }

    pub fn read(&mut self, read: bool) -> &mut Self {
        self.read = read;
        self
    }

    pub fn write(&mut self, write: bool) -> &mut Self {
        self.write = write;
        self
    }

    pub fn append(&mut self, append: bool) -> &mut Self {
        self.append = append;
        self
    }

    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.truncate = truncate;
        self
    }

    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create_new = create_new;
        self
    }

    fn get_access_mode(&self) -> Result<c_int> {
        match (self.read, self.write, self.append) {
            (true, false, false) => Ok(libc::O_RDONLY),
            (false, true, false) => Ok(libc::O_WRONLY),
            (true, true, false) => Ok(libc::O_RDWR),
            (false, _, true) => Ok(libc::O_WRONLY | libc::O_APPEND),
            (true, _, true) => Ok(libc::O_RDWR | libc::O_APPEND),
            (false, false, false) => Err(Error::from_raw_os_error(libc::EINVAL)),
        }
    }

    fn get_creation_mode(&self) -> Result<c_int> {
        match (self.write, self.append) {
            (true, false) => {}
            (false, false) => {
                if self.truncate || self.create || self.create_new {
                    return Err(Error::from_raw_os_error(libc::EINVAL));
                }
            }
            (_, true) => {
                if self.truncate && !self.create_new {
                    return Err(Error::from_raw_os_error(libc::EINVAL));
                }
            }
        }

        Ok(match (self.create, self.truncate, self.create_new) {
            (false, false, false) => 0,
            (true, false, false) => libc::O_CREAT,
            (false, true, false) => libc::O_TRUNC,
            (true, true, false) => libc::O_CREAT | libc::O_TRUNC,
            (_, _, true) => libc::O_CREAT | libc::O_EXCL,
        })
    }
}

impl OpenOptionsExt for OpenOptions {
    fn mode(&mut self, mode: u32) -> &mut Self {
        self.mode = mode;

        self
    }

    fn custom_flags(&mut self, flags: i32) -> &mut Self {
        self.custom_flags = flags;

        self
    }
}

struct Open {
    path: Option<CString>,
    flags: c_int,
    mode: mode_t,
    user_data: Option<u64>,
}

impl Future for Open {
    type Output = Result<File>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let open_at_cqe = DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().expect("Driver is not running");

                driver
                    .take_cqe_with_waker(user_data, cx.waker())
                    .map(|cqe| cqe.ok())
                    .transpose()
            })?;

            return match open_at_cqe {
                None => Poll::Pending,
                Some(open_at_cqe) => {
                    // drop won't send useless cancel
                    self.user_data.take();

                    let fd = open_at_cqe.ok()?.result();

                    // Safety: fd is valid
                    Poll::Ready(Ok(unsafe { File::new(RingFd::new_file(fd)) }))
                }
            };
        }

        let path = &*self.path.as_ref().expect("path is not set");
        let open_at_sqe = OpenAt::new(Fd(libc::AT_FDCWD), path.as_ptr())
            .flags(self.flags)
            .mode(self.mode)
            .build();

        let user_data = DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.push_sqe_with_waker(open_at_sqe, cx.waker().clone())
        })?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Open {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                match driver.as_mut() {
                    None => {}
                    Some(driver) => {
                        let _ = driver
                            .cancel_open_at(user_data, self.path.take().expect("path is not set"));
                    }
                }
            })
        }
    }
}

pub struct Sync<'a> {
    fd: &'a RingFd,
    only_data: bool,
    user_data: Option<u64>,
}

impl<'a> Future for Sync<'a> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let sync_cqe = DRIVER.with(|driver| {
                let mut driver = driver.borrow_mut();
                let driver = driver.as_mut().expect("Driver is not running");

                driver
                    .take_cqe_with_waker(user_data, cx.waker())
                    .map(|cqe| cqe.ok())
                    .transpose()
            })?;

            return match sync_cqe {
                None => Poll::Pending,
                Some(sync_cqe) => {
                    // drop won't send useless cancel
                    self.user_data.take();

                    sync_cqe.ok()?;

                    // Safety: fd is valid
                    Poll::Ready(Ok(()))
                }
            };
        }

        let mut flags = FsyncFlags::empty();
        if self.only_data {
            flags = FsyncFlags::DATASYNC;
        }

        let fsync_sqe = Fsync::new(Fd(self.fd.as_raw_fd())).flags(flags).build();

        let user_data = DRIVER.with(|driver| {
            let mut driver = driver.borrow_mut();
            let driver = driver.as_mut().expect("Driver is not running");

            driver.push_sqe_with_waker(fsync_sqe, cx.waker().clone())
        })?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl<'a> Drop for Sync<'a> {
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

pub async fn read<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    let path = path.as_ref();

    let mut file = File::open(path).await?;

    let data_size = metadata(path).await?.len();

    let mut buf = Vec::with_capacity(data_size as _);

    let n = file.read_to_end(&mut buf).await?;

    debug_assert_eq!(buf.len(), n);

    Ok(buf)
}

pub async fn write<P: AsRef<Path>, C: AsRef<[u8]>>(path: P, contents: C) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await?;

    file.write_all(contents.as_ref()).await
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::io::{ErrorKind, Read, Seek, Write};

    use futures_util::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
    use tempfile::{NamedTempFile, TempDir};

    use super::*;
    use crate::block_on;

    #[test]
    fn test_open_read_only() {
        block_on(async {
            File::open("testdata/book.txt").await.unwrap();
        })
    }

    #[test]
    fn test_write_only() {
        block_on(async {
            OpenOptions::new()
                .write(true)
                .open("testdata/book.txt")
                .await
                .unwrap();
        })
    }

    #[test]
    fn test_read_write() {
        block_on(async {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open("testdata/book.txt")
                .await
                .unwrap();
        })
    }

    #[test]
    fn test_read() {
        block_on(async {
            let mut file = File::open("testdata/book.txt").await.unwrap();

            let mut buf = vec![];

            let n = file.read_to_end(&mut buf).await.unwrap();

            dbg!(n);

            let book = std::fs::read("testdata/book.txt").unwrap();

            assert_eq!(&book, &buf[..n]);
        })
    }

    #[test]
    fn test_write() {
        block_on(async {
            let tmp_dir = env::temp_dir();

            let mut file = NamedTempFile::new_in(tmp_dir).unwrap();

            let mut open_file = OpenOptions::new()
                .write(true)
                .open(file.path())
                .await
                .unwrap();
            open_file.write_all(b"test").await.unwrap();

            let mut buf = vec![];
            let n = file.read_to_end(&mut buf).unwrap();

            assert_eq!(&buf[..n], b"test");
        })
    }

    #[test]
    fn test_create() {
        block_on(async {
            let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
            let mut tmp_file_path = tmp_dir.path().to_path_buf();
            tmp_file_path.push("test-file");

            assert_eq!(
                std::fs::metadata(&tmp_file_path).unwrap_err().kind(),
                ErrorKind::NotFound
            );

            let _file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_file_path)
                .await
                .unwrap();

            assert!(std::fs::metadata(tmp_file_path).unwrap().is_file());
        })
    }

    #[test]
    fn test_seek() {
        block_on(async {
            let tmp_dir = env::temp_dir();

            let file = NamedTempFile::new_in(tmp_dir).unwrap();

            let mut open_file = OpenOptions::new()
                .write(true)
                .read(true)
                .open(file.path())
                .await
                .unwrap();
            open_file.write_all(b"test").await.unwrap();

            let n = open_file.seek(SeekFrom::Start(0)).await.unwrap();
            assert_eq!(n, 0);

            let mut buf = vec![];
            let n = open_file.read_to_end(&mut buf).await.unwrap();

            assert_eq!(&buf[..n], b"test");
        })
    }

    #[test]
    fn test_open_truncate() {
        block_on(async {
            let tmp_dir = env::temp_dir();

            let mut file = NamedTempFile::new_in(tmp_dir).unwrap();

            file.write_all(b"test").unwrap();

            let mut open_file = OpenOptions::new()
                .truncate(true)
                .write(true)
                .read(true)
                .open(file.path())
                .await
                .unwrap();

            let n = open_file.read(&mut [0; 1]).await.unwrap();
            assert_eq!(n, 0);
        })
    }

    #[test]
    fn test_open_append() {
        block_on(async {
            let tmp_dir = env::temp_dir();

            let mut file = NamedTempFile::new_in(tmp_dir).unwrap();

            file.write_all(b"test").unwrap();

            let mut open_file = OpenOptions::new()
                .append(true)
                .write(true)
                .open(file.path())
                .await
                .unwrap();

            open_file.write_all(b"test").await.unwrap();

            file.seek(SeekFrom::Start(0)).unwrap();

            let mut buf = vec![];

            let n = file.read_to_end(&mut buf).unwrap();

            assert_eq!(&buf[..n], b"testtest");
        })
    }

    #[test]
    fn test_sync() {
        block_on(async {
            let tmp_dir = env::temp_dir();

            let mut file = NamedTempFile::new_in(tmp_dir).unwrap();

            let mut open_file = OpenOptions::new()
                .write(true)
                .open(file.path())
                .await
                .unwrap();
            open_file.write_all(b"test").await.unwrap();

            open_file.sync_data().await.unwrap();
            open_file.sync_all().await.unwrap();

            let mut buf = vec![];
            let n = file.read_to_end(&mut buf).unwrap();

            assert_eq!(&buf[..n], b"test");
        })
    }

    #[test]
    fn test_read_func() {
        block_on(async {
            let tmp_dir = env::temp_dir();

            let file = NamedTempFile::new_in(tmp_dir).unwrap();

            let mut open_file = OpenOptions::new()
                .write(true)
                .open(file.path())
                .await
                .unwrap();
            open_file.write_all(b"test").await.unwrap();

            let buf = read(file.path()).await.unwrap();

            assert_eq!(&buf, b"test");
        })
    }

    #[test]
    fn test_write_func() {
        block_on(async {
            let tmp_dir = env::temp_dir();

            let mut file = NamedTempFile::new_in(tmp_dir).unwrap();

            dbg!(file.path());

            write(file.path(), b"test").await.unwrap();

            let mut buf = vec![];

            let n = file.read_to_end(&mut buf).unwrap();

            assert_eq!(&buf[..n], b"test");
        })
    }
}
