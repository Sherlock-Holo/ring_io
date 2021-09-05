use std::ffi::CString;
use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::opcode::OpenAt;
use io_uring::types::Fd;
use libc::{c_int, mode_t};

use crate::cqe_ext::EntryExt;
use crate::helper::ResultExt;
use crate::Driver;

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

    pub fn open<P: AsRef<Path>>(&self, path: P, driver: &Driver) -> Open {
        let flags = match self
            .get_access_mode()
            .map(|access_mode| {
                self.get_creation_mode().map(|creation_mode| {
                    libc::O_CLOEXEC
                        | access_mode
                        | creation_mode
                        | (self.custom_flags as c_int & !libc::O_ACCMODE)
                })
            })
            .flatten_result()
        {
            Err(err) => {
                return Open {
                    fast_err: Some(err),
                    path: None,
                    flags: 0,
                    mode: 0,
                    user_data: None,
                    driver: driver.clone(),
                };
            }

            Ok(flags) => flags,
        };

        let path = match path_to_cstring(path) {
            Err(err) => {
                return Open {
                    fast_err: Some(err),
                    path: None,
                    flags: 0,
                    mode: 0,
                    user_data: None,
                    driver: driver.clone(),
                };
            }

            Ok(path) => path,
        };

        Open {
            fast_err: None,
            path: Some(path),
            flags,
            mode: self.mode,
            user_data: None,
            driver: driver.clone(),
        }
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

fn path_to_cstring<P: AsRef<Path>>(path: P) -> Result<CString> {
    CString::new(path.as_ref().as_os_str().as_bytes())
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err))
}

#[derive(Debug)]
pub struct Open {
    fast_err: Option<Error>,
    path: Option<CString>,
    flags: c_int,
    mode: mode_t,
    user_data: Option<u64>,
    driver: Driver,
}

impl Open {
    pub fn new<P: AsRef<Path>>(path: P, driver: &Driver) -> Self {
        OpenOptions::new().read(true).open(path, driver)
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

                    let fd = open_at_cqe.ok()?.result();

                    // Safety: fd is valid
                    Poll::Ready(Ok(fd))
                }
            };
        }

        if let Some(err) = self.fast_err.take() {
            return Poll::Ready(Err(err));
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
    use std::env;
    use std::fs;
    use std::fs::File;
    use std::io::SeekFrom;
    use std::io::{Read, Seek, Write};
    use std::os::unix::io::FromRawFd;
    use std::sync::mpsc::SyncSender;
    use std::sync::{mpsc, Arc};

    use futures_task::ArcWake;
    use tempfile::{NamedTempFile, TempDir};

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
    fn test_open_read_only() {
        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut open = Open::new("testdata/book.txt", &driver);

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

        dbg!(fd);
    }

    #[test]
    fn test_open_write_only() {
        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut open = OpenOptions::new()
            .write(true)
            .open("testdata/book.txt", &driver);

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

        dbg!(fd);
    }

    #[test]
    fn test_read_write() {
        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut open = OpenOptions::new()
            .read(true)
            .write(true)
            .open("testdata/book.txt", &driver);

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

        dbg!(fd);
    }

    #[test]
    fn test_read() {
        let data = fs::read("testdata/book.txt").unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut open = OpenOptions::new()
            .read(true)
            .open("testdata/book.txt", &driver);

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

        let mut buf = vec![];

        file.read_to_end(&mut buf).unwrap();

        assert_eq!(data, buf);
    }

    #[test]
    fn test_write() {
        let data = fs::read("testdata/book.txt").unwrap();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut tmp_file = NamedTempFile::new_in(&tmp_dir).unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut open = OpenOptions::new()
            .write(true)
            .open(tmp_file.path(), &driver);

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

        file.write_all(&data).unwrap();

        let mut buf = vec![];

        tmp_file.read_to_end(&mut buf).unwrap();

        assert_eq!(data, buf);
    }

    #[test]
    fn test_create_new() {
        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();

        let mut path = tmp_dir.path().to_path_buf();
        path.push("new-file");

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut open = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path, &driver);

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

        let metadata = fs::metadata(path).unwrap();

        assert!(metadata.is_file());
    }

    #[test]
    fn test_create() {
        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let tmp_file = NamedTempFile::new_in(&tmp_dir).unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut open = OpenOptions::new()
            .create(true)
            .write(true)
            .open(tmp_file.path(), &driver);

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

        let metadata = fs::metadata(tmp_file.path()).unwrap();

        assert!(metadata.is_file());
    }

    #[test]
    fn test_truncate() {
        let data = fs::read("testdata/book.txt").unwrap();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut tmp_file = NamedTempFile::new_in(&tmp_dir).unwrap();

        tmp_file.write_all(&data).unwrap();
        tmp_file.flush().unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut open = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .open(tmp_file.path(), &driver);

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

        assert_eq!(file.read(&mut [0; 1]).unwrap(), 0);
    }

    #[test]
    fn test_append() {
        let data = fs::read("testdata/book.txt").unwrap();

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut tmp_file = NamedTempFile::new_in(&tmp_dir).unwrap();

        tmp_file.write_all(&data).unwrap();
        tmp_file.flush().unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut open = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(tmp_file.path(), &driver);

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

        file.write_all(&data).unwrap();

        file.seek(SeekFrom::Start(0)).unwrap();

        let mut buf = vec![];

        file.read_to_end(&mut buf).unwrap();

        let mut new_data = Vec::with_capacity(data.len() * 2);
        new_data.extend_from_slice(&data);
        new_data.extend_from_slice(&data);

        assert_eq!(new_data, buf);
    }
}
