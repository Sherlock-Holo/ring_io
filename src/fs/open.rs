use std::ffi::CString;
use std::future::Future;
use std::io;
use std::io::Error;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use libc::{c_int, mode_t};

use crate::drive::{DemoDriver, Drive, Event, OpenEvent};
use crate::file_descriptor::FileDescriptor;
use crate::fs::File;

pub struct Open<D: Drive> {
    path: CString,
    event: Arc<Event>,
    flags: c_int,
    mode: mode_t,
    driver: Option<D>,
}

impl<D: Drive> Open<D> {
    pub(crate) fn new_read_only(path: CString, driver: D) -> Self {
        Self::new(path, libc::O_RDONLY, 0o644, driver)
    }

    fn new(path: CString, flags: c_int, mode: mode_t, driver: D) -> Self {
        Self {
            path,
            event: Arc::new(Event::Nothing),
            flags,
            mode,
            driver: Some(driver),
        }
    }
}

impl<D: Drive + Unpin> Future for Open<D> {
    type Output = io::Result<File<D>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match &*this.event {
            Event::Open(open_event) => {
                let mut open_event = open_event.lock().unwrap();

                if open_event.result.is_none() {
                    open_event.waker.register(cx.waker());

                    futures_util::ready!(
                        Pin::new(this.driver.as_mut().unwrap()).poll_submit(cx, false)
                    )?;

                    Poll::Pending
                } else {
                    let fd = open_event.result.take().unwrap()?;

                    Poll::Ready(Ok(File::new(FileDescriptor::new(
                        fd as _,
                        this.driver.take().unwrap(),
                        0,
                    ))))
                }
            }

            Event::Nothing => {
                let path = this.path.as_ptr();
                let flags = this.flags;
                let mode = this.mode;

                let event = futures_util::ready!(Pin::new(this.driver.as_mut().unwrap())
                    .poll_prepare(cx, |sqe, cx| {
                        unsafe {
                            uring_sys::io_uring_prep_openat(
                                sqe.raw_mut(),
                                libc::AT_FDCWD,
                                path,
                                flags,
                                mode,
                            );
                        }

                        let open_event = OpenEvent::new();

                        open_event.waker.register(cx.waker());

                        Arc::new(Event::Open(Mutex::new(open_event)))
                    }))?;

                this.event = event;

                futures_util::ready!(Pin::new(this.driver.as_mut().unwrap()).poll_submit(cx, true))?;

                Poll::Pending
            }

            _ => unreachable!(),
        }
    }
}

impl<D: Drive> Drop for Open<D> {
    fn drop(&mut self) {
        self.event.cancel();
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
            mode: 0o644,
        }
    }

    pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File<DemoDriver>> {
        let flags = libc::O_CLOEXEC
            | self.get_access_mode()?
            | self.get_creation_mode()?
            | (self.custom_flags as c_int & !libc::O_ACCMODE);

        let path = path.as_ref().as_os_str();
        let path = CString::new(path.as_bytes()).unwrap();

        Open::new(path, flags, self.mode, DemoDriver::new()).await
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

    fn get_access_mode(&self) -> io::Result<c_int> {
        match (self.read, self.write, self.append) {
            (true, false, false) => Ok(libc::O_RDONLY),
            (false, true, false) => Ok(libc::O_WRONLY),
            (true, true, false) => Ok(libc::O_RDWR),
            (false, _, true) => Ok(libc::O_WRONLY | libc::O_APPEND),
            (true, _, true) => Ok(libc::O_RDWR | libc::O_APPEND),
            (false, false, false) => Err(Error::from_raw_os_error(libc::EINVAL)),
        }
    }

    fn get_creation_mode(&self) -> io::Result<c_int> {
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
