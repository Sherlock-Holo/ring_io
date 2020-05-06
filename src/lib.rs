use std::collections::HashMap;
use std::ffi::CString;
use std::fs::File;
use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::FromRawFd;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, Once};
use std::task::{Context, Poll, Waker};
use std::thread;

use futures_channel::oneshot;
use futures_channel::oneshot::{Receiver, Sender};
use futures_util::pin_mut;
use futures_util::ready;
use io_uring::concurrent::{CompletionQueue, IoUring, SubmissionQueue};
use io_uring::opcode::types::{OpenHow, Target};
use io_uring::opcode::{AsyncCancel, Openat2, Read, Write};
use libc::mode_t;
use once_cell::sync::OnceCell;

pub use ext::*;
pub use open_options::OpenOptions;

mod ext;
mod open_options;

pub mod prelude {
    pub use crate::ext::*;
}

fn get_user_data() -> u64 {
    static USER_DATA_GEN: AtomicU64 = AtomicU64::new(1);

    USER_DATA_GEN.fetch_add(1, Ordering::Relaxed)
}

fn get_ring() -> &'static IoUring {
    static RING: OnceCell<IoUring> = OnceCell::new();

    RING.get_or_init(|| io_uring::IoUring::new(4096).unwrap().concurrent())
}

fn get_result_map() -> &'static Mutex<HashMap<u64, (Sender<Result<usize>>, Waker)>> {
    static MAP: OnceCell<Mutex<HashMap<u64, (Sender<Result<usize>>, Waker)>>> = OnceCell::new();

    MAP.get_or_init(|| Default::default())
}

fn get_sq() -> SubmissionQueue<'static> {
    get_ring().submission()
}

fn get_cq() -> CompletionQueue<'static> {
    get_ring().completion()
}

fn run_ring() {
    let ring = get_ring();
    let completion_queue = get_cq();

    loop {
        ring.submit_and_wait(1).unwrap();

        let mut map_guard = get_result_map().lock().unwrap();

        while let Some(entry) = completion_queue.pop() {
            if let Some((sender, waker)) = map_guard.remove(&entry.user_data()) {
                let res = entry.result();

                let result = if res < 0 {
                    Err(Error::from_raw_os_error(-res))
                } else {
                    Ok(res as usize)
                };

                // we don't care if future is dropped or not
                let _ = sender.send(result);

                waker.wake();
            }
        }
    }
}

fn init_ring() {
    static RING_THREAD: Once = Once::new();

    RING_THREAD.call_once(|| {
        thread::spawn(run_ring);
    });
}

pub struct ReadFuture<'a> {
    fd: RawFd,
    buf: &'a mut [u8],
    user_data: u64,
    receiver: Option<Receiver<Result<usize>>>,
}

impl<'a> ReadFuture<'a> {
    fn new(fd: RawFd, buf: &'a mut [u8]) -> Self {
        init_ring();

        let user_data = get_user_data();

        Self {
            fd,
            buf,
            user_data,
            receiver: None,
        }
    }
}

impl Future for ReadFuture<'_> {
    type Output = Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(receiver) = self.receiver.as_mut() {
            return match receiver.try_recv().unwrap() {
                None => {
                    let mut map_guard = get_result_map().lock().unwrap();

                    if let Some((_, waker)) = map_guard.get_mut(&self.user_data) {
                        *waker = cx.waker().clone();
                    }

                    Poll::Pending
                }

                Some(result) => {
                    self.receiver.take(); // make drop judge more easily

                    Poll::Ready(result)
                }
            };
        }

        let (sender, receiver) = oneshot::channel();

        self.receiver.replace(receiver);

        get_result_map()
            .lock()
            .unwrap()
            .insert(self.user_data, (sender, cx.waker().clone()));

        let submission_queue = get_sq();

        unsafe {
            let entry = Read::new(
                Target::Fd(self.fd),
                self.buf.as_mut_ptr(),
                self.buf.len() as _,
            )
            .build()
            .user_data(self.user_data);

            if let Err(_entry) = submission_queue.push(entry) {
                todo!("handle sq is full");
            }

            if let Err(err) = get_ring().submit() {
                return Poll::Ready(Err(err));
            }
        }

        Poll::Pending
    }
}

impl Drop for ReadFuture<'_> {
    fn drop(&mut self) {
        if self.receiver.is_none() {
            return;
        }

        cancel_async_entry(self.user_data);
    }
}

pub struct ReadExactFuture<'a> {
    fd: RawFd,
    buf: &'a mut [u8],
    user_data: u64,
    receiver: Option<Receiver<Result<usize>>>,
}

impl<'a> ReadExactFuture<'a> {
    fn new(fd: RawFd, buf: &'a mut [u8]) -> Self {
        init_ring();

        Self {
            fd,
            buf,
            user_data: get_user_data(),
            receiver: None,
        }
    }
}

impl Future for ReadExactFuture<'_> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            if let Some(receiver) = self.receiver.as_mut() {
                if let Some(result) = receiver.try_recv().unwrap() {
                    let read = match result {
                        Err(err) => return Poll::Ready(Err(err)),
                        Ok(n) => n,
                    };

                    let buf = mem::replace(&mut self.buf, &mut []);
                    self.buf = &mut buf[read..];

                    if self.buf.len() == 0 {
                        return Poll::Ready(Ok(()));
                    }

                    self.receiver.take();
                } else {
                    let mut map_guard = get_result_map().lock().unwrap();

                    if let Some((_, waker)) = map_guard.get_mut(&self.user_data) {
                        *waker = cx.waker().clone();
                    }

                    return Poll::Pending;
                }
            }

            let (sender, receiver) = oneshot::channel();

            self.receiver.replace(receiver);

            get_result_map()
                .lock()
                .unwrap()
                .insert(self.user_data, (sender, cx.waker().clone()));

            let submission_queue = get_sq();

            unsafe {
                let entry = Read::new(
                    Target::Fd(self.fd),
                    self.buf.as_mut_ptr(),
                    self.buf.len() as _,
                )
                .build()
                .user_data(self.user_data);

                if let Err(_entry) = submission_queue.push(entry) {
                    todo!("handle sq is full");
                }

                if let Err(err) = get_ring().submit() {
                    return Poll::Ready(Err(err));
                }
            }

            return Poll::Pending;
        }
    }
}

impl Drop for ReadExactFuture<'_> {
    fn drop(&mut self) {
        if self.receiver.is_none() {
            return;
        }

        cancel_async_entry(self.user_data);
    }
}

pub struct WriteFuture<'a> {
    fd: RawFd,
    buf: &'a [u8],
    user_data: u64,
    receiver: Option<Receiver<Result<usize>>>,
}

impl<'a> WriteFuture<'a> {
    fn new(fd: RawFd, buf: &'a [u8]) -> Self {
        init_ring();

        let user_data = get_user_data();

        Self {
            fd,
            buf,
            user_data,
            receiver: None,
        }
    }
}

impl Future for WriteFuture<'_> {
    type Output = Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(receiver) = self.receiver.as_mut() {
            return match receiver.try_recv().unwrap() {
                None => {
                    let mut map_guard = get_result_map().lock().unwrap();

                    if let Some((_, waker)) = map_guard.get_mut(&self.user_data) {
                        *waker = cx.waker().clone();
                    }

                    Poll::Pending
                }

                Some(result) => {
                    self.receiver.take(); // make drop judge more easily

                    Poll::Ready(result)
                }
            };
        }

        let (sender, receiver) = oneshot::channel();

        self.receiver.replace(receiver);

        get_result_map()
            .lock()
            .unwrap()
            .insert(self.user_data, (sender, cx.waker().clone()));

        let submission_queue = get_sq();

        unsafe {
            let entry = Write::new(Target::Fd(self.fd), self.buf.as_ptr(), self.buf.len() as _)
                .build()
                .user_data(self.user_data);

            if let Err(_entry) = submission_queue.push(entry) {
                todo!("handle sq is full");
            }

            if let Err(err) = get_ring().submit() {
                return Poll::Ready(Err(err));
            }
        }

        Poll::Pending
    }
}

impl Drop for WriteFuture<'_> {
    fn drop(&mut self) {
        if self.receiver.is_none() {
            return;
        }

        cancel_async_entry(self.user_data);
    }
}

pub struct WriteAllFuture<'a> {
    fd: RawFd,
    buf: &'a [u8],
    written: usize,
    write_future: WriteFuture<'a>,
}

impl<'a> WriteAllFuture<'a> {
    pub fn new(fd: RawFd, buf: &'a [u8]) -> Self {
        let write_future = WriteFuture::new(fd, buf);

        Self {
            fd,
            buf,
            written: 0,
            write_future,
        }
    }
}

impl Future for WriteAllFuture<'_> {
    type Output = Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let write_future = &mut self.write_future;

            pin_mut!(write_future);

            let written = match ready!(write_future.poll(cx)) {
                Err(err) => return Poll::Ready(Err(err)),
                Ok(written) => written,
            };

            self.written += written;

            if self.written == self.buf.len() {
                return Poll::Ready(Ok(self.written));
            }

            let new_future = WriteFuture::new(self.fd, &self.buf[self.written..]);

            self.write_future = new_future;
        }
    }
}

fn cancel_async_entry(user_data: u64) {
    let submission_queue = get_sq();

    unsafe {
        let mut cancel_entry = AsyncCancel::new(user_data).build();

        // TODO is there a better way?
        while let Err(entry) = submission_queue.push(cancel_entry) {
            cancel_entry = entry;
        }

        get_ring().submit().unwrap();
    }
}

pub struct OpenFuture {
    path: CString,
    open_how: OpenHow,
    user_data: u64,
    receiver: Option<Receiver<Result<usize>>>,
}

impl OpenFuture {
    fn new<P: AsRef<Path>>(path: P, mode: mode_t, flags: i32) -> Result<Self> {
        let path = match CString::new(path.as_ref().as_os_str().as_bytes()) {
            Err(err) => return Err(Error::new(ErrorKind::InvalidInput, err)),

            Ok(path) => path,
        };

        init_ring();

        let open_how = OpenHow::new().flags(flags as _).mode(mode as _);

        Ok(Self {
            path,
            open_how,
            user_data: get_user_data(),
            receiver: None,
        })
    }
}

impl Future for OpenFuture {
    type Output = Result<File>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(receiver) = self.receiver.as_mut() {
            return match receiver.try_recv().unwrap() {
                None => {
                    let mut map_guard = get_result_map().lock().unwrap();

                    if let Some((_, waker)) = map_guard.get_mut(&self.user_data) {
                        *waker = cx.waker().clone();
                    }

                    Poll::Pending
                }

                Some(result) => {
                    self.receiver.take(); // make drop judge more easily

                    match result {
                        Err(err) => Poll::Ready(Err(err)),
                        Ok(fd) => Poll::Ready(Ok(unsafe { File::from_raw_fd(fd as RawFd) })),
                    }
                }
            };
        }

        let (sender, receiver) = oneshot::channel();

        self.receiver.replace(receiver);

        get_result_map()
            .lock()
            .unwrap()
            .insert(self.user_data, (sender, cx.waker().clone()));

        let submission_queue = get_sq();

        unsafe {
            let open_entry = Openat2::new(libc::AT_FDCWD, self.path.as_ptr(), &self.open_how)
                .build()
                .user_data(self.user_data);

            if let Err(_entry) = submission_queue.push(open_entry) {
                todo!("handle sq is full");
            }

            if let Err(err) = get_ring().submit() {
                return Poll::Ready(Err(err));
            }
        }

        Poll::Pending
    }
}

impl Drop for OpenFuture {
    fn drop(&mut self) {
        if self.receiver.is_none() {
            return;
        }

        cancel_async_entry(self.user_data);
    }
}

#[cfg(test)]
mod tests {
    use std::io::SeekFrom;
    use std::io::{Read, Seek, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn file_open() {
        let dir = tempfile::tempdir().unwrap();

        futures_executor::block_on(async {
            let mut path = dir.path().to_path_buf();

            path.push("test");

            let file: File = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
                .await
                .unwrap();

            let metadata = file.metadata().unwrap();

            assert!(metadata.is_file());
            assert_eq!(metadata.permissions().mode(), libc::S_IFREG | 0o600);
        })
    }

    #[test]
    fn file_read() {
        let mut file = tempfile::tempfile().unwrap();

        futures_executor::block_on(async {
            Write::write(&mut file, b"test").unwrap();

            file.seek(SeekFrom::Start(0)).unwrap();

            let mut buf = vec![0; 4];

            let n = file.read(&mut buf).await.unwrap();

            assert_eq!(n, 4);
            assert_eq!(buf, b"test");
        })
    }

    #[test]
    fn file_write() {
        let mut file = tempfile::tempfile().unwrap();

        futures_executor::block_on(async {
            let n = file.write_all(b"test").await.unwrap();
            assert_eq!(n, 4);

            let mut buf = vec![0; 4];

            let n = Read::read(&mut file, &mut buf).unwrap();

            assert_eq!(n, 4);
            assert_eq!(buf, b"test");
        })
    }

    #[test]
    fn tcp_write() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let addr = listener.local_addr().unwrap();

        let client_stream = TcpStream::connect(addr).unwrap();

        let (mut server_stream, _) = listener.accept().unwrap();

        futures_executor::block_on(async {
            let n = client_stream.write_all(b"test").await.unwrap();
            assert_eq!(n, 4);

            let mut buf = vec![0; 4];

            Read::read_exact(&mut server_stream, &mut buf).unwrap();

            assert_eq!(buf, b"test");
        })
    }

    #[test]
    fn tcp_read() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let addr = listener.local_addr().unwrap();

        let client_stream = TcpStream::connect(addr).unwrap();

        let (mut server_stream, _) = listener.accept().unwrap();

        futures_executor::block_on(async {
            Write::write_all(&mut server_stream, b"test").unwrap();

            let mut buf = vec![0; 4];

            client_stream.read_exact(&mut buf).await.unwrap();

            assert_eq!(buf, b"test");
        })
    }
}
