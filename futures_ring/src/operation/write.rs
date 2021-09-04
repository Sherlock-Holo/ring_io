use std::future::Future;
use std::io::Result;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use io_uring::opcode::Write as RingWrite;
use io_uring::types::Fd;
use libc::off_t;

use crate::cqe_ext::EntryExt;
use crate::Driver;

#[must_use = "Future do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct Write {
    fd: RawFd,
    data: Bytes,
    user_data: Option<u64>,
    offset: off_t,
    driver: Driver,
}

impl Write {
    pub unsafe fn new_file<FD: AsRawFd, O: Into<Option<libc::off_t>>, B: Into<Bytes>>(
        fd: &FD,
        offset: O,
        data: B,
        driver: &Driver,
    ) -> Self {
        Self {
            fd: fd.as_raw_fd(),
            data: data.into(),
            user_data: None,
            offset: offset.into().unwrap_or(-1),
            driver: driver.clone(),
        }
    }

    pub unsafe fn new_socket<FD: AsRawFd, B: Into<Bytes>>(
        fd: &FD,
        data: B,
        driver: &Driver,
    ) -> Self {
        Self {
            fd: fd.as_raw_fd(),
            data: data.into(),
            user_data: None,
            offset: 0,
            driver: driver.clone(),
        }
    }
}

impl Future for Write {
    type Output = Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let write_cqe = self
                .driver
                .take_cqe_with_waker(user_data, cx.waker())
                .map(|cqe| cqe.ok())
                .transpose()?;

            return match write_cqe {
                None => Poll::Pending,
                Some(write_cqe) => {
                    // drop won't send useless cancel
                    self.user_data.take();

                    Poll::Ready(Ok(write_cqe.result() as _))
                }
            };
        }

        let write_sqe =
            RingWrite::new(Fd(self.fd), self.data.as_ptr(), self.data.len() as _).build();

        let user_data = self
            .driver
            .push_sqe_with_waker(write_sqe, cx.waker().clone())?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Write {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            // clone the Bytes will still pointer to the same address
            let _ = self.driver.cancel_write(user_data, self.data.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::io::{Read, Seek, SeekFrom};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc::SyncSender;
    use std::sync::{mpsc, Arc};
    use std::thread;

    use bytes::Buf;
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
    fn test_write_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        dbg!(addr);

        let handle = thread::spawn(move || TcpStream::connect(addr).unwrap());

        let (tcp1, _) = listener.accept().unwrap();
        let mut tcp2 = handle.join().unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut write = unsafe { Write::new_socket(&tcp1, &b"test"[..], &driver) };

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        assert!(Pin::new(&mut write)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        if let Poll::Ready(result) = Pin::new(&mut write).poll(&mut Context::from_waker(&waker)) {
            let n = result.unwrap();

            let mut buf = vec![0; n];

            tcp2.read_exact(&mut buf).unwrap();

            assert_eq!(&b"test"[..n], &buf);
        } else {
            panic!("write is still no ready");
        }
    }

    #[test]
    fn test_write_file() {
        let data = fs::read("testdata/book.txt").unwrap();
        let mut write_data = Bytes::copy_from_slice(&data);

        let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut tmp_file = NamedTempFile::new_in(tmp_dir.path()).unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut write = unsafe { Write::new_file(&tmp_file, None, write_data.clone(), &driver) };

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        loop {
            match Pin::new(&mut write).poll(&mut Context::from_waker(&waker)) {
                Poll::Pending => {
                    assert!(reactor.run_at_least_one(None).unwrap() > 0);

                    rx.recv().unwrap();
                }

                Poll::Ready(result) => {
                    let n = result.unwrap();

                    if n == 0 {
                        break;
                    }

                    write_data.advance(n);

                    write =
                        unsafe { Write::new_file(&tmp_file, None, write_data.clone(), &driver) };
                }
            }
        }

        tmp_file.seek(SeekFrom::Start(0)).unwrap();

        let mut written_data = vec![];

        tmp_file.read_to_end(&mut written_data).unwrap();

        assert_eq!(data, written_data);
    }
}
