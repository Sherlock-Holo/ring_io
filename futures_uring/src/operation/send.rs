use std::future::Future;
use std::io::Result;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use io_uring::opcode::Send as RingSend;
use io_uring::types::Fd;

use crate::cqe_ext::EntryExt;
use crate::Driver;

#[must_use = "Future do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct Send {
    fd: RawFd,
    data: Bytes,
    user_data: Option<u64>,
    flags: Option<libc::c_int>,
    driver: Driver,
}

impl Send {
    pub unsafe fn new<FD: AsRawFd, B: Into<Bytes>>(fd: &FD, data: B, driver: &Driver) -> Self {
        Self {
            fd: fd.as_raw_fd(),
            data: data.into(),
            user_data: None,
            flags: None,
            driver: driver.clone(),
        }
    }

    pub fn flags(mut self, flags: libc::c_int) -> Self {
        self.flags.replace(flags);

        self
    }
}

impl Future for Send {
    type Output = Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let write_cqe = self
                .driver
                .take_cqe_with_waker(user_data, cx.waker())
                .map(|cqe| cqe.ok())
                .transpose()
                .map_err(|err| {
                    // drop won't send useless cancel when error happened
                    self.user_data.take();

                    err
                })?;

            return match write_cqe {
                None => Poll::Pending,
                Some(write_cqe) => {
                    // drop won't send useless cancel
                    self.user_data.take();

                    Poll::Ready(Ok(write_cqe.result() as _))
                }
            };
        }

        let mut send_sqe = RingSend::new(Fd(self.fd), self.data.as_ptr(), self.data.len() as _);
        if let Some(flags) = self.flags {
            send_sqe = send_sqe.flags(flags);
        }

        let send_sqe = send_sqe.build();

        let user_data = self
            .driver
            .push_sqe_with_waker(send_sqe, cx.waker().clone())?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Send {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            // clone the Bytes will still pointer to the same address
            let _ = self
                .driver
                .cancel_write_or_send(user_data, self.data.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc::SyncSender;
    use std::sync::{mpsc, Arc};
    use std::thread;

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
    fn test_send_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        dbg!(addr);

        let handle = thread::spawn(move || TcpStream::connect(addr).unwrap());

        let (tcp1, _) = listener.accept().unwrap();
        let mut tcp2 = handle.join().unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut write = unsafe { Send::new(&tcp1, &b"test"[..], &driver) };

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
            panic!("send is still no ready");
        }
    }
}
