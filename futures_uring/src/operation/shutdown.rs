use std::future::Future;
use std::io::Result;
use std::net::Shutdown as HowShutdown;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::opcode::Shutdown as RingShutdown;
use io_uring::types::Fd;

use crate::cqe_ext::EntryExt;
use crate::Driver;

#[must_use = "Future do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct Shutdown {
    fd: RawFd,
    how: libc::c_int,
    user_data: Option<u64>,
    driver: Driver,
}

impl Shutdown {
    pub unsafe fn new<FD: AsRawFd>(fd: &FD, how: HowShutdown, driver: &Driver) -> Self {
        let how = match how {
            HowShutdown::Read => libc::SHUT_RD,
            HowShutdown::Write => libc::SHUT_WR,
            HowShutdown::Both => libc::SHUT_RDWR,
        };

        Self {
            fd: fd.as_raw_fd(),
            how,
            user_data: None,
            driver: driver.clone(),
        }
    }
}

impl Future for Shutdown {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let shutdown_sqe = self
                .driver
                .take_cqe_with_waker(user_data, cx.waker())
                .map(|cqe| cqe.ok())
                .transpose()
                .map_err(|err| {
                    // drop won't send useless cancel when error happened
                    self.user_data.take();

                    err
                })?;

            return if shutdown_sqe.is_some() {
                // drop won't send useless cancel
                self.user_data.take();

                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            };
        }

        let shutdown_sqe = RingShutdown::new(Fd(self.fd), self.how).build();

        let user_data = self
            .driver
            .push_sqe_with_waker(shutdown_sqe, cx.waker().clone())?;

        user_data.map(|user_data| self.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Shutdown {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            let _ = self.driver.cancel_normal(user_data);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{ErrorKind, Read, Write};
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
    fn test_shutdown_read() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        dbg!(addr);

        let handle = thread::spawn(move || TcpStream::connect(addr).unwrap());

        let (mut tcp1, _) = listener.accept().unwrap();
        let mut tcp2 = handle.join().unwrap();

        tcp1.write_all(b"test").unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut shutdown = unsafe { Shutdown::new(&tcp2, HowShutdown::Read, &driver) };

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        assert!(Pin::new(&mut shutdown)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        if let Poll::Ready(result) = Pin::new(&mut shutdown).poll(&mut Context::from_waker(&waker))
        {
            result.unwrap()
        } else {
            panic!("shutdown is still no ready");
        };

        // clear the buffer
        tcp2.read_exact(&mut [0; 4]).unwrap();

        assert_eq!(tcp2.read(&mut [0; 1]).unwrap(), 0);
    }

    #[test]
    fn test_shutdown_write() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        dbg!(addr);

        let handle = thread::spawn(move || TcpStream::connect(addr).unwrap());

        let (mut tcp1, _) = listener.accept().unwrap();
        let mut tcp2 = handle.join().unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut shutdown = unsafe { Shutdown::new(&tcp2, HowShutdown::Write, &driver) };

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        assert!(Pin::new(&mut shutdown)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        if let Poll::Ready(result) = Pin::new(&mut shutdown).poll(&mut Context::from_waker(&waker))
        {
            result.unwrap()
        } else {
            panic!("shutdown is still no ready");
        };

        assert_eq!(
            tcp2.write_all(b"test").unwrap_err().kind(),
            ErrorKind::BrokenPipe
        );

        assert_eq!(tcp1.read(&mut [0; 1]).unwrap(), 0);
    }

    #[test]
    fn test_shutdown_read_and_write() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        dbg!(addr);

        let handle = thread::spawn(move || TcpStream::connect(addr).unwrap());

        let (mut tcp1, _) = listener.accept().unwrap();
        let mut tcp2 = handle.join().unwrap();

        tcp1.write_all(b"test").unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut shutdown = unsafe { Shutdown::new(&tcp2, HowShutdown::Both, &driver) };

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        assert!(Pin::new(&mut shutdown)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        if let Poll::Ready(result) = Pin::new(&mut shutdown).poll(&mut Context::from_waker(&waker))
        {
            result.unwrap()
        } else {
            panic!("shutdown is still no ready");
        };

        assert_eq!(
            tcp2.write_all(b"test").unwrap_err().kind(),
            ErrorKind::BrokenPipe
        );

        // clear the buffer
        tcp2.read_exact(&mut [0; 4]).unwrap();

        assert_eq!(tcp2.read(&mut [0; 1]).unwrap(), 0);
    }
}
