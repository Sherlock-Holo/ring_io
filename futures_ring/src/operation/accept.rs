use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use io_uring::opcode::Accept as RingAccept;
use io_uring::types::Fd;
use libc::c_int;
use nix::sys::socket::InetAddr;

use crate::cqe_ext::EntryExt;
use crate::Driver;

#[derive(Debug)]
#[must_use = "Future do nothing unless you `.await` or poll them"]
pub struct Accept {
    fd: RawFd,
    peer_addr: Option<Box<libc::sockaddr_storage>>,
    addr_size: Option<Box<libc::socklen_t>>,
    user_data: Option<u64>,
    driver: Driver,
    flags: Option<c_int>,
}

impl Accept {
    pub unsafe fn accept<FD: AsRawFd>(fd: &FD, driver: &Driver) -> Self {
        Self {
            fd: fd.as_raw_fd(),
            peer_addr: None,
            addr_size: None,
            user_data: None,
            driver: driver.clone(),
            flags: None,
        }
    }

    pub fn flags<F: Into<Option<c_int>>>(mut self, flags: F) -> Self {
        self.flags = flags.into();

        self
    }
}

impl Future for Accept {
    type Output = Result<(RawFd, SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(user_data) = this.user_data {
            let accept_cqe = this
                .driver
                .take_cqe_with_waker(user_data, cx.waker())
                .map(|cqe| cqe.ok())
                .transpose()
                .map_err(|err| {
                    // drop won't send useless cancel when error happened
                    this.user_data.take();

                    err
                })?;

            return match accept_cqe {
                None => Poll::Pending,
                Some(accept_cqe) => {
                    // drop won't send useless cancel
                    this.user_data.take();

                    let fd = accept_cqe.result();

                    let peer_addr = &**this.peer_addr.as_ref().expect("peer addr is not init");
                    let addr_size = **this.addr_size.as_ref().expect("addr size is not init");

                    let addr = libc_sockaddr_to_socket_addr(peer_addr, addr_size as _)?;

                    Poll::Ready(Ok((fd, addr)))
                }
            };
        }

        // Safety: we use the value after accept success
        let mut peer_addr: Box<libc::sockaddr_storage> = unsafe { Box::new(mem::zeroed()) };
        let mut addr_size: Box<libc::socklen_t> = Box::new(mem::size_of_val(&*peer_addr) as _);

        let peer_addr_ptr = peer_addr.as_mut() as *mut libc::sockaddr_storage;
        let addr_size_ptr = addr_size.as_mut() as *mut _;

        this.peer_addr.replace(peer_addr);
        this.addr_size.replace(addr_size);

        let mut accept_sqe = RingAccept::new(Fd(this.fd), peer_addr_ptr.cast(), addr_size_ptr);
        if let Some(flags) = this.flags {
            accept_sqe = accept_sqe.flags(flags);
        }

        let accept_sqe = accept_sqe.build();

        let user_data = this
            .driver
            .push_sqe_with_waker(accept_sqe, cx.waker().clone())?;

        user_data.map(|user_data| this.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Accept {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            let peer_addr = self.peer_addr.take().expect("peer_addr not init");
            let addr_size = self.addr_size.take().expect("addr_size not init");

            let _ = self.driver.cancel_accept(user_data, peer_addr, addr_size);
        }
    }
}

#[derive(Debug)]
#[must_use = "Stream do nothing unless you `.next().await` or poll them"]
pub struct AcceptStream {
    fd: RawFd,
    fut: Option<Accept>,
    flags: Option<c_int>,
    driver: Driver,
}

impl AcceptStream {
    pub unsafe fn accept<FD: AsRawFd>(fd: &FD, driver: &Driver) -> Self {
        Self {
            fd: fd.as_raw_fd(),
            fut: None,
            flags: None,
            driver: driver.clone(),
        }
    }

    pub fn flags<F: Into<Option<c_int>>>(mut self, flags: F) -> Self {
        self.flags = flags.into();

        self
    }
}

impl Stream for AcceptStream {
    type Item = Result<RawFd>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.fut.as_mut() {
            None => {
                let accept = unsafe { Accept::accept(&self.fd, &self.driver) }.flags(self.flags);
                self.fut.replace(accept);

                self.poll_next(cx)
            }

            Some(fut) => {
                let result = futures_core::ready!(Pin::new(fut).poll(cx));

                self.fut.take();

                Poll::Ready(Some(result.map(|(fd, _)| fd)))
            }
        }
    }
}

fn libc_sockaddr_to_socket_addr(
    storage: &libc::sockaddr_storage,
    len: usize,
) -> Result<SocketAddr> {
    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            assert!(len as usize >= mem::size_of::<libc::sockaddr_in>());

            // Safety: storage is a v4 addr and libc::sockaddr_in and nix::sys::socket::sockaddr_in
            // have the same layout
            let sockaddr_in = unsafe {
                *(storage as *const _ as *const libc::sockaddr_in
                    as *const nix::sys::socket::sockaddr_in)
            };

            Ok(InetAddr::V4(sockaddr_in).to_std())
        }
        libc::AF_INET6 => {
            assert!(len as usize >= mem::size_of::<libc::sockaddr_in6>());

            // Safety: storage is a v6 addr and libc::sockaddr_in6 and
            // nix::sys::socket::sockaddr_in6 have the same layout
            let sockaddr_in6 = unsafe {
                *(storage as *const _ as *const libc::sockaddr_in6
                    as *const nix::sys::socket::sockaddr_in6)
            };

            Ok(InetAddr::V6(sockaddr_in6).to_std())
        }
        _ => Err(Error::new(ErrorKind::InvalidInput, "invalid sock addr")),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::unix::io::FromRawFd;
    use std::sync::mpsc::SyncSender;
    use std::sync::{mpsc, Arc};
    use std::thread;

    use futures_task::ArcWake;
    use futures_util::StreamExt;

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
    fn test_accept_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        dbg!(addr);

        let handle = thread::spawn(move || TcpStream::connect(addr).unwrap());

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut accept = unsafe { Accept::accept(&listener, &driver) }.flags(libc::SOCK_CLOEXEC);

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        assert!(Pin::new(&mut accept)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        let (fd, peer_addr) = if let Poll::Ready(result) =
            Pin::new(&mut accept).poll(&mut Context::from_waker(&waker))
        {
            result.unwrap()
        } else {
            panic!("accept is still no ready");
        };

        let mut tcp1 = unsafe { TcpStream::from_raw_fd(fd) };
        let mut tcp2 = handle.join().unwrap();

        assert_eq!(peer_addr, tcp2.local_addr().unwrap());

        tcp1.write_all(b"test").unwrap();

        let mut buf = vec![0; 4];

        tcp2.read_exact(&mut buf).unwrap();

        assert_eq!(&buf, b"test");
    }

    #[test]
    fn test_accept_stream_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        dbg!(addr);

        let handle1 = thread::spawn(move || TcpStream::connect(addr).unwrap());
        let handle2 = thread::spawn(move || TcpStream::connect(addr).unwrap());

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut accept =
            unsafe { AcceptStream::accept(&listener, &driver) }.flags(libc::SOCK_CLOEXEC);

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        let mut next = accept.next();

        assert!(Pin::new(&mut next)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        let fd1 = if let Poll::Ready(result) =
            Pin::new(&mut next).poll(&mut Context::from_waker(&waker))
        {
            result.unwrap().unwrap()
        } else {
            panic!("connect is still no ready");
        };

        let tcp11 = unsafe { TcpStream::from_raw_fd(fd1) };

        let mut next = accept.next();

        assert!(Pin::new(&mut next)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        let fd2 = if let Poll::Ready(result) =
            Pin::new(&mut next).poll(&mut Context::from_waker(&waker))
        {
            result.unwrap().unwrap()
        } else {
            panic!("connect is still no ready");
        };

        let tcp12 = unsafe { TcpStream::from_raw_fd(fd2) };

        handle1.join().unwrap();
        handle2.join().unwrap();
    }
}
