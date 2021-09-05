use std::future::Future;
use std::io::Result;
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::opcode::Connect as RingConnect;
use io_uring::types::Fd;
use nix::sys::socket;
use nix::sys::socket::sockopt::{ReuseAddr, ReusePort};
use nix::sys::socket::{InetAddr, SockAddr, SockFlag, SockProtocol, SockType};
use nix::unistd;

use crate::cqe_ext::EntryExt;
use crate::Driver;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ConnectType {
    Tcp,
}

#[derive(Debug)]
#[must_use = "Future do nothing unless you `.await` or poll them"]
pub struct Connect {
    peer_addr: Option<Box<SockAddr>>,
    fd: Option<RawFd>,
    user_data: Option<u64>,
    reuse_port: bool,
    reuse_addr: bool,
    connect_type: ConnectType,
    driver: Driver,
}

impl Connect {
    pub fn connect_tcp(addr: SocketAddr, driver: &Driver) -> Self {
        let sockaddr = SockAddr::Inet(InetAddr::from_std(&addr));

        Self {
            peer_addr: Some(Box::new(sockaddr)),
            fd: None,
            user_data: None,
            reuse_port: false,
            reuse_addr: false,
            connect_type: ConnectType::Tcp,
            driver: driver.clone(),
        }
    }

    pub fn reuse_port(mut self, reuse_port: bool) -> Self {
        self.reuse_port = reuse_port;

        self
    }

    pub fn reuse_addr(mut self, reuse_addr: bool) -> Self {
        self.reuse_addr = reuse_addr;

        self
    }
}

impl Future for Connect {
    type Output = Result<RawFd>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(user_data) = self.user_data {
            let connect_cqe = self
                .driver
                .take_cqe_with_waker(user_data, cx.waker())
                .map(|cqe| cqe.ok())
                .transpose()?;

            return match connect_cqe {
                None => Poll::Pending,
                Some(connect_cqe) => {
                    // drop won't send useless cancel
                    self.user_data.take();
                    let fd = self.fd.take().unwrap();

                    connect_cqe.ok()?;

                    Poll::Ready(Ok(fd))
                }
            };
        }

        let this = self.get_mut();

        let peer_addr = this.peer_addr.as_ref().expect("peer addr not init");

        let fd = match this.connect_type {
            ConnectType::Tcp => socket::socket(
                peer_addr.family(),
                SockType::Stream,
                SockFlag::SOCK_CLOEXEC,
                SockProtocol::Tcp,
            )?,
        };

        if this.reuse_port {
            socket::setsockopt(fd, ReusePort, &true)?;
        }

        if this.reuse_addr {
            socket::setsockopt(fd, ReuseAddr, &true)?;
        }

        this.fd.replace(fd);

        let (libc_sockaddr, addr_len) = peer_addr.as_ffi_pair();

        let connect_sqe = RingConnect::new(Fd(fd), libc_sockaddr, addr_len).build();

        let user_data = this
            .driver
            .push_sqe_with_waker(connect_sqe, cx.waker().clone())?;

        user_data.map(|user_data| this.user_data.replace(user_data));

        Poll::Pending
    }
}

impl Drop for Connect {
    fn drop(&mut self) {
        if let Some(user_data) = self.user_data {
            let peer_addr = self.peer_addr.take().expect("peer addr not init");

            let _ = self
                .driver
                .cancel_connect(user_data, peer_addr, self.fd.expect("fd not init"));
        } else if let Some(fd) = self.fd {
            let _ = unistd::close(fd);
        }
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
    fn test_connect() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        dbg!(addr);

        let handle = thread::spawn(move || listener.accept().map(|(stream, _)| stream).unwrap());

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut connect = Connect::connect_tcp(addr, &driver)
            .reuse_addr(true)
            .reuse_port(true);

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        assert!(Pin::new(&mut connect)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        let fd = if let Poll::Ready(result) =
            Pin::new(&mut connect).poll(&mut Context::from_waker(&waker))
        {
            result.unwrap()
        } else {
            panic!("connect is still no ready");
        };

        let mut tcp1 = unsafe { TcpStream::from_raw_fd(fd) };
        let mut tcp2 = handle.join().unwrap();

        tcp1.write_all(b"test").unwrap();

        let mut buf = vec![0; 4];

        tcp2.read_exact(&mut buf).unwrap();

        assert_eq!(&buf, b"test");
    }
}
