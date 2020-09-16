use std::future::Future;
use std::io::{Error, ErrorKind, Result};
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::future;
use nix::sys::socket;
use nix::sys::socket::sockopt::ReuseAddr;
use nix::sys::socket::{AddressFamily, SockFlag, SockProtocol, SockType};
use parking_lot::Mutex;

use crate::drive::{ConnectEvent, DefaultDriver, Drive, Event};
use crate::io::FileDescriptor;
use crate::{drive, from_nix_err};

pub struct UdpSocket<D> {
    fd: FileDescriptor<D>,
}

impl<D> UdpSocket<D> {
    pub async fn bind_with_driver(addr: SocketAddr, driver: D) -> Result<Self> {
        let addr_family = if addr.is_ipv4() {
            AddressFamily::Inet
        } else {
            AddressFamily::Inet6
        };

        let fd = socket::socket(
            addr_family,
            SockType::Datagram,
            SockFlag::SOCK_CLOEXEC,
            SockProtocol::Tcp,
        )
        .map_err(from_nix_err)?;

        socket::setsockopt(fd, ReuseAddr, &true).map_err(from_nix_err)?;

        socket::bind(
            fd,
            &socket::SockAddr::Inet(socket::InetAddr::from_std(&addr)),
        )
        .map_err(from_nix_err)?;

        Ok(Self {
            fd: FileDescriptor::new(fd, driver, None),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        let nix_addr = socket::getsockname(self.fd.as_raw_fd()).map_err(from_nix_err)?;

        match nix_addr {
            nix::sys::socket::SockAddr::Inet(inet_addr) => Ok(inet_addr.to_std()),

            addr => Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid sock addr family {:?}", addr.family()),
            )),
        }
    }

    pub fn connect(&mut self, addr: SocketAddr) -> Connect<D> {
        Connect::new(addr, self)
    }
}

impl UdpSocket<DefaultDriver> {
    #[inline]
    pub async fn bind(addr: SocketAddr) -> Result<Self> {
        Self::bind_with_driver(addr, drive::get_default_driver()).await
    }
}

impl<D: Drive + Unpin> UdpSocket<D> {
    #[inline]
    pub async fn recv(&mut self, buf: &mut [u8]) -> Result<usize> {
        future::poll_fn(|cx| Pin::new(&mut self.fd).poll_recv(cx, buf)).await
    }

    #[inline]
    pub async fn send(&mut self, buf: &[u8]) -> Result<usize> {
        future::poll_fn(|cx| Pin::new(&mut self.fd).poll_send(cx, buf)).await
    }
}

pub struct Connect<'a, D> {
    addr: SocketAddr,
    event: Arc<Event>,
    udp_socket: &'a mut UdpSocket<D>,
    finish: bool,
}

impl<D> Connect<'_, D> {
    pub fn new(addr: SocketAddr, udp_socket: &mut UdpSocket<D>) -> Connect<D> {
        Connect {
            addr,
            event: Arc::new(Event::Nothing),
            udp_socket,
            finish: false,
        }
    }
}

impl<D: Drive + Unpin> Future for Connect<'_, D> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match &*this.event {
            Event::Nothing => {
                let fd = this.udp_socket.fd.as_raw_fd();

                let addr = this.addr;

                /*let event = futures_util::ready!(Pin::new(this.udp_socket.fd.get_driver_mut())
                    .poll_prepare(cx, |sqe, cx| {
                        let connect_event = ConnectEvent::new(fd, &addr);

                        unsafe {
                            sqe.prep_connect(fd, &connect_event.addr);
                        }

                        connect_event.waker.register(cx.waker());

                        Arc::new(Event::Connect(Mutex::new(connect_event)))
                    }))?;

                this.event = event;

                futures_util::ready!(
                    Pin::new(this.udp_socket.fd.get_driver_mut()).poll_submit(cx, false)
                )?;

                Poll::Pending*/
                match Pin::new(this.udp_socket.fd.get_driver_mut()).poll_prepare_with_submit(
                    cx,
                    |sqe, cx| {
                        let connect_event = ConnectEvent::new(fd, &addr);

                        unsafe {
                            sqe.prep_connect(fd, &connect_event.addr);
                        }

                        connect_event.waker.register(cx.waker());

                        Arc::new(Event::Connect(Mutex::new(connect_event)))
                    },
                    Some(false),
                ) {
                    (Poll::Pending, _) => Poll::Pending,
                    (Poll::Ready(result), poll_result) => {
                        this.event = result?;

                        match poll_result {
                            None => {
                                futures_util::ready!(Pin::new(
                                    this.udp_socket.fd.get_driver_mut()
                                )
                                .poll_submit(cx, true))?;

                                Poll::Pending
                            }

                            Some(poll_result) => match poll_result {
                                Poll::Pending => {
                                    futures_util::ready!(Pin::new(
                                        this.udp_socket.fd.get_driver_mut()
                                    )
                                    .poll_submit(cx, true))?;

                                    Poll::Pending
                                }

                                Poll::Ready(result) => {
                                    result?;

                                    Poll::Pending
                                }
                            },
                        }
                    }
                }
            }

            Event::Connect(connect_event) => {
                let mut connect_event = connect_event.lock();

                match connect_event.result.take() {
                    None => {
                        connect_event.waker.register(cx.waker());

                        futures_util::ready!(
                            Pin::new(this.udp_socket.fd.get_driver_mut()).poll_submit(cx, false)
                        )?;

                        Poll::Pending
                    }

                    Some(result) => {
                        result?;

                        this.finish = true;

                        Poll::Ready(Ok(()))
                    }
                }
            }

            _ => unreachable!(),
        }
    }
}

impl<D> Drop for Connect<'_, D> {
    fn drop(&mut self) {
        if !self.finish {
            self.event.cancel();
        }
    }
}
