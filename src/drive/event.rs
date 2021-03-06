use std::io;
use std::net::SocketAddr;
use std::os::unix::io::RawFd;

use futures_util::task::AtomicWaker;
use parking_lot::Mutex;

use crate::io::buffer::Buffer;

pub enum Event {
    Nothing,

    Read(Mutex<ReadEvent>),

    Write(Mutex<WriteEvent>),

    Open(Mutex<OpenEvent>),

    Connect(Mutex<ConnectEvent>),

    Accept(Mutex<AcceptEvent>),

    Recv(Mutex<RecvEvent>),

    Send(Mutex<SendEvent>),
}

impl Event {
    pub fn cancel(&self) {
        match self {
            Event::Nothing | Event::Read(_) | Event::Write(_) | Event::Recv(_) | Event::Send(_) => {
            }
            Event::Open(open_event) => {
                open_event.lock().cancel = true;
            }

            Event::Connect(connect_event) => {
                connect_event.lock().cancel = true;
            }

            Event::Accept(accept_event) => {
                accept_event.lock().cancel = true;
            }
        }
    }

    pub fn is_cancel(&self) -> bool {
        match self {
            Event::Nothing | Event::Read(_) | Event::Write(_) | Event::Recv(_) | Event::Send(_) => {
                false
            }
            Event::Open(open_event) => open_event.lock().cancel,
            Event::Connect(connect_event) => connect_event.lock().cancel,
            Event::Accept(accept_event) => accept_event.lock().cancel,
        }
    }

    pub fn set_result(&self, result: io::Result<usize>) {
        match self {
            Event::Nothing => {}

            Event::Read(read_event) => {
                let mut read_event = read_event.lock();

                read_event.result.replace(result);

                read_event.waker.wake();
            }

            Event::Write(write_event) => {
                let mut write_event = write_event.lock();

                write_event.result.replace(result);

                write_event.waker.wake();
            }

            Event::Open(open_event) => {
                let mut open_event = open_event.lock();

                open_event.result.replace(result);

                open_event.waker.wake();
            }

            Event::Connect(connect_event) => {
                let mut connect_event = connect_event.lock();

                connect_event.result.replace(result.map(|_| ()));

                connect_event.waker.wake();
            }

            Event::Accept(accept_event) => {
                let mut accept_event = accept_event.lock();

                accept_event.result.replace(result);

                accept_event.waker.wake();
            }

            Event::Recv(recv_event) => {
                let mut recv_event = recv_event.lock();

                recv_event.result.replace(result);

                recv_event.waker.wake();
            }

            Event::Send(send_event) => {
                let mut send_event = send_event.lock();

                send_event.result.replace(result);

                send_event.waker.wake();
            }
        }
    }
}

pub struct ReadEvent {
    pub buf: Option<Buffer>,
    pub waker: AtomicWaker,
    pub result: Option<io::Result<usize>>,
}

impl ReadEvent {
    pub fn new(buf: Buffer) -> Self {
        Self {
            buf: Some(buf),
            waker: Default::default(),
            result: None,
        }
    }
}

pub struct WriteEvent {
    pub buf: Option<Buffer>,
    pub waker: AtomicWaker,
    pub result: Option<io::Result<usize>>,
}

impl WriteEvent {
    pub fn new(buf: Buffer) -> Self {
        Self {
            buf: Some(buf),
            waker: Default::default(),
            result: None,
        }
    }
}

#[derive(Default)]
pub struct OpenEvent {
    pub waker: AtomicWaker,
    pub result: Option<io::Result<usize>>,
    cancel: bool,
}

impl OpenEvent {
    pub fn new() -> Self {
        Self {
            waker: Default::default(),
            result: None,
            cancel: false,
        }
    }
}

impl Drop for OpenEvent {
    fn drop(&mut self) {
        if self.cancel {
            if let Some(Ok(fd)) = self.result.take() {
                let _ = nix::unistd::close(fd as _);
            }
        }
    }
}

pub struct ConnectEvent {
    pub waker: AtomicWaker,
    pub result: Option<io::Result<()>>,
    pub fd: RawFd,
    pub addr: Box<iou::SockAddr>,
    cancel: bool,
}

impl ConnectEvent {
    pub fn new(fd: RawFd, addr: &SocketAddr) -> Self {
        Self {
            waker: Default::default(),
            result: None,
            fd,
            addr: Box::new(iou::SockAddr::Inet(iou::InetAddr::from_std(addr))),
            cancel: false,
        }
    }
}

impl Drop for ConnectEvent {
    fn drop(&mut self) {
        if self.cancel {
            let _ = nix::unistd::close(self.fd as _);
        }
    }
}

#[derive(Default)]
pub struct AcceptEvent {
    pub waker: AtomicWaker,
    pub result: Option<io::Result<usize>>,
    cancel: bool,
}

impl AcceptEvent {
    pub fn new() -> Self {
        Self {
            waker: Default::default(),
            result: None,
            cancel: false,
        }
    }
}

impl Drop for AcceptEvent {
    fn drop(&mut self) {
        if self.cancel {
            if let Some(Ok(fd)) = self.result {
                let _ = nix::unistd::close(fd as _);
            }
        }
    }
}

pub struct RecvEvent {
    pub buf: Option<Buffer>,
    pub waker: AtomicWaker,
    pub result: Option<io::Result<usize>>,
}

impl RecvEvent {
    pub fn new(buf: Buffer) -> Self {
        Self {
            buf: Some(buf),
            waker: Default::default(),
            result: None,
        }
    }
}

pub struct SendEvent {
    pub buf: Option<Buffer>,
    pub waker: AtomicWaker,
    pub result: Option<io::Result<usize>>,
}

impl SendEvent {
    pub fn new(buf: Buffer) -> Self {
        Self {
            buf: Some(buf),
            waker: Default::default(),
            result: None,
        }
    }
}
