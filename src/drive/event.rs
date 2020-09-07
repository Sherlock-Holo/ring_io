use std::io;
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::sync::Mutex;

use futures_util::task::AtomicWaker;

use crate::io::buffer::Buffer;

pub enum Event {
    Nothing,

    Read(Mutex<ReadEvent>),

    Write(Mutex<WriteEvent>),

    Open(Mutex<OpenEvent>),

    Connect(Mutex<ConnectEvent>),
}

impl Event {
    pub fn cancel(&self) {
        match self {
            Event::Nothing | Event::Read(_) | Event::Write(_) => {}
            Event::Open(open_event) => {
                open_event.lock().unwrap().cancel = true;
            }

            Event::Connect(connect_event) => {
                connect_event.lock().unwrap().cancel = true;
            }
        }
    }

    pub fn is_cancel(&self) -> bool {
        match self {
            Event::Nothing | Event::Read(_) | Event::Write(_) => false,
            Event::Open(open_event) => open_event.lock().unwrap().cancel,
            Event::Connect(connect_event) => connect_event.lock().unwrap().cancel,
        }
    }

    pub fn set_result(&self, result: io::Result<usize>) {
        match self {
            Event::Nothing => {}

            Event::Read(read_event) => {
                let mut read_event = read_event.lock().unwrap();

                read_event.result.replace(result);

                read_event.waker.wake();
            }

            Event::Write(write_event) => {
                let mut write_event = write_event.lock().unwrap();

                write_event.result.replace(result);

                write_event.waker.wake();
            }

            Event::Open(open_event) => {
                let mut open_event = open_event.lock().unwrap();

                open_event.result.replace(result);

                open_event.waker.wake();
            }

            Event::Connect(connect_event) => {
                let mut connect_event = connect_event.lock().unwrap();

                connect_event.result.replace(result.map(|_| ()));

                connect_event.waker.wake();
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
