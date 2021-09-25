use std::future::Future;
use std::io::{Error, Result};
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll};

use io_uring::cqueue::buffer_select;
use io_uring::opcode::Read as RingRead;
use io_uring::squeue::Flags;
use io_uring::types::Fd;
use libc::off_t;

use crate::buffer::Buffer;
use crate::cqe_ext::EntryExt;
use crate::Driver;

#[must_use = "Future do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct Read {
    fd: RawFd,
    want_buf_size: usize,
    group_id: Option<u16>,
    user_data: Option<u64>,
    offset: off_t,
    driver: Driver,
}

impl Read {
    pub unsafe fn new_file<FD: AsRawFd, O: Into<Option<libc::off_t>>>(
        fd: &FD,
        want_size: usize,
        offset: O,
        driver: &Driver,
    ) -> Self {
        Self {
            fd: fd.as_raw_fd(),
            want_buf_size: want_size.next_power_of_two().min(65536),
            group_id: None,
            user_data: None,
            offset: offset.into().unwrap_or(-1),
            driver: driver.clone(),
        }
    }

    pub unsafe fn new_socket<FD: AsRawFd>(fd: &FD, want_size: usize, driver: &Driver) -> Self {
        Self {
            fd: fd.as_raw_fd(),
            want_buf_size: want_size.next_power_of_two().min(65536),
            group_id: None,
            user_data: None,
            offset: 0,
            driver: driver.clone(),
        }
    }
}

impl Future for Read {
    type Output = Result<Buffer>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        enum ReadBufferState {
            None,
            Buffer(Buffer),
            ErrNoBuf,
        }

        match (self.group_id, self.user_data) {
            (Some(group_id), Some(user_data)) => {
                let buffer = match self.driver.take_cqe_with_waker(user_data, cx.waker()) {
                    None => Ok(ReadBufferState::None),
                    Some(cqe) => {
                        // although read is error, we also need to check if buffer is used or not
                        if cqe.is_err() {
                            if let Some(buffer_id) = buffer_select(cqe.flags()) {
                                self.driver
                                    .give_back_buffer_with_id(group_id, buffer_id, true);
                            }

                            if cqe.result() == -libc::ENOBUFS {
                                // we use a no available buffer group buffer, that's so strange
                                // but we can retry to avoid read failed

                                Ok(ReadBufferState::ErrNoBuf)
                            } else {
                                Err(Error::from_raw_os_error(-cqe.result()))
                            }
                        } else {
                            let buffer_id =
                                buffer_select(cqe.flags()).expect("read success but no buffer id");

                            Ok(ReadBufferState::Buffer(self.driver.take_buffer(
                                self.want_buf_size,
                                group_id,
                                buffer_id,
                                cqe.result() as _,
                            )))
                        }
                    }
                }
                .map_err(|err| {
                    // drop won't send useless cancel when error happened
                    self.user_data.take();

                    err
                })?;

                match buffer {
                    ReadBufferState::None => Poll::Pending,
                    ReadBufferState::Buffer(buffer) => {
                        // drop won't send useless cancel
                        self.user_data.take();
                        self.group_id.take();

                        Poll::Ready(Ok(buffer))
                    }
                    ReadBufferState::ErrNoBuf => self.poll(cx),
                }
            }

            (None, Some(_)) => unreachable!(),

            (Some(group_id), None) => {
                let sqe = RingRead::new(Fd(self.fd), ptr::null_mut(), self.want_buf_size as _)
                    .offset(self.offset)
                    .buf_group(group_id)
                    .build()
                    .flags(Flags::BUFFER_SELECT);

                let user_data = self.driver.push_sqe_with_waker(sqe, cx.waker().clone())?;

                user_data.map(|user_data| self.user_data.replace(user_data));

                Poll::Pending
            }

            (None, None) => {
                match self
                    .driver
                    .select_group_buffer(self.want_buf_size, cx.waker())?
                {
                    None => Poll::Pending,
                    Some(group_id) => {
                        let sqe =
                            RingRead::new(Fd(self.fd), ptr::null_mut(), self.want_buf_size as _)
                                .offset(self.offset)
                                .buf_group(group_id)
                                .build()
                                .flags(Flags::BUFFER_SELECT);

                        let user_data = self.driver.push_sqe_with_waker(sqe, cx.waker().clone())?;

                        self.group_id.replace(group_id);
                        user_data.map(|user_data| self.user_data.replace(user_data));

                        Poll::Pending
                    }
                }
            }
        }
    }
}

impl Drop for Read {
    fn drop(&mut self) {
        match (self.group_id, self.user_data) {
            (Some(group_id), Some(user_data)) => {
                let _ = self.driver.cancel_read(user_data, group_id);
            }

            (Some(group_id), None) => {
                self.driver
                    .decrease_on_fly_for_not_use_group_buffer(self.want_buf_size, group_id);
            }

            (None, Some(_)) => unreachable!(),

            (None, None) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc::{self, SyncSender};
    use std::sync::Arc;
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
    fn test_read_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        dbg!(addr);

        let handle = thread::spawn(move || TcpStream::connect(addr).unwrap());

        let (mut tcp1, _) = listener.accept().unwrap();
        let tcp2 = handle.join().unwrap();

        tcp1.write_all(b"test").unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut read = unsafe { Read::new_socket(&tcp2, 100, &driver) };

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        // preparing the ProviderBuffer
        assert!(Pin::new(&mut read)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        // wait ProviderBuffer become ready
        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        // preparing the read
        assert!(Pin::new(&mut read)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());

        // wait read become ready
        assert_eq!(reactor.run_at_least_one(None).unwrap(), 1);

        rx.recv().unwrap();

        if let Poll::Ready(result) =
            Pin::new(&mut read).poll(&mut Context::from_waker(futures_task::noop_waker_ref()))
        {
            let buffer = result.unwrap();

            assert_eq!(b"test", buffer.as_ref());
        } else {
            panic!("read is still no ready");
        }
    }

    #[test]
    fn test_read_file() {
        let file = File::open("testdata/book.txt").unwrap();
        let file_content = fs::read("testdata/book.txt").unwrap();

        let (driver, mut reactor) = Driver::builder().build().unwrap();

        let mut read = unsafe { Read::new_file(&file, 100, None, &driver) };

        let (tx, rx) = mpsc::sync_channel(1);

        let waker = Arc::new(ArcWaker { tx });
        let waker = futures_task::waker(waker);

        let mut read_content = vec![];
        loop {
            match Pin::new(&mut read).poll(&mut Context::from_waker(&waker)) {
                Poll::Pending => {
                    assert!(reactor.run_at_least_one(None).unwrap() > 0);

                    rx.recv().unwrap();
                }

                Poll::Ready(result) => {
                    let buffer = result.unwrap();

                    // read to the end
                    if buffer.is_empty() {
                        break;
                    }

                    read_content.extend_from_slice(&buffer);

                    read = unsafe { Read::new_file(&file, 100, None, &driver) };
                }
            }
        }

        assert_eq!(file_content, read_content);
    }
}
