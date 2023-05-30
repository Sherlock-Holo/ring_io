use std::os::fd::RawFd;

use io_uring::opcode;
use io_uring::types::Fd;

use crate::buf::IoBufMut;
use crate::op::{Completable, Op};
use crate::operation::{Droppable, Operation, OperationResult};
use crate::runtime::with_runtime_context;
use crate::BufResult;

pub struct Read<T: IoBufMut> {
    buffer: T,
}

impl<T: IoBufMut> Read<T> {
    pub(crate) fn new(fd: RawFd, mut buf: T, offset: u64) -> Op<Self> {
        let entry = opcode::Read::new(Fd(fd), buf.stable_mut_ptr(), buf.bytes_total() as _)
            .offset(offset)
            .build();
        let (operation, receiver, data_drop) = Operation::new();

        with_runtime_context(|runtime| runtime.submit(entry, operation)).unwrap();

        Op::new(Self { buffer: buf }, receiver, data_drop)
    }
}

impl<T: IoBufMut> Completable for Read<T> {
    type Output = BufResult<usize, T>;

    fn complete(mut self, result: OperationResult) -> Self::Output {
        let result = result.result;

        if let Ok(n) = result {
            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe {
                self.buffer.set_init(n as _);
            }
        }

        (result.map(|n| n as _), self.buffer)
    }

    fn data_drop(self) -> Option<Box<dyn Droppable>> {
        Some(Box::new(self.buffer))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::AsRawFd;
    use std::{fs, thread};

    use super::*;
    use crate::runtime::block_on;

    #[test]
    fn test_file_read() {
        let book = fs::read("testdata/book.txt").unwrap();
        let file = File::open("testdata/book.txt").unwrap();

        block_on(async move {
            let fd = file.as_raw_fd();
            let mut offset = 0;
            let mut data = vec![];
            let mut buf = vec![0; 4096];

            loop {
                let result = Read::new(fd, buf, offset).await;
                buf = result.1;
                let n = result.0.unwrap();

                data.extend_from_slice(&buf[..n]);
                offset += n as u64;

                if book.len() == data.len() {
                    break;
                }
            }

            assert_eq!(book, data);
        })
    }

    #[test]
    fn test_socket_read() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let join_handle = thread::spawn(move || listener.accept().unwrap().0);

        let mut tcp1 = TcpStream::connect(addr).unwrap();
        let tcp2 = join_handle.join().unwrap();

        tcp1.write_all(b"test").unwrap();

        let fd = tcp2.as_raw_fd();
        block_on(async move {
            let buf = vec![0; 4];

            let (result, buf) = Read::new(fd, buf, 0).await;
            let n = result.unwrap();
            assert_eq!(n, 4);

            assert_eq!(buf, b"test");
        })
    }
}
