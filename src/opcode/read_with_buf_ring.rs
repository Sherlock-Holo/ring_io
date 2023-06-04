use std::os::fd::RawFd;
use std::{io, ptr};

use io_uring::opcode::Read;
use io_uring::squeue::Flags;
use io_uring::types::Fd;

use crate::buf::{FixedSizeBufRing, GBuf};
use crate::op::Completable;
use crate::operation::{Droppable, Operation, OperationResult};
use crate::runtime::with_runtime_context;
use crate::Op;

pub struct ReadWithBufRing {
    _buf_ring: FixedSizeBufRing,
}

impl ReadWithBufRing {
    pub(crate) fn new(fd: RawFd, buf_ring: FixedSizeBufRing, offset: u64) -> Op<Self> {
        let entry = Read::new(Fd(fd), ptr::null_mut(), buf_ring.buf_len() as _)
            .offset(offset)
            .buf_group(buf_ring.buf_group())
            .build()
            .flags(Flags::BUFFER_SELECT);
        let (operation, receiver, data_drop) =
            Operation::new_with_buf_ring(buf_ring.clone(), false);

        with_runtime_context(|runtime| runtime.submit(entry, operation)).unwrap();

        Op::new(
            Self {
                _buf_ring: buf_ring,
            },
            receiver,
            data_drop,
        )
    }
}

impl Completable for ReadWithBufRing {
    type Output = io::Result<GBuf>;

    fn complete(self, mut result: OperationResult) -> Self::Output {
        result.result?;

        Ok(result.g_buf.take().expect("gbuf miss"))
    }

    fn data_drop(self) -> Option<Box<dyn Droppable>> {
        // operation already has the buf_ring
        None
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::AsRawFd;
    use std::sync::Arc;
    use std::{fs, thread};

    use super::*;
    use crate::block_on;
    use crate::buf::Builder;

    #[test]
    fn test_read_file() {
        block_on(async move {
            let file_data = fs::read("testdata/book.txt").unwrap();
            let file = File::open("testdata/book.txt").unwrap();
            let buf_ring = Builder::new(1)
                .buf_len(4096)
                .ring_entries(1024)
                .build()
                .unwrap();

            let mut data = vec![];
            let mut offset = 0;

            loop {
                let g_buf = ReadWithBufRing::new(file.as_raw_fd(), buf_ring.clone(), offset)
                    .await
                    .unwrap();

                if g_buf.is_empty() {
                    break;
                }

                data.extend_from_slice(&g_buf);
                offset += g_buf.len() as u64;
            }

            assert_eq!(file_data, data);
        })
    }

    #[test]
    fn test_read_socket() {
        block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let file_data: Arc<[u8]> =
                Arc::from(fs::read("testdata/book.txt").unwrap().into_boxed_slice());
            let data_clone = file_data.clone();

            let join_handle = thread::spawn(move || {
                let mut stream = listener.accept().unwrap().0;
                stream.write_all(&data_clone).unwrap();
            });

            let stream = TcpStream::connect(addr).unwrap();

            let buf_ring = Builder::new(1)
                .buf_len(4096)
                .ring_entries(1024)
                .build()
                .unwrap();

            let mut data = vec![];

            loop {
                let g_buf = ReadWithBufRing::new(stream.as_raw_fd(), buf_ring.clone(), u64::MAX)
                    .await
                    .unwrap();

                if g_buf.is_empty() {
                    break;
                }

                data.extend_from_slice(&g_buf);
            }

            join_handle.join().unwrap();

            assert_eq!(file_data.as_ref(), data);
        })
    }
}
