use std::io;
use std::os::fd::RawFd;

use io_uring::opcode;
use io_uring::types::Fd;

use crate::buf::{FixedSizeBufRing, GBuf};
use crate::op::{MultiCompletable, MultiOp};
use crate::operation::{Droppable, Operation, OperationResult};
use crate::runtime::with_runtime_context;

pub struct RecvMulti {
    _buf_ring: FixedSizeBufRing,
}

impl RecvMulti {
    pub(crate) fn new(fd: RawFd, buf_ring: FixedSizeBufRing) -> MultiOp<Self> {
        let entry = opcode::RecvMulti::new(Fd(fd), buf_ring.buf_group()).build();
        let (operation, receiver, data_drop) = Operation::new_with_buf_ring(buf_ring.clone(), true);

        with_runtime_context(|runtime| runtime.submit(entry, operation)).unwrap();

        MultiOp::new(
            Self {
                _buf_ring: buf_ring,
            },
            receiver,
            data_drop,
        )
    }
}

impl MultiCompletable for RecvMulti {
    type Output = io::Result<GBuf>;

    fn complete(&mut self, mut result: OperationResult) -> Option<Self::Output> {
        match result.result {
            Err(err) => {
                if err.raw_os_error() == Some(libc::ENOBUFS) {
                    return None;
                }

                Some(Err(err))
            }

            Ok(_) => {
                let g_buf = result.g_buf.take().expect("gbuf miss");
                if g_buf.is_empty() {
                    None
                } else {
                    Some(Ok(g_buf))
                }
            }
        }
    }

    fn data_drop(self) -> Option<Box<dyn Droppable>> {
        // operation already has the buf_ring
        None
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::{TcpListener, TcpStream, UdpSocket};
    use std::os::fd::AsRawFd;
    use std::sync::Arc;
    use std::{fs, thread};

    use futures_util::TryStreamExt;

    use super::*;
    use crate::block_on;
    use crate::buf::Builder;

    #[test]
    fn test_recv_tcp() {
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

            let buf_ring = Builder::new(10)
                .buf_len(4096)
                .ring_entries(1024)
                .build()
                .unwrap();

            let mut data = vec![];
            let mut recv_multi = RecvMulti::new(stream.as_raw_fd(), buf_ring.clone());
            while let Some(g_buf) = recv_multi.try_next().await.unwrap() {
                data.extend_from_slice(&g_buf);
            }

            join_handle.join().unwrap();

            assert_eq!(file_data.as_ref(), data);
        })
    }

    #[test]
    fn test_recv_multi_udp() {
        block_on(async move {
            let server = UdpSocket::bind("127.0.0.1:0").unwrap();
            let addr = server.local_addr().unwrap();
            let client = UdpSocket::bind("0.0.0.0:0").unwrap();
            client.connect(addr).unwrap();
            let client_addr = client.local_addr().unwrap();

            let buf_ring = Builder::new(4)
                .buf_len(4096)
                .ring_entries(32)
                .build()
                .unwrap();

            let mut recv_multi = RecvMulti::new(client.as_raw_fd(), buf_ring.clone());

            server.send_to(b"hello", client_addr).unwrap();

            assert_eq!(
                recv_multi.try_next().await.unwrap().unwrap().as_ref(),
                b"hello"
            );

            server.send_to(b"world", client_addr).unwrap();

            assert_eq!(
                recv_multi.try_next().await.unwrap().unwrap().as_ref(),
                b"world"
            );
        })
    }
}
