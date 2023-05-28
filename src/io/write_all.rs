use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures_util::FutureExt;

use crate::buf::{IoBuf, Slice};
use crate::op::Op;
use crate::opcode::Write;
use crate::BufResult;

pub struct WriteAll<'a, B: IoBuf, T: sealed::Writable> {
    io: &'a T,
    buf: Option<Slice<B>>,
    fut: Option<Op<Write<Slice<B>>>>,
    count: usize,
}

impl<'a, B: IoBuf, T: sealed::Writable> WriteAll<'a, B, T> {
    pub(crate) fn new(io: &'a T, buf: B) -> Self {
        Self {
            io,
            buf: Some(buf.slice(..)),
            fut: None,
            count: 0,
        }
    }
}

impl<'a, B: IoBuf, T: sealed::Writable> Future for WriteAll<'a, B, T> {
    type Output = BufResult<usize, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match this.fut.as_mut() {
                None => {
                    let buf = this.buf.take().unwrap();
                    if buf.is_empty() {
                        return Poll::Ready((Ok(this.count), buf.into_inner()));
                    }

                    this.fut.replace(this.io.write_buf(buf));
                }

                Some(fut) => {
                    let result = ready!(fut.poll_unpin(cx));
                    let buf = result.1;
                    let result = result.0;
                    match result {
                        Err(err) => return Poll::Ready((Err(err), buf.into_inner())),

                        Ok(n) => {
                            this.count += n;
                            this.buf.replace(buf.into_inner().slice(this.count..));
                            this.fut.take();
                        }
                    }
                }
            }
        }
    }
}

mod sealed {
    use crate::buf::IoBuf;
    use crate::fs::File;
    use crate::net::tcp::TcpStream;
    use crate::op::Op;
    use crate::opcode::Write;

    pub trait Writable {
        fn write_buf<B: IoBuf>(&self, buf: B) -> Op<Write<B>>;
    }

    impl Writable for File {
        fn write_buf<B: IoBuf>(&self, buf: B) -> Op<Write<B>> {
            self.write(buf)
        }
    }

    impl Writable for TcpStream {
        fn write_buf<B: IoBuf>(&self, buf: B) -> Op<Write<B>> {
            self.write(buf)
        }
    }
}
