pub mod stdin {
    use std::io::{IoSliceMut, Result};
    use std::os::unix::io::{AsRawFd, IntoRawFd, RawFd};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures_util::io::BufReader;
    use futures_util::lock::{Mutex, MutexGuard};
    use futures_util::{AsyncBufRead, AsyncRead};
    use once_cell::sync::OnceCell;

    use crate::io::ring_fd::RingFd;

    #[derive(Debug)]
    pub struct Stdin(&'static Mutex<BufReader<RingFd>>);

    pub fn stdin() -> Stdin {
        static INSTANCE: OnceCell<Mutex<BufReader<RingFd>>> = OnceCell::new();

        let raw = INSTANCE.get_or_init(|| {
            // Safety: stdin is valid
            Mutex::new(BufReader::new(unsafe {
                RingFd::new_file(libc::STDIN_FILENO)
            }))
        });

        Stdin(raw)
    }

    impl Stdin {
        pub async fn lock(&self) -> StdinGuard<'_> {
            StdinGuard {
                raw_guard: self.0.lock().await,
            }
        }
    }

    impl AsRawFd for Stdin {
        fn as_raw_fd(&self) -> RawFd {
            libc::STDIN_FILENO
        }
    }

    impl IntoRawFd for Stdin {
        fn into_raw_fd(self) -> RawFd {
            libc::STDIN_FILENO
        }
    }

    pub struct StdinGuard<'a> {
        raw_guard: MutexGuard<'a, BufReader<RingFd>>,
    }

    impl<'a> AsyncRead for StdinGuard<'a> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<Result<usize>> {
            Pin::new(&mut *self.raw_guard).poll_read(cx, buf)
        }

        fn poll_read_vectored(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &mut [IoSliceMut<'_>],
        ) -> Poll<Result<usize>> {
            Pin::new(&mut *self.raw_guard).poll_read_vectored(cx, bufs)
        }
    }

    impl<'a> AsyncBufRead for StdinGuard<'a> {
        fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
            let this = self.get_mut();

            Pin::new(&mut *this.raw_guard).poll_fill_buf(cx)
        }

        fn consume(mut self: Pin<&mut Self>, amt: usize) {
            Pin::new(&mut *self.raw_guard).consume(amt)
        }
    }
}

pub mod stdout_and_stderr {
    use std::io::IoSlice;
    use std::io::Result;
    use std::os::unix::io::{AsRawFd, IntoRawFd, RawFd};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures_util::lock::{Mutex, MutexGuard};
    use futures_util::AsyncWrite;
    use once_cell::sync::OnceCell;

    use crate::io::ring_fd::RingFd;

    macro_rules! wrapper_async_write {
        () => {
            fn poll_write(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<Result<usize>> {
                Pin::new(&mut *self.0).poll_write(cx, buf)
            }

            fn poll_write_vectored(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                bufs: &[IoSlice<'_>],
            ) -> Poll<Result<usize>> {
                Pin::new(&mut *self.0).poll_write_vectored(cx, bufs)
            }

            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
                Pin::new(&mut *self.0).poll_flush(cx)
            }

            fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
                Pin::new(&mut *self.0).poll_close(cx)
            }
        };
    }

    pub struct Stdout(&'static Mutex<RingFd>);

    pub fn stdout() -> Stdout {
        static INSTANCE: OnceCell<Mutex<RingFd>> = OnceCell::new();

        let raw = INSTANCE.get_or_init(|| {
            // Safety: stdout is valid
            Mutex::new(unsafe { RingFd::new_file(libc::STDOUT_FILENO) })
        });

        Stdout(raw)
    }

    impl Stdout {
        pub async fn lock(&self) -> StdoutGuard<'_> {
            StdoutGuard(self.0.lock().await)
        }
    }

    impl AsRawFd for Stdout {
        fn as_raw_fd(&self) -> RawFd {
            libc::STDOUT_FILENO
        }
    }

    impl IntoRawFd for Stdout {
        fn into_raw_fd(self) -> RawFd {
            libc::STDOUT_FILENO
        }
    }

    pub struct StdoutGuard<'a>(MutexGuard<'a, RingFd>);

    impl<'a> AsyncWrite for StdoutGuard<'a> {
        wrapper_async_write!();
    }

    pub struct Stderr(&'static Mutex<RingFd>);

    pub fn stderr() -> Stderr {
        static INSTANCE: OnceCell<Mutex<RingFd>> = OnceCell::new();

        let raw = INSTANCE.get_or_init(|| {
            // Safety: stderr is valid
            Mutex::new(unsafe { RingFd::new_file(libc::STDERR_FILENO) })
        });

        Stderr(raw)
    }

    impl Stderr {
        pub async fn lock(&self) -> StderrGuard<'_> {
            StderrGuard(self.0.lock().await)
        }
    }

    impl AsRawFd for Stderr {
        fn as_raw_fd(&self) -> RawFd {
            libc::STDERR_FILENO
        }
    }

    impl IntoRawFd for Stderr {
        fn into_raw_fd(self) -> RawFd {
            libc::STDERR_FILENO
        }
    }

    pub struct StderrGuard<'a>(MutexGuard<'a, RingFd>);

    impl<'a> AsyncWrite for StderrGuard<'a> {
        wrapper_async_write!();
    }
}
