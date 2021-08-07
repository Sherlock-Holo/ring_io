pub mod stdin {
    use std::io::{IoSliceMut, Result};
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use futures_util::AsyncRead;
    use once_cell::sync::OnceCell;

    use crate::io::ring_fd::RingFd;

    #[derive(Debug)]
    pub struct Stdin(&'static Mutex<RingFd>);

    pub fn stdin() -> Stdin {
        static INSTANCE: OnceCell<Mutex<RingFd>> = OnceCell::new();

        let raw = INSTANCE.get_or_init(|| {
            // Safety: stdin is valid
            Mutex::new(unsafe { RingFd::new_file(libc::STDIN_FILENO) })
        });

        Stdin(raw)
    }

    impl AsyncRead for Stdin {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<Result<usize>> {
            let mut guard = self.0.lock().unwrap();

            Pin::new(&mut *guard).poll_read(cx, buf)
        }

        fn poll_read_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &mut [IoSliceMut<'_>],
        ) -> Poll<Result<usize>> {
            let mut guard = self.0.lock().unwrap();

            Pin::new(&mut *guard).poll_read_vectored(cx, bufs)
        }
    }
}

pub mod stdout_and_stderr {
    use std::io::IoSlice;
    use std::io::Result;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use futures_util::AsyncWrite;
    use once_cell::sync::OnceCell;

    use crate::io::ring_fd::RingFd;

    pub type Stdout = StdOutput;
    pub type Stderr = StdOutput;

    pub fn stdout() -> Stdout {
        static INSTANCE: OnceCell<Mutex<RingFd>> = OnceCell::new();

        let raw = INSTANCE.get_or_init(|| {
            // Safety: stdin is valid
            Mutex::new(unsafe { RingFd::new_file(libc::STDOUT_FILENO) })
        });

        StdOutput(raw)
    }

    pub fn stderr() -> Stdout {
        static INSTANCE: OnceCell<Mutex<RingFd>> = OnceCell::new();

        let raw = INSTANCE.get_or_init(|| {
            // Safety: stdin is valid
            Mutex::new(unsafe { RingFd::new_file(libc::STDERR_FILENO) })
        });

        StdOutput(raw)
    }

    #[derive(Debug)]
    pub struct StdOutput(&'static Mutex<RingFd>);

    impl AsyncWrite for StdOutput {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize>> {
            let mut guard = self.0.lock().unwrap();

            Pin::new(&mut *guard).poll_write(cx, buf)
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<Result<usize>> {
            let mut guard = self.0.lock().unwrap();

            Pin::new(&mut *guard).poll_write_vectored(cx, bufs)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
            let mut guard = self.0.lock().unwrap();

            Pin::new(&mut *guard).poll_flush(cx)
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
            let mut guard = self.0.lock().unwrap();

            Pin::new(&mut *guard).poll_close(cx)
        }
    }
}
