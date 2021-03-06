use std::io::{Result, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::os::unix::io::FromRawFd;
use std::os::unix::io::IntoRawFd;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_io::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWrite};

use crate::drive;
use crate::drive::{DefaultDriver, Drive};
use crate::fs::Open;
use crate::io::FileDescriptor;

pub struct File<D> {
    fd: FileDescriptor<D>,
}

impl<D> File<D> {
    pub(crate) fn new(fd: FileDescriptor<D>) -> Self {
        File { fd }
    }
}

impl File<DefaultDriver> {
    pub fn open(path: impl AsRef<Path>) -> Open<DefaultDriver> {
        FileDescriptor::open_with_driver(path, drive::get_default_driver())
    }
}

impl<D: Drive + Unpin> AsyncBufRead for File<D> {
    #[inline]
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        let this = self.get_mut();

        Pin::new(&mut this.fd).poll_fill_buf(cx)
    }

    #[inline]
    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.fd).consume(amt)
    }
}

impl<D: Drive + Unpin> AsyncRead for File<D> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.fd).poll_read(cx, buf)
    }
}

impl<D: Drive + Unpin> AsyncWrite for File<D> {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.fd).poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.fd).poll_flush(cx)
    }

    #[inline]
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.fd).poll_close(cx)
    }
}

impl<D: Drive + Unpin> AsyncSeek for File<D> {
    #[inline]
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<Result<u64>> {
        Pin::new(&mut self.fd).poll_seek(cx, pos)
    }
}

impl<D> AsRawFd for File<D> {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl<D> IntoRawFd for File<D> {
    fn into_raw_fd(self) -> RawFd {
        self.fd.into_raw_fd()
    }
}

impl FromRawFd for File<DefaultDriver> {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        File::new(FileDescriptor::new(fd, drive::get_default_driver(), 0))
    }
}

impl From<std::fs::File> for File<DefaultDriver> {
    fn from(std_file: std::fs::File) -> Self {
        unsafe { Self::from_raw_fd(std_file.into_raw_fd()) }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, SeekFrom, Write};

    use futures_util::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    use crate::fs::OpenOptions;

    use super::*;

    #[test]
    fn test_file_open() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();

        let path = temp_file.path();

        futures_executor::block_on(async {
            let _file = File::open(path).await.unwrap();
        })
    }

    #[test]
    fn test_file_read() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();

        temp_file.as_file_mut().write_all(b"test").unwrap();
        temp_file.as_file_mut().flush().unwrap();

        let path = temp_file.path();

        futures_executor::block_on(async {
            let mut file = File::open(path).await.unwrap();

            let mut buf = vec![0; 4];

            file.read_exact(&mut buf).await.unwrap();

            assert_eq!(b"test".as_ref(), buf.as_slice());
        })
    }

    #[test]
    fn test_file_write() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();

        let path = temp_file.path();

        futures_executor::block_on(async {
            let mut file = OpenOptions::new().write(true).open(path).await.unwrap();

            file.write_all(b"test").await.unwrap();
            file.flush().await.unwrap();
        });

        let mut buf = vec![0; 4];

        temp_file.read_exact(&mut buf).unwrap();

        assert_eq!(b"test".as_ref(), buf.as_slice());
    }

    #[test]
    fn test_file_write_read() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();

        let path = temp_file.path();

        futures_executor::block_on(async {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .await
                .unwrap();

            file.write_all(b"test").await.unwrap();
            file.flush().await.unwrap();

            assert_eq!(file.seek(SeekFrom::Start(0)).await.unwrap(), 0);

            let mut buf = vec![0; 4];

            file.read_exact(&mut buf).await.unwrap();

            assert_eq!(b"test".as_ref(), buf.as_slice());
        })
    }
}
