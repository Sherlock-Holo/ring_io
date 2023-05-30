use std::io;
use std::path::Path;

pub use file::File;
pub use open_options::OpenOptions;

use crate::buf::IoBuf;
use crate::BufResult;

mod file;
mod open_options;

pub async fn read<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
    let mut file = File::open(path).await?;
    let mut data = vec![];
    let mut buf = vec![0; 4096];

    loop {
        let result = file.read(buf).await;
        buf = result.1;
        let n = result.0?;
        if n == 0 {
            break;
        }

        data.extend_from_slice(&buf[..n])
    }

    let _ = file.close().await;

    Ok(data)
}

pub async fn write<P: AsRef<Path>, B: IoBuf>(path: P, data: B) -> BufResult<usize, B> {
    match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await
    {
        Err(err) => (Err(err), data),
        Ok(file) => file.write_all(data).await,
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;
    use crate::block_on;

    #[test]
    fn test_write() {
        block_on(async move {
            let dir = temp_dir();
            let path = dir.join("test.txt");

            assert_eq!(write(&path, "test").await.0.unwrap(), 4);
            assert_eq!(std::fs::read(path).unwrap(), b"test");
        })
    }

    #[test]
    fn test_read() {
        block_on(async move {
            let mut file = NamedTempFile::new().unwrap();
            file.write_all(b"test").unwrap();

            assert_eq!(read(file.path()).await.unwrap(), b"test");
        })
    }
}
