use std::io::{Result, SeekFrom};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use futures_util::AsyncSeekExt;

use crate::fs::{File, OpenOptions};
use crate::io::{self, Splice};

pub async fn copy<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> Result<u64> {
    let mut from = File::open(from).await?;

    let perm = from.metadata().await?.permissions();

    let mut to = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .mode(perm.mode())
        .open(to)
        .await?;

    if let Ok(mut splice) = Splice::new() {
        if let Ok(copied) = splice.copy(from.as_raw_fd(), to.as_raw_fd()).await {
            return Ok(copied);
        }
    }

    // when fast path failed, fallback to normal copy
    from.seek(SeekFrom::Start(0)).await?;
    to.seek(SeekFrom::Start(0)).await?;

    io::copy_buf(from, &mut to).await
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::io::Write;

    use tempfile::{NamedTempFile, TempDir};

    use super::*;
    use crate::block_on;
    use crate::fs::{self, metadata};

    #[test]
    fn test_copy_file() {
        block_on(async {
            let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
            let file = NamedTempFile::new_in(tmp_dir.path()).unwrap();

            let size = metadata("testdata/book.txt").await.unwrap().len();

            let copied = copy("testdata/book.txt", file.path()).await.unwrap();

            assert_eq!(copied, size);

            let origin_data = fs::read("testdata/book.txt").await.unwrap();

            let copied_data = fs::read(file.path()).await.unwrap();

            assert_eq!(origin_data, copied_data);
        })
    }

    #[test]
    fn test_copy_to_not_exist_file() {
        block_on(async {
            let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
            let mut path = tmp_dir.path().to_path_buf();
            path.push("test");

            let size = metadata("testdata/book.txt").await.unwrap().len();

            let copied = copy("testdata/book.txt", &path).await.unwrap();

            assert_eq!(copied, size);

            let origin_data = fs::read("testdata/book.txt").await.unwrap();

            let copied_data = fs::read(&path).await.unwrap();

            assert_eq!(origin_data, copied_data);
        })
    }

    #[test]
    fn test_copy_truncate_file() {
        block_on(async {
            let tmp_dir = TempDir::new_in(env::temp_dir()).unwrap();
            let mut file = NamedTempFile::new_in(tmp_dir.path()).unwrap();

            file.write_all(b"test").unwrap();

            let size = metadata("testdata/book.txt").await.unwrap().len();

            let copied = copy("testdata/book.txt", file.path()).await.unwrap();

            assert_eq!(copied, size);

            let origin_data = fs::read("testdata/book.txt").await.unwrap();

            let copied_data = fs::read(file.path()).await.unwrap();

            assert_eq!(origin_data, copied_data);
        })
    }
}
