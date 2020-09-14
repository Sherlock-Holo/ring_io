use std::io::Result;
use std::path::Path;

use futures_util::AsyncReadExt;

pub use file::File;
pub use open::*;

mod file;
mod metadata;
mod open;

pub async fn read(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    let mut file = File::open(path).await?;

    let mut buf = Vec::with_capacity(1024);

    file.read_to_end(&mut buf).await?;

    Ok(buf)
}

pub async fn read_to_string(path: impl AsRef<Path>) -> Result<String> {
    let mut file = File::open(path).await?;

    let mut string = String::with_capacity(1024);

    file.read_to_string(&mut string).await?;

    Ok(string)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn test_read() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();

        temp_file.as_file_mut().write_all(b"test").unwrap();
        temp_file.as_file_mut().flush().unwrap();

        let path = temp_file.path();

        futures_executor::block_on(async {
            let data = read(path).await.unwrap();

            assert_eq!(data, b"test");
        })
    }

    #[test]
    fn test_read_to_string() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();

        temp_file.as_file_mut().write_all(b"test").unwrap();
        temp_file.as_file_mut().flush().unwrap();

        let path = temp_file.path();

        futures_executor::block_on(async {
            let data = read_to_string(path).await.unwrap();

            assert_eq!(data, "test");
        })
    }
}
