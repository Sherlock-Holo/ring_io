use std::io;
use std::path::Path;

pub use file::File;
pub use open_options::OpenOptions;

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

pub async fn create<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .await?;
    let _ = file.close().await;

    Ok(())
}
