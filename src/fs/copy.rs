use std::io::Result;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use crate::fs::{File, OpenOptions};
use crate::io::{self, Splice};

pub async fn copy<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> Result<u64> {
    let from = File::open(from).await?;

    let perm = from.metadata().await?.permissions();

    let mut to = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create_new(true)
        .mode(perm.mode())
        .open(to)
        .await?;

    if let Ok(mut splice) = Splice::new() {
        if let Ok(copied) = splice.copy(from.as_raw_fd(), to.as_raw_fd()).await {
            return Ok(copied);
        }
    }

    io::copy_buf(from, &mut to).await
}
