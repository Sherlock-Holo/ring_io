use std::fs::Permissions;
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct Metadata {
    accessed: SystemTime,
    created: SystemTime,
    modified: SystemTime,
    file_type: FileType,
    length: usize,
    permissions: Permissions,
}

#[derive(Debug, Copy, Clone)]
pub struct FileType {}

struct MetadataFuture {
    statx: Box<libc::statx>,
}
