use std::io;

use flume::Sender;

use crate::buf::Builder;

pub mod driver;
pub mod runtime;

pub enum IoUringModify {
    RegisterBufRing(Builder, Sender<io::Result<()>>),
    UnRegisterBufRing(u16),
}
