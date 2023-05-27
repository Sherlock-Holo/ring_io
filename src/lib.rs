use std::io;

pub use runtime::block_on;

pub mod buf;
mod driver;
pub mod fs;
pub mod net;
pub mod op;
pub mod opcode;
mod operation;
mod runtime;

pub type BufResult<T, B> = (io::Result<T>, B);
