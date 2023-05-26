use std::io;

pub mod buf;
mod driver;
pub mod op;
pub mod opcode;
mod operation;
mod runtime;

pub type BufResult<T, B> = (io::Result<T>, B);
