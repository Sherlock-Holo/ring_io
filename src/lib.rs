pub use runtime::{block_on, spawn};

pub mod buf;
mod driver;
pub mod fs;
pub mod io;
pub mod net;
pub mod op;
pub mod opcode;
mod operation;
pub mod runtime;

pub type BufResult<T, B> = (std::io::Result<T>, B);
