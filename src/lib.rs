pub use op::Op;
pub use runtime::{block_on, block_on_with_io_uring_builder, spawn};

pub mod buf;
mod driver;
pub mod fs;
pub mod io;
pub mod net;
mod op;
pub mod opcode;
mod operation;
pub mod runtime;

pub type BufResult<T, B> = (std::io::Result<T>, B);
