pub use op::Op;
pub use runtime::{block_on, spawn, spawn_blocking};

pub mod buf;
pub mod fs;
pub mod io;
pub mod net;
mod op;
pub mod opcode;
mod operation;
mod per_thread;
pub mod runtime;
mod thread_pool;

pub type BufResult<T, B> = (std::io::Result<T>, B);
