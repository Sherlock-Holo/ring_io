//! Utilities for working with buffers.
//!
//! `io-uring` APIs require passing ownership of buffers to the runtime. The
//! crate defines [`IoBuf`] and [`IoBufMut`] traits which are implemented by buffer
//! types that respect the `io-uring` contract.
//!
//! thanks tokio_uring, these codes is fork from tokio_uring::buf

pub use io_buf::IoBuf;
pub use io_buf_mut::IoBufMut;
pub use ring_buffer::{Builder, FixedSizeBufRing, GBuf};
pub use slice::Slice;

mod io_buf;
mod io_buf_mut;
mod ring_buffer;
mod slice;

pub(crate) fn deref(buf: &impl IoBuf) -> &[u8] {
    // Safety: the `IoBuf` trait is marked as unsafe and is expected to be
    // implemented correctly.
    unsafe { std::slice::from_raw_parts(buf.stable_ptr(), buf.bytes_init()) }
}

pub(crate) fn deref_mut(buf: &mut impl IoBufMut) -> &mut [u8] {
    // Safety: the `IoBufMut` trait is marked as unsafe and is expected to be
    // implemented correct.
    unsafe { std::slice::from_raw_parts_mut(buf.stable_mut_ptr(), buf.bytes_init()) }
}
