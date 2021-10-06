use std::collections::HashMap;
use std::ffi::CString;
use std::mem::MaybeUninit;
use std::os::unix::prelude::RawFd;
use std::task::Waker;

use bytes::Bytes;
use io_uring::cqueue::Entry;
use io_uring::types::Timespec;
use nix::sys::socket::SockAddr;

#[derive(Debug)]
pub enum Callback {
    ProvideBuffer {
        group_id: u16,
    },

    CancelReadOrRecv {
        group_id: u16,
    },

    // save the addr in Box so no matter how to move the addr, won't break the pointer that in the
    // io_uring
    CancelConnect {
        addr: Box<SockAddr>,
        fd: RawFd,
    },

    CancelOpenAt {
        path: CString,
    },

    CancelStatx {
        path: CString,
        statx: Box<MaybeUninit<libc::statx>>,
    },

    CancelRenameAt {
        old_path: CString,
        new_path: CString,
    },

    CancelUnlinkAt {
        path: CString,
    },

    Wakeup {
        waker: Waker,
    },

    // save the Timespec in Box so no matter how to move the Timespec, won't break the pointer that
    // in the io_uring
    CancelTimeout {
        timespec: Box<Timespec>,
    },

    // save the peer_addr and addr_size in Box so no matter how to move the peer_addr or addr_size,
    // won't break the pointer that in the io_uring
    CancelAccept {
        peer_addr: Box<libc::sockaddr_storage>,
        addr_size: Box<libc::socklen_t>,
    },

    // save the data so won't break the pointer that in the io_uring
    CancelWriteOrSend {
        data: Bytes,
    },
}

#[derive(Debug, Default)]
pub struct CallbacksAndCompleteEntries {
    // store the cqe and let op future get it
    pub(crate) completion_queue_entries: HashMap<u64, Entry>,

    // store the callback, when reactor acquires a cqe, take out the callback when
    // user_data == cqe.user_data()
    pub(crate) callbacks: HashMap<u64, Callback>,
}
