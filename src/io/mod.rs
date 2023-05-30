pub use write_all::WriteAll;

pub mod ring_fd;
mod write_all;

#[doc(hidden)]
#[macro_export]
macro_rules! fd_trait {
    ($t:path) => {
        impl From<std::os::fd::OwnedFd> for $t {
            fn from(value: std::os::fd::OwnedFd) -> Self {
                use std::os::fd::IntoRawFd;
                $t {
                    fd: value.into_raw_fd(),
                }
            }
        }

        impl From<$t> for std::os::fd::OwnedFd {
            fn from(value: $t) -> Self {
                use std::os::fd::{FromRawFd, IntoRawFd};
                // Safety: fd is valid
                unsafe { std::os::fd::OwnedFd::from_raw_fd(value.into_raw_fd()) }
            }
        }

        impl std::os::fd::FromRawFd for $t {
            unsafe fn from_raw_fd(fd: std::os::fd::RawFd) -> Self {
                Self { fd }
            }
        }

        impl std::os::fd::AsRawFd for $t {
            fn as_raw_fd(&self) -> std::os::fd::RawFd {
                self.fd
            }
        }

        impl std::os::fd::AsFd for $t {
            fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
                if self.fd < 0 {
                    panic!("can't borrow because closed");
                }

                // Safety: fd is valid
                unsafe { std::os::fd::BorrowedFd::borrow_raw(self.fd) }
            }
        }

        impl std::os::fd::IntoRawFd for $t {
            fn into_raw_fd(mut self) -> std::os::fd::RawFd {
                let fd = self.fd;
                self.fd = -1;

                fd
            }
        }
    };
}
