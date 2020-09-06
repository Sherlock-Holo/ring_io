use std::io::{Error, ErrorKind, Result};
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::unix::io::RawFd;

pub fn socket<A: ToSocketAddrs>(addr: A) -> Result<(RawFd, SocketAddr)> {
    let mut error = Error::new(
        ErrorKind::InvalidInput,
        "could not resolve to any addresses",
    );

    for addr in addr.to_socket_addrs()? {
        let domain = match addr.is_ipv6() {
            true => libc::AF_INET6,
            false => libc::AF_INET,
        };

        let ty = libc::SOCK_STREAM | libc::SOCK_CLOEXEC;
        let protocol = libc::IPPROTO_TCP;

        match unsafe { libc::socket(domain, ty, protocol) } {
            fd if fd >= 0 => return Ok((fd, addr)),
            _ => error = Error::last_os_error(),
        }
    }

    Err(error)
}
