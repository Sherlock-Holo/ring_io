use std::io;
use std::net::ToSocketAddrs;

use crate::spawn_blocking;

pub mod tcp;
pub mod udp;

/// just [`ToSocketAddrs`] async wrapper
pub async fn to_socket_addrs<A: ToSocketAddrs + Send + 'static>(addr: A) -> io::Result<A::Iter>
where
    A: ToSocketAddrs + Send + 'static,
    A::Iter: Send + 'static,
{
    spawn_blocking(move || addr.to_socket_addrs()).await
}
