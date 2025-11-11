#![doc = include_str!("../README.md")]
#![allow(clippy::multiple_inherent_impl)]

#[cfg(not(unix))]
mod default;
#[cfg(unix)]
mod unix;

use std::io;

use socket2::SockRef;
use uni_addr::{UniAddr, UniAddrInner};

#[cfg(not(unix))]
pub use self::default::{OwnedReadHalf, OwnedWriteHalf, UniStream};
#[cfg(unix)]
pub use self::unix::{OwnedReadHalf, OwnedWriteHalf, UniStream};

#[derive(Debug)]
/// A unified listener that can listen on both TCP and Unix domain sockets.
pub enum UniListener {
    /// [`TcpListener`](tokio::net::TcpListener)
    Tcp(tokio::net::TcpListener),

    #[cfg(unix)]
    /// [`UnixListener`](tokio::net::UnixListener)
    Unix(tokio::net::UnixListener),
}

impl UniListener {
    /// Creates a new [`UniListener`], which will be bound to the specified
    /// address.
    ///
    /// The returned listener is ready for accepting connections.
    ///
    /// Binding with a port number of 0 will request that the OS assigns a port
    /// to this listener. The port allocated can be queried via the
    /// [`local_addr`](Self::local_addr) method.
    ///
    /// The address type can be a host. If it yields multiple addresses, `bind`
    /// will be attempted with each of the addresses until one succeeds and
    /// returns the listener. If none of the addresses succeed in creating a
    /// listener, the error returned from the last attempt (the last
    /// address) is returned.
    pub async fn bind(addr: &UniAddr) -> io::Result<Self> {
        match addr.as_inner() {
            UniAddrInner::Inet(addr) => tokio::net::TcpListener::bind(addr).await.map(Self::Tcp),
            #[cfg(unix)]
            UniAddrInner::Unix(addr) => {
                tokio::net::UnixListener::bind(addr.to_os_string()).map(Self::Unix)
            }
            UniAddrInner::Host(addr) => tokio::net::TcpListener::bind(&**addr).await.map(Self::Tcp),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "unsupported address type",
            )),
        }
    }

    /// Returns the local address that this listener is bound to.
    ///
    /// This can be useful, for example, when binding to port 0 to figure out
    /// which port was actually bound
    pub fn local_addr(&self) -> io::Result<UniAddr> {
        match self {
            UniListener::Tcp(listener) => listener.local_addr().map(UniAddr::from),
            #[cfg(unix)]
            UniListener::Unix(listener) => listener.local_addr().map(UniAddr::from),
        }
    }

    /// Returns a [`SockRef`] to the underlying socket for configuration.
    pub fn as_socket_ref(&self) -> SockRef<'_> {
        match self {
            UniListener::Tcp(listener) => listener.into(),
            #[cfg(unix)]
            UniListener::Unix(listener) => listener.into(),
        }
    }

    /// Accepts a new incoming connection from this listener.
    ///
    /// This function will yield once a new TCP connection is established. When
    /// established, the corresponding [`UniStream`] and the remote peer's
    /// address will be returned.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. If the method is used as the event in a
    /// `tokio::select!` statement and some other branch completes first, then
    /// it is guaranteed that no new connections were accepted by this
    /// method.
    pub async fn accept(&self) -> io::Result<(UniStream, UniAddr)> {
        match self {
            UniListener::Tcp(listener) => {
                let (stream, addr) = listener.accept().await?;
                Ok((UniStream::try_from(stream)?, UniAddr::from(addr)))
            }
            #[cfg(unix)]
            UniListener::Unix(listener) => {
                let (stream, addr) = listener.accept().await?;
                Ok((UniStream::try_from(stream)?, UniAddr::from(addr)))
            }
        }
    }
}
