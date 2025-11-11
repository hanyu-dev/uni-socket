//! A simple TCP stream wrapper for non-Unix platforms.

use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::BorrowedSocket;

use socket2::SockRef;
use uni_addr::{UniAddr, UniAddrInner};

wrapper_lite::wrapper!(
    #[wrapper_impl(Debug)]
    #[wrapper_impl(AsRef)]
    #[wrapper_impl(AsMut)]
    #[wrapper_impl(BorrowMut)]
    #[wrapper_impl(DerefMut)]
    #[wrapper_impl(From)]
    /// Oh, just a simple wrapper of [`tokio::net::TcpStream`] for non-Unix
    /// platforms.
    pub struct UniStream(tokio::net::TcpStream);
);

impl AsFd for UniStream {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl AsRawFd for UniStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(windows)]
impl AsSocket for UniStream {
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.inner.as_socket()
    }
}

impl TryFrom<std::net::TcpStream> for UniStream {
    type Error = std::io::Error;

    /// Converts a standard library TCP stream into a [`Stream`].
    ///
    /// # Panics
    ///
    /// This function panics if it is not called from within a runtime with
    /// IO enabled.
    ///
    /// The runtime is usually set implicitly when this function is called
    /// from a future driven by a tokio runtime, otherwise runtime can be
    /// set explicitly with `Runtime::enter` function.
    fn try_from(stream: std::net::TcpStream) -> Result<Self, Self::Error> {
        stream.set_nonblocking(true)?;

        Ok(Self::const_from(stream.try_into()?))
    }
}

impl UniStream {
    /// See [`tokio::net::TcpStream::connect`].
    pub async fn connect(addr: &UniAddr) -> io::Result<Self> {
        match addr.as_inner() {
            UniAddrInner::Inet(addr) => tokio::net::TcpStream::connect(addr)
                .await
                .map(Self::const_from),
            UniAddrInner::Host(addr) => tokio::net::TcpStream::connect(&**addr)
                .await
                .map(Self::const_from),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "unsupported address type",
            )),
        }
    }

    /// Returns a [`SockRef`] to the underlying socket for configuration.
    pub fn as_socket_ref(&self) -> SockRef<'_> {
        self.as_inner().into()
    }

    /// See [`tokio::net::TcpStream::into_split`].
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let (read_half, write_half) = self.inner.into_split();

        (
            OwnedReadHalf::const_from(read_half),
            OwnedWriteHalf::const_from(write_half),
        )
    }
}

wrapper_lite::wrapper!(
    #[wrapper_impl(Debug)]
    #[wrapper_impl(AsRef)]
    #[wrapper_impl(AsMut)]
    #[wrapper_impl(BorrowMut)]
    #[wrapper_impl(DerefMut)]
    #[wrapper_impl(From)]
    /// See [`tokio::net::tcp::OwnedReadHalf`].
    pub struct OwnedReadHalf(tokio::net::tcp::OwnedReadHalf);
);

wrapper_lite::wrapper!(
    #[wrapper_impl(Debug)]
    #[wrapper_impl(AsRef)]
    #[wrapper_impl(AsMut)]
    #[wrapper_impl(BorrowMut)]
    #[wrapper_impl(DerefMut)]
    #[wrapper_impl(From)]
    /// See [`tokio::net::tcp::OwnedWriteHalf`].
    pub struct OwnedWriteHalf(tokio::net::tcp::OwnedWriteHalf);
);
