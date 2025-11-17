//! A simple TCP stream wrapper for non-Unix platforms.

use std::io;

use socket2::SockRef;
use uni_addr::{UniAddr, UniAddrInner};

wrapper_lite::wrapper!(
    #[wrapper_impl(Debug)]
    #[wrapper_impl(AsRef)]
    #[wrapper_impl(AsMut)]
    #[wrapper_impl(BorrowMut)]
    #[wrapper_impl(DerefMut)]
    /// Oh, just a simple wrapper of [`tokio::net::TcpStream`] for non-Unix
    /// platforms.
    pub struct UniStream(tokio::net::TcpStream);
);

#[cfg(windows)]
mod sys {
    use std::os::windows::io::{AsRawSocket, AsSocket, BorrowedSocket, RawSocket};

    use super::UniStream;

    impl AsSocket for UniStream {
        fn as_socket(&self) -> BorrowedSocket<'_> {
            self.as_inner().as_socket()
        }
    }

    impl AsRawSocket for UniStream {
        fn as_raw_socket(&self) -> RawSocket {
            self.as_inner().as_raw_socket()
        }
    }
}

impl TryFrom<tokio::net::TcpStream> for UniStream {
    type Error = io::Error;

    /// Converts a Tokio TCP stream into a [`UniStream`].
    ///
    /// # Errors
    ///
    /// This is infallible and always returns `Ok`, for APIs consistency.
    fn try_from(value: tokio::net::TcpStream) -> Result<Self, Self::Error> {
        Ok(Self::const_from(value))
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
    ///
    /// # Errors
    ///
    /// See [`tokio::net::TcpStream::connect`] for possible errors.
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

    #[must_use]
    /// See [`tokio::net::TcpStream::into_split`].
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        self.inner.into_split()
    }
}

// Re-export split halves
pub use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
