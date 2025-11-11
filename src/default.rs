//! A simple TCP stream wrapper for non-Unix platforms.

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::BorrowedSocket;

wrapper_lite::wrapper!(
    #[wrapper_impl(Debug)]
    #[wrapper_impl(AsRef)]
    #[wrapper_impl(AsMut)]
    #[wrapper_impl(BorrowMut)]
    #[wrapper_impl(DerefMut)]
    #[wrapper_impl(From)]
    /// Oh, just a simple wrapper of [`tokio::net::TcpStream`] for non-Unix
    /// platforms.
    pub struct Stream(tokio::net::TcpStream);
);

impl AsFd for Stream {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl AsRawFd for Stream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(windows)]
impl AsSocket for Stream {
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.inner.as_socket()
    }
}

impl TryFrom<std::net::TcpStream> for Stream {
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
