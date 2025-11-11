//! A unified stream type for both TCP and Unix domain sockets.

use std::io::Write as _;
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::{fmt, io};

use socket2::Socket;
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use uni_addr::UniAddr;

wrapper_lite::wrapper!(
    #[wrapper_impl(AsRef)]
    #[wrapper_impl(AsMut)]
    #[wrapper_impl(BorrowMut)]
    #[wrapper_impl(DerefMut)]
    /// A unified stream type that can represent either a TCP or Unix domain
    /// socket stream.
    pub struct Stream {
        inner: AsyncFd<Socket>,
        local_addr: UniAddr,
        peer_addr: UniAddr,
    }
);

#[allow(clippy::missing_fields_in_debug)]
impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stream")
            .field("local_addr", &self.local_addr)
            .field("peer_addr", &self.peer_addr)
            .finish()
    }
}

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

impl TryFrom<tokio::net::TcpStream> for Stream {
    type Error = io::Error;

    /// Converts a Tokio TCP stream into a unified [`Stream`].
    ///
    /// # Panics
    ///
    /// This function panics if there is no current Tokio reactor set, or if
    /// the `rt` feature flag is not enabled.
    fn try_from(stream: tokio::net::TcpStream) -> Result<Self, Self::Error> {
        let peer_addr = UniAddr::from(stream.peer_addr()?);
        let local_addr = UniAddr::from(stream.local_addr()?);

        stream
            .into_std()
            .map(Into::into)
            .and_then(AsyncFd::new)
            .map(|inner| Self {
                inner,
                local_addr,
                peer_addr,
            })
    }
}

impl TryFrom<tokio::net::UnixStream> for Stream {
    type Error = io::Error;

    /// Converts a Tokio Unix stream into a unified [`Stream`].
    ///
    /// # Panics
    ///
    /// This function panics if there is no current Tokio reactor set, or if
    /// the `rt` feature flag is not enabled.
    fn try_from(stream: tokio::net::UnixStream) -> Result<Self, Self::Error> {
        let peer_addr = UniAddr::from(stream.peer_addr()?);
        let local_addr = UniAddr::from(stream.local_addr()?);

        stream
            .into_std()
            .map(Into::into)
            .and_then(AsyncFd::new)
            .map(|inner| Self {
                inner,
                local_addr,
                peer_addr,
            })
    }
}

impl TryFrom<std::net::TcpStream> for Stream {
    type Error = io::Error;

    /// Converts a standard library TCP stream into a unified [`Stream`].
    ///
    /// # Panics
    ///
    /// This function panics if there is no current Tokio reactor set, or if
    /// the `rt` feature flag is not enabled.
    fn try_from(stream: std::net::TcpStream) -> Result<Self, Self::Error> {
        stream.set_nonblocking(true)?;

        let peer_addr = UniAddr::from(stream.peer_addr()?);
        let local_addr = UniAddr::from(stream.local_addr()?);

        AsyncFd::new(stream.into()).map(|inner| Self {
            inner,
            local_addr,
            peer_addr,
        })
    }
}

impl TryFrom<std::os::unix::net::UnixStream> for Stream {
    type Error = io::Error;

    /// Converts a standard library Unix stream into a unified [`Stream`].
    ///
    /// # Panics
    ///
    /// This function panics if there is no current Tokio reactor set, or if
    /// the `rt` feature flag is not enabled.
    fn try_from(stream: std::os::unix::net::UnixStream) -> Result<Self, Self::Error> {
        stream.set_nonblocking(true)?;

        let peer_addr = UniAddr::from(stream.peer_addr()?);
        let local_addr = UniAddr::from(stream.local_addr()?);

        AsyncFd::new(stream.into()).map(|inner| Self {
            inner,
            local_addr,
            peer_addr,
        })
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            let mut guard = ready!(self.inner.poll_read_ready(cx))?;

            #[allow(unsafe_code)]
            let unfilled = unsafe { buf.unfilled_mut() };

            match guard.try_io(|inner| inner.get_ref().recv(unfilled)) {
                Ok(Ok(len)) => {
                    // Advance initialized
                    #[allow(unsafe_code)]
                    unsafe {
                        buf.assume_init(len);
                    };

                    // Advance filled
                    buf.advance(len);

                    return Poll::Ready(Ok(()));
                }
                Ok(Err(err)) => return Poll::Ready(Err(err)),
                Err(_would_block) => {}
            }
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            let mut guard = ready!(self.inner.poll_write_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().send(buf)) {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => {},
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.get_ref().flush())
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.get_ref().shutdown(std::net::Shutdown::Write)?;

        Poll::Ready(Ok(()))
    }
}

impl Stream {
    #[inline]
    /// Returns the local address of this stream.
    #[must_use]
    pub const fn local_addr(&self) -> &UniAddr {
        &self.local_addr
    }

    #[inline]
    /// Returns the peer address of this stream.
    #[must_use]
    pub const fn peer_addr(&self) -> &UniAddr {
        &self.peer_addr
    }

    /// Receives data on the socket from the remote adress to which it is
    /// connected, without removing that data from the queue. On success,
    /// returns the number of bytes peeked.
    ///
    /// Successive calls return the same data. This is accomplished by passing
    /// `MSG_PEEK` as a flag to the underlying `recv` system call.
    ///
    /// # Errors
    ///
    /// See [`AsyncFd::readable`] and [`Socket::peek`] for possible errors.
    pub async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.inner.readable().await?;

            #[allow(unsafe_code)]
            let buf = unsafe { &mut *(buf as *mut [u8] as *mut [MaybeUninit<u8>]) };

            match guard.try_io(|inner| inner.get_ref().peek(buf)) {
                Ok(result) => return result,
                Err(_would_block) => {},
            }
        }
    }

    /// Receives data on the socket from the remote adress to which it is
    /// connected, without removing that data from the queue. On success,
    /// returns the number of bytes peeked.
    ///
    /// Successive calls return the same data. This is accomplished by passing
    /// `MSG_PEEK` as a flag to the underlying `recv` system call.
    ///
    /// # Errors
    ///
    /// See [`AsyncFd::poll_read_ready`] and [`Socket::peek`] for possible
    /// errors.
    pub fn poll_peek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<usize>> {
        loop {
            let mut guard = ready!(self.inner.poll_read_ready(cx))?;

            #[allow(unsafe_code)]
            let unfilled = unsafe { buf.unfilled_mut() };

            match guard.try_io(|inner| inner.get_ref().peek(unfilled)) {
                Ok(Ok(len)) => {
                    // Advance initialized
                    #[allow(unsafe_code)]
                    unsafe {
                        buf.assume_init(len);
                    };

                    // Advance filled
                    buf.advance(len);

                    return Poll::Ready(Ok(len));
                }
                Ok(Err(err)) => return Poll::Ready(Err(err)),
                Err(_would_block) => {},
            }
        }
    }
}
