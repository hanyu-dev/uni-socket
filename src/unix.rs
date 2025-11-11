//! A unified stream type for both TCP and Unix domain sockets.

use std::io::Write as _;
use std::mem::MaybeUninit;
use std::net::Shutdown;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::{fmt, io};

use socket2::{SockRef, Socket};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use uni_addr::{UniAddr, UniAddrInner};

wrapper_lite::wrapper!(
    #[wrapper_impl(AsRef)]
    #[wrapper_impl(AsMut)]
    #[wrapper_impl(BorrowMut)]
    #[wrapper_impl(DerefMut)]
    /// A unified stream type that can represent either a TCP or Unix domain
    /// socket stream.
    pub struct UniStream {
        inner: AsyncFd<Socket>,
        local_addr: UniAddr,
        peer_addr: UniAddr,
    }
);

#[allow(clippy::missing_fields_in_debug)]
impl fmt::Debug for UniStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UniStream")
            .field("local_addr", &self.local_addr)
            .field("peer_addr", &self.peer_addr)
            .finish()
    }
}

impl AsFd for UniStream {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl AsRawFd for UniStream {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl TryFrom<tokio::net::TcpStream> for UniStream {
    type Error = io::Error;

    #[inline]
    /// Converts a Tokio TCP stream into a unified [`UniStream`].
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

impl TryFrom<tokio::net::UnixStream> for UniStream {
    type Error = io::Error;

    #[inline]
    /// Converts a Tokio Unix stream into a unified [`UniStream`].
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

impl TryFrom<std::net::TcpStream> for UniStream {
    type Error = io::Error;

    #[inline]
    /// Converts a standard library TCP stream into a unified [`UniStream`].
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

impl TryFrom<std::os::unix::net::UnixStream> for UniStream {
    type Error = io::Error;

    #[inline]
    /// Converts a standard library Unix stream into a unified [`UniStream`].
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

impl UniStream {
    /// Opens a TCP connection to a remote host.
    ///
    /// `addr` is an address of the remote host. If `addr` is a host which
    /// yields multiple addresses, `connect` will be attempted with each of
    /// the addresses until a connection is successful. If none of the
    /// addresses result in a successful connection, the error returned from
    /// the last connection attempt (the last address) is returned.
    pub async fn connect(addr: &UniAddr) -> io::Result<Self> {
        match addr.as_inner() {
            UniAddrInner::Inet(addr) => tokio::net::TcpStream::connect(addr)
                .await
                .and_then(Self::try_from),
            UniAddrInner::Unix(addr) => tokio::net::UnixStream::connect(addr.to_os_string())
                .await
                .and_then(Self::try_from),
            UniAddrInner::Host(addr) => tokio::net::TcpStream::connect(&**addr)
                .await
                .and_then(Self::try_from),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "unsupported address type",
            )),
        }
    }

    /// Returns a [`SockRef`] to the underlying socket for configuration.
    pub fn as_socket_ref(&self) -> SockRef<'_> {
        self.inner.get_ref().into()
    }

    #[inline]
    /// Returns the local address of this stream.
    pub const fn local_addr(&self) -> &UniAddr {
        &self.local_addr
    }

    #[inline]
    /// Returns the peer address of this stream.
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
                Err(_would_block) => {}
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
                Err(_would_block) => {}
            }
        }
    }

    #[inline]
    /// Splits a [`UniStream`] into a read half and a write half, which can be
    /// used to read and write the stream concurrently.
    ///
    /// Note: dropping the write half will shutdown the write half of the
    /// stream.
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let this = Arc::new(self);

        (
            OwnedReadHalf::const_from(this.clone()),
            OwnedWriteHalf::const_from(this),
        )
    }

    fn poll_read_priv(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
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

    fn poll_write_priv(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        loop {
            let mut guard = ready!(self.inner.poll_write_ready(cx))?;

            match guard.try_io(|inner| inner.get_ref().send(buf)) {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => {}
            }
        }
    }

    #[inline]
    fn flush_priv(&self) -> io::Result<()> {
        self.inner.get_ref().flush()
    }

    #[inline]
    fn shutdown_priv(&self, shutdown: Shutdown) -> io::Result<()> {
        self.inner.get_ref().shutdown(shutdown)
    }
}

impl AsyncRead for UniStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.poll_read_priv(cx, buf)
    }
}

impl AsyncWrite for UniStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_priv(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.flush_priv())
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.shutdown_priv(Shutdown::Write))
    }
}

wrapper_lite::wrapper!(
    #[wrapper_impl(AsRef<UniStream>)]
    #[derive(Debug)]
    /// A owned read half of a [`UniStream`].
    pub struct OwnedReadHalf(Arc<UniStream>);
);

impl AsyncRead for OwnedReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.inner.poll_read_priv(cx, buf)
    }
}

wrapper_lite::wrapper!(
    #[wrapper_impl(AsRef<UniStream>)]
    #[derive(Debug)]
    /// A owned write half of a [`UniStream`].
    pub struct OwnedWriteHalf(Arc<UniStream>);
);

impl AsyncWrite for OwnedWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_write_priv(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.inner.flush_priv())
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.inner.shutdown_priv(Shutdown::Write))
    }
}

impl Drop for OwnedWriteHalf {
    fn drop(&mut self) {
        let _ = self.inner.get_ref().shutdown(Shutdown::Write);
    }
}

#[cfg(feature = "splice")]
mod splice {
    use std::io;
    use std::num::NonZeroUsize;
    use std::os::fd::AsFd;
    use std::pin::Pin;
    use std::task::{ready, Context, Poll};

    use rustix::pipe::{splice, SpliceFlags};

    use super::{OwnedReadHalf, OwnedWriteHalf, UniStream};

    impl UniStream {
        /// `poll_splice_drain` moves data from a socket to a pipe (write end).
        ///
        /// Returns the total number of bytes drained on success.
        ///
        /// # Behaviour
        ///
        /// Keep reading data from the socket to the pipe until either:
        ///
        /// - Read `bytes_to_drain` bytes.
        /// - Partial read but read `EAGAIN` or EOF (returns the actual drained
        ///   bytes).
        ///
        /// # Constraints
        ///
        /// - `poll_splice_drain` assumes that the pipe has at least
        ///   `bytes_to_drain` space available for writing. This may be achieved
        ///   by exclusively using the pipe (i.e., either writing to the pipe or
        ///   reading from the pipe, but not reading and writing simultaneously)
        ///   and tracking the pipe capacity, the number of bytes written/read.
        ///
        ///   If this constraint is violated, when the pipe is created with flag
        ///   `O_NONBLOCK`, the readiness state of the underlying file
        ///   descriptor would be out of sync with the tokio-side readiness
        ///   state, which may cause the pending task to sleep forever. Of
        ///   course you can create the pipe without `O_NONBLOCK` flag,
        ///   but a long blocking operation in asynchronous context is still
        ///   discouraged and you could not rely on it.
        ///
        /// - When error returned, the pipe shall be closed properly and no
        ///   further `poll_splice_*` calls shall be made.
        ///
        /// # Errors
        ///
        /// See [splice(2)'s man page].
        ///
        /// [splice(2)'s man page]: https://man7.org/linux/man-pages/man2/splice.2.html
        pub fn poll_splice_drain<Fd>(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            pipe_write_fd: &Fd,
            bytes_to_drain: NonZeroUsize,
        ) -> Poll<io::Result<NonZeroUsize>>
        where
            Fd: AsFd,
        {
            self.poll_splice_drain_priv(cx, pipe_write_fd, bytes_to_drain)
        }

        fn poll_splice_drain_priv<Fd>(
            &self,
            cx: &mut Context<'_>,
            pipe_write_fd: &Fd,
            bytes_to_drain: NonZeroUsize,
        ) -> Poll<io::Result<NonZeroUsize>>
        where
            Fd: AsFd,
        {
            let mut remaining_bytes_to_drain = bytes_to_drain;

            loop {
                // In theory calling splice(2) with `SPLICE_F_NONBLOCK` could end up an infinite
                // loop here, because it could return EAGAIN ceaselessly when the write
                // end of the pipe is full, but this shouldn't be a concern here, since
                // the pipe buffer must be sufficient (all buffered bytes will be written to
                // writer after this).
                let mut guard = ready!(self.inner.poll_read_ready(cx))?;

                match guard.try_io(|inner| {
                    splice(
                        inner.as_fd(),
                        None,
                        pipe_write_fd,
                        None,
                        remaining_bytes_to_drain.get(),
                        SpliceFlags::NONBLOCK,
                    )
                    .map(NonZeroUsize::new)
                    .map_err(|e| io::Error::from_raw_os_error(e.raw_os_error()))
                }) {
                    Ok(Ok(Some(drained))) => {
                        let Some(remaining) =
                            remaining_bytes_to_drain.get().checked_sub(drained.get())
                        else {
                            // Read more bytes than requested?
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::Other,
                                "Rare bug: spliced more bytes than requested",
                            )));
                        };

                        let Some(remaining) = NonZeroUsize::new(remaining) else {
                            // Drained all requested bytes.
                            return Poll::Ready(Ok(bytes_to_drain));
                        };

                        remaining_bytes_to_drain = remaining;
                    }
                    Ok(Ok(None)) => {
                        let actual_bytes_drained = NonZeroUsize::new(
                            bytes_to_drain.get() - remaining_bytes_to_drain.get(),
                        )
                        .expect("The remaining bytes must be less than the original one");

                        // EOF reached, just return the drained bytes instead of erroring.
                        return Poll::Ready(Ok(actual_bytes_drained));
                    }
                    Ok(Err(err)) if matches!(err.kind(), io::ErrorKind::Interrupted) => {}
                    Ok(Err(err)) => return Poll::Ready(Err(err)),
                    Err(_would_block) => {
                        if remaining_bytes_to_drain != bytes_to_drain {
                            let actual_bytes_drained = NonZeroUsize::new(
                                bytes_to_drain.get() - remaining_bytes_to_drain.get(),
                            )
                            .expect("The remaining bytes must be less than the original one");

                            // Have drained some bytes, return early.
                            return Poll::Ready(Ok(actual_bytes_drained));
                        }
                    }
                }
            }
        }

        /// `poll_splice_pump` moves data from a pipe (read end) to a socket.
        ///
        /// Returns the total number of bytes pumped on success.
        ///
        /// # Behaviour
        ///
        /// Keep writing data from the pipe to the socket until either:
        ///
        /// - All `bytes_to_pump` bytes have been written.
        /// - Partial write but write EOF (returns the actual pumped bytes).
        ///
        /// # Constraints
        ///
        /// - `poll_splice_pump` assumes that the pipe has at least
        ///   `bytes_to_pump` bytes available for reading. This may be achieved
        ///   by exclusively using the pipe (i.e., either writing to the pipe or
        ///   reading from the pipe, but not reading and writing simultaneously)
        ///   and tracking the pipe capacity, the number of bytes written/read.
        ///
        ///   If this constraint is violated, when the pipe is created with flag
        ///   `O_NONBLOCK`, the readiness state of the underlying file
        ///   descriptor would be out of sync with the tokio-side readiness
        ///   state, which may cause the pending task to sleep forever. Of
        ///   course you can create the pipe without `O_NONBLOCK` flag,
        ///   but a long blocking operation in asynchronous context is still
        ///   discouraged and you could not rely on it.
        ///
        /// - When error returned, the pipe shall be closed properly and no
        ///   further `poll_splice_*` calls shall be made.
        ///
        /// # Errors
        ///
        /// See [splice(2)'s man page].
        ///
        /// [splice(2)'s man page]: https://man7.org/linux/man-pages/man2/splice.2.html
        pub fn poll_splice_pump<Fd>(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            pipe_read_fd: &Fd,
            bytes_to_pump: NonZeroUsize,
        ) -> Poll<io::Result<NonZeroUsize>>
        where
            Fd: AsFd,
        {
            self.poll_splice_pump_priv(cx, pipe_read_fd, bytes_to_pump)
        }

        fn poll_splice_pump_priv<Fd>(
            &self,
            cx: &mut Context<'_>,
            pipe_read_fd: &Fd,
            bytes_to_pump: NonZeroUsize,
        ) -> Poll<io::Result<NonZeroUsize>>
        where
            Fd: AsFd,
        {
            let mut remaining_bytes_to_pump = bytes_to_pump;

            loop {
                // In theory calling splice(2) with `SPLICE_F_NONBLOCK` could end up an infinite
                // loop here, because it could return EAGAIN ceaselessly when the write
                // end of the pipe is full, but this shouldn't be a concern here, since
                // the pipe buffer must be sufficient (all buffered bytes will be written to
                // writer after this).
                let mut guard = ready!(self.inner.poll_write_ready(cx))?;

                match guard.try_io(|inner| {
                    splice(
                        pipe_read_fd,
                        None,
                        inner.as_fd(),
                        None,
                        remaining_bytes_to_pump.get(),
                        SpliceFlags::NONBLOCK,
                    )
                    .map(NonZeroUsize::new)
                    .map_err(|e| io::Error::from_raw_os_error(e.raw_os_error()))
                }) {
                    Ok(Ok(Some(drained))) => {
                        let Some(remaining) =
                            remaining_bytes_to_pump.get().checked_sub(drained.get())
                        else {
                            // Write more bytes than requested?
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::Other,
                                "Rare bug: spliced more bytes than requested",
                            )));
                        };

                        let Some(remaining) = NonZeroUsize::new(remaining) else {
                            // Drained all requested bytes.
                            return Poll::Ready(Ok(bytes_to_pump));
                        };

                        remaining_bytes_to_pump = remaining;
                    }
                    Ok(Ok(None)) => {
                        let actual_bytes_drained =
                            NonZeroUsize::new(bytes_to_pump.get() - remaining_bytes_to_pump.get())
                                .expect("The remaining bytes  must be less than the original one");

                        // EOF reached, just return the drained bytes instead of erroring.
                        return Poll::Ready(Ok(actual_bytes_drained));
                    }
                    Ok(Err(err)) if matches!(err.kind(), io::ErrorKind::Interrupted) => {}
                    Ok(Err(err)) => return Poll::Ready(Err(err)),
                    Err(_would_block) => {
                        // Keep writing until all requested bytes are pumped.
                    }
                }
            }
        }
    }

    impl OwnedReadHalf {
        /// See [`UniStream::poll_splice_drain`].
        pub fn poll_splice_drain<Fd>(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            pipe_write_fd: &Fd,
            bytes_to_drain: NonZeroUsize,
        ) -> Poll<io::Result<NonZeroUsize>>
        where
            Fd: AsFd,
        {
            self.inner
                .poll_splice_drain_priv(cx, pipe_write_fd, bytes_to_drain)
        }
    }

    impl OwnedWriteHalf {
        /// See [`UniStream::poll_splice_pump`].
        pub fn poll_splice_pump<Fd>(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            pipe_read_fd: &Fd,
            bytes_to_pump: NonZeroUsize,
        ) -> Poll<io::Result<NonZeroUsize>>
        where
            Fd: AsFd,
        {
            self.inner
                .poll_splice_pump_priv(cx, pipe_read_fd, bytes_to_pump)
        }
    }

    #[cfg(test)]
    mod smoking {
        use std::future::poll_fn;
        use std::num::NonZeroUsize;

        use rustix::pipe::{fcntl_getpipe_size, pipe_with, PipeFlags};
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tokio::net::TcpListener;

        use super::*;

        async fn splice_io(mut r: OwnedReadHalf, mut w: OwnedWriteHalf) {
            let (pipe_r, pipe_w) =
                pipe_with(PipeFlags::NONBLOCK | PipeFlags::CLOEXEC).expect("Failed to create pipe");
            let pipe_size =
                NonZeroUsize::new(fcntl_getpipe_size(&pipe_w).expect("Failed to get pipe size"))
                    .expect("Pipe size is non-zero");

            println!("Pipe size: {}", pipe_size.get());

            let mut has_read = 0;
            let mut has_written = 0;

            loop {
                if let Some(to_drain) = NonZeroUsize::new(pipe_size.get() - has_read + has_written)
                {
                    println!(
                        "Draining up to {} bytes, r {has_read} / w {has_written}",
                        to_drain.get()
                    );

                    let drained =
                        poll_fn(|cx| Pin::new(&mut r).poll_splice_drain(cx, &pipe_w, to_drain))
                            .await
                            .expect("Failed to splice drain");

                    has_read += drained.get();

                    println!(
                        "Drained {} bytes, r {has_read} / w {has_written}",
                        drained.get()
                    );
                }

                if let Some(to_pump) = NonZeroUsize::new(
                    has_read
                        .checked_sub(has_written)
                        .expect("Can never write more than read"),
                ) {
                    println!(
                        "Pumping up to {} bytes, r {has_read} / w {has_written}",
                        to_pump.get()
                    );

                    let pumped =
                        poll_fn(|cx| Pin::new(&mut w).poll_splice_pump(cx, &pipe_r, to_pump))
                            .await
                            .expect("Failed to splice pump");

                    has_written += pumped.get();

                    println!(
                        "Pumped {} bytes, r {has_read} / w {has_written}",
                        pumped.get()
                    );
                }
            }
        }

        #[tokio::test]
        async fn test_splice_drain_and_pump() {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("Bind error");
            let relay_listener = TcpListener::bind("127.0.0.1:0").await.expect("Bind error");

            let server_addr = listener.local_addr().expect("Failed to get local addr");
            let relay_server_addr = relay_listener
                .local_addr()
                .expect("Failed to get local addr");

            println!("Server listening on {}", server_addr);
            println!("Relay listening on {}", relay_server_addr);

            // The echo server
            tokio::spawn(async move {
                let (accepted, peer_addr) = listener.accept().await.expect("Failed to accept");
                let mut accepted = UniStream::try_from(accepted).expect("Failed to create stream");

                println!("Echo server accepted from {peer_addr}");

                loop {
                    let mut buf = vec![0u8; 1024];

                    let read = accepted
                        .read(&mut buf)
                        .await
                        .expect("Failed to read from client");

                    println!("Echoing back {} bytes", read);

                    accepted
                        .write_all(&buf[..read])
                        .await
                        .expect("Failed to echo back");

                    println!("Echoed back {} bytes", read);
                }
            });

            // The relay server
            tokio::spawn(async move {
                let (accepted, peer_addr) =
                    relay_listener.accept().await.expect("Failed to accept");
                let (accepted_r, accepted_w) = UniStream::try_from(accepted)
                    .expect("Failed to create stream")
                    .into_split();

                println!("Relay accepted connection from {peer_addr}");

                let (target_r, target_w) = tokio::net::TcpStream::connect(server_addr)
                    .await
                    .and_then(UniStream::try_from)
                    .expect("Failed to connect to server")
                    .into_split();

                println!("Relay connected to server at {}", server_addr);

                tokio::select! {
                    biased;
                    _ = splice_io(accepted_r, target_w) => {},
                    _ = splice_io(target_r, accepted_w) => {},
                }
            });

            // The client
            tokio::spawn(async move {
                let mut stream = tokio::net::TcpStream::connect(relay_server_addr)
                    .await
                    .and_then(UniStream::try_from)
                    .expect("Connect error");

                println!("Client connected to relay at {}", relay_server_addr);

                // Test data
                let message = b"Hello, splice world!";
                stream.write_all(message).await.expect("Write error");

                let mut received = vec![0u8; message.len()];
                stream.read_exact(&mut received).await.expect("Read error");

                assert_eq!(&received, message);
            })
            .await
            .unwrap();
        }
    }
}
