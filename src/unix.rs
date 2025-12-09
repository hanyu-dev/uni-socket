//! A unified stream type for both TCP and Unix domain sockets.

#[cfg(all(target_os = "linux", feature = "splice"))]
pub mod splice;

use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::net::{Shutdown, SocketAddr};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;
use std::{fmt, io};

use socket2::{SockRef, Socket};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::sleep;
use uni_addr::{UniAddr, UniAddrInner};

wrapper_lite::wrapper!(
    #[wrapper_impl(AsRef)]
    /// An async [`Socket`].
    pub struct UniSocket<Ty = ()> {
        inner: AsyncFd<Socket>,
        ty: PhantomData<Ty>,
    }
);

impl fmt::Debug for UniSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("UniSocket").field(&self.inner).finish()
    }
}

impl<Ty> AsFd for UniSocket<Ty> {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl<Ty> AsRawFd for UniSocket<Ty> {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl UniSocket {
    /// Creates a new [`UniSocket`], and applies the given initialization
    /// function to the underlying socket.
    ///
    /// The given address determines the socket type, and the caller should bind
    /// to / connect to the address later.
    pub fn new(addr: &UniAddr) -> io::Result<Self> {
        Self::new_priv(addr)
    }
}

impl<Ty> UniSocket<Ty> {
    #[inline]
    const fn from_inner(inner: AsyncFd<Socket>) -> Self {
        Self {
            inner,
            ty: PhantomData,
        }
    }

    fn new_priv(addr: &UniAddr) -> io::Result<Self> {
        let ty = socket2::Type::STREAM;

        #[cfg(any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "linux",
            target_os = "netbsd",
            target_os = "openbsd"
        ))]
        let ty = ty.nonblocking();

        let inner = match addr.as_inner() {
            UniAddrInner::Inet(SocketAddr::V4(_)) => {
                Socket::new(socket2::Domain::IPV4, ty, Some(socket2::Protocol::TCP))
            }
            UniAddrInner::Inet(SocketAddr::V6(_)) => {
                Socket::new(socket2::Domain::IPV6, ty, Some(socket2::Protocol::TCP))
            }
            UniAddrInner::Unix(_) => Socket::new(socket2::Domain::UNIX, ty, None),
            UniAddrInner::Host(_) => Err(io::Error::new(
                io::ErrorKind::Other,
                "The Host address type must be resolved before creating a socket",
            )),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "Unsupported address type",
            )),
        }?;

        #[cfg(not(any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "linux",
            target_os = "netbsd",
            target_os = "openbsd"
        )))]
        inner.set_nonblocking(true)?;

        // On platforms with Berkeley-derived sockets, this allows to quickly
        // rebind a socket, without needing to wait for the OS to clean up the
        // previous one.
        //
        // On Windows, this allows rebinding sockets which are actively in use,
        // which allows “socket hijacking”, so we explicitly don't set it here.
        // https://docs.microsoft.com/en-us/windows/win32/winsock/using-so-reuseaddr-and-so-exclusiveaddruse
        #[cfg(not(windows))]
        inner.set_reuse_address(true)?;

        AsyncFd::new(inner).map(Self::from_inner)
    }

    /// Binds the socket to the specified address.
    ///
    /// Notes that the address must be the one used to create the socket.
    pub fn bind(self, addr: &UniAddr) -> io::Result<Self> {
        self.inner.get_ref().bind(&addr.try_into()?)?;

        Ok(Self::from_inner(self.inner))
    }

    #[cfg(any(
        target_os = "ios",
        target_os = "visionos",
        target_os = "macos",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "illumos",
        target_os = "solaris",
        target_os = "linux",
        target_os = "android",
        target_os = "fuchsia",
    ))]
    /// Sets the value for the `SO_BINDTODEVICE` option on this socket, then
    /// [`bind`s](Self::bind) the socket to the specified address.
    ///
    /// If a socket is bound to an interface, only packets received from that
    /// particular interface are processed by the socket. Note that this only
    /// works for some socket types, particularly `AF_INET` sockets.
    ///
    /// For those platforms, like macOS, that do not support `SO_BINDTODEVICE`,
    /// this function will fallback to `bind_device_by_index_v(4|6)`, while the
    /// `if_index` obtained from the interface name with `if_nametoindex(3)`.
    ///
    /// When `device` is `None`, this is equivalent to calling
    /// [`bind`](Self::bind), instead of clearing the option like
    /// [`Socket::bind_device`].
    pub fn bind_device(self, addr: &UniAddr, device: Option<&str>) -> io::Result<Self> {
        if let Some(device) = device {
            #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
            {
                self.inner.get_ref().bind_device(Some(device.as_bytes()))?;
            }

            #[cfg(all(
                not(any(target_os = "android", target_os = "fuchsia", target_os = "linux")),
                any(
                    target_os = "ios",
                    target_os = "visionos",
                    target_os = "macos",
                    target_os = "tvos",
                    target_os = "watchos",
                    target_os = "illumos",
                    target_os = "solaris",
                )
            ))]
            {
                use std::num::NonZeroU32;

                #[allow(unsafe_code)]
                let if_index = unsafe { libc::if_nametoindex(device.as_ptr().cast()) };

                let Some(if_index) = NonZeroU32::new(if_index) else {
                    return Err(io::Error::last_os_error());
                };

                match addr.as_inner() {
                    UniAddrInner::Inet(SocketAddr::V4(_)) => {
                        self.inner
                            .get_ref()
                            .bind_device_by_index_v4(Some(if_index))?;
                    }
                    UniAddrInner::Inet(SocketAddr::V6(_)) => {
                        self.inner
                            .get_ref()
                            .bind_device_by_index_v6(Some(if_index))?;
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "`bind_device_by_index` only works for IPv4 and IPv6 addresses",
                        ))
                    }
                }
            }
        }

        self.bind(addr)
    }

    /// Mark a socket as ready to accept incoming connection requests using
    /// [`UniListener::accept`].
    ///
    /// This function directly corresponds to the `listen(2)` function on Unix.
    ///
    /// An error will be returned if `listen` or `connect` has already been
    /// called on this builder.
    pub fn listen(self, backlog: u32) -> io::Result<UniListener> {
        #[allow(clippy::cast_possible_wrap)]
        self.inner.get_ref().listen(backlog as i32)?;

        Ok(UniListener::from_inner(self.inner))
    }

    /// Initiates and completes a connection on this socket to the specified
    /// address.
    ///
    /// This function directly corresponds to the `connect(2)` function on Unix.
    ///
    /// An error will be returned if `connect` has already been called.
    pub async fn connect(self, addr: &UniAddr) -> io::Result<UniStream> {
        if let Err(e) = self.inner.get_ref().connect(&addr.try_into()?) {
            if e.raw_os_error() != Some(libc::EINPROGRESS) {
                return Err(e);
            }
        }

        let this = UniStream::from_inner(self.inner);

        // Poll connection completion
        loop {
            let mut guard = this.inner.writable().await?;

            match guard.try_io(|inner| inner.get_ref().take_error()) {
                Ok(Ok(None)) => break,
                Ok(Ok(Some(e)) | Err(e)) => return Err(e),
                Err(_would_block) => {}
            }
        }

        Ok(this)
    }

    /// Returns the socket address of the local half of this socket.
    ///
    /// This function directly corresponds to the `getsockname(2)` function on
    /// Windows and Unix.
    ///
    /// # Notes
    ///
    /// Depending on the OS this may return an error if the socket is not
    /// [bound](Self::bind).
    pub fn local_addr(&self) -> io::Result<UniAddr> {
        self.inner
            .get_ref()
            .local_addr()
            .and_then(TryFrom::try_from)
    }

    /// Returns a [`SockRef`] to the underlying socket for configuration.
    pub fn as_socket_ref(&self) -> SockRef<'_> {
        SockRef::from(&self.inner)
    }
}

#[derive(Debug)]
/// Marker type: this socket is a listener.
pub struct ListenerTy;

/// A [`UniSocket`] used as a listener.
pub type UniListener = UniSocket<ListenerTy>;

impl fmt::Debug for UniListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UniListener")
            .field("local_addr", &self.local_addr().ok())
            .finish()
    }
}

impl TryFrom<std::net::TcpListener> for UniListener {
    type Error = io::Error;

    /// Converts a standard library TCP listener into a unified [`UniListener`].
    ///
    /// # Panics
    ///
    /// This function panics if there is no current Tokio reactor set, or if
    /// the `rt` feature flag is not enabled.
    fn try_from(listener: std::net::TcpListener) -> Result<Self, Self::Error> {
        listener.set_nonblocking(true)?;

        AsyncFd::new(listener.into()).map(Self::from_inner)
    }
}

impl TryFrom<tokio::net::TcpListener> for UniListener {
    type Error = io::Error;

    /// Converts a Tokio library TCP listener into a unified [`UniListener`].
    ///
    /// # Panics
    ///
    /// This function panics if there is no current Tokio reactor set, or if
    /// the `rt` feature flag is not enabled.
    fn try_from(listener: tokio::net::TcpListener) -> Result<Self, Self::Error> {
        listener
            .into_std()
            .map(Into::into)
            .and_then(AsyncFd::new)
            .map(Self::from_inner)
    }
}

impl UniListener {
    /// Accepts an incoming connection to this listener, and returns the
    /// accepted stream and the peer address.
    ///
    /// This method will retry on non-deadly errors, including:
    ///
    /// - `ECONNREFUSED`.
    /// - `ECONNABORTED`.
    /// - `ECONNRESET`.
    /// - `EMFILE`.
    pub async fn accept(&self) -> io::Result<(UniStream, UniAddr)> {
        fn accept(socket: &Socket) -> io::Result<(UniStream, UniAddr)> {
            // On platforms that support it we can use `accept4(2)` to set `NONBLOCK`
            // and `CLOEXEC` in the call to accept the connection.
            // Android x86's seccomp profile forbids calls to `accept4(2)`
            // See https://github.com/tokio-rs/mio/issues/1445 for details
            #[cfg(any(
                all(not(target_arch = "x86"), target_os = "android"),
                target_os = "dragonfly",
                target_os = "freebsd",
                target_os = "fuchsia",
                target_os = "hurd",
                target_os = "illumos",
                target_os = "linux",
                target_os = "netbsd",
                target_os = "openbsd",
                target_os = "solaris",
                target_os = "cygwin",
            ))]
            let (accepted, peer_addr) = socket.accept4(libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK)?;

            // But not all platforms have the `accept4(2)` call. Luckily BSD (derived)
            // OSs inherit the non-blocking flag from the listener, so we just have to
            // set `CLOEXEC`.
            #[cfg(any(
                target_os = "aix",
                target_os = "haiku",
                target_os = "ios",
                target_os = "macos",
                target_os = "redox",
                target_os = "tvos",
                target_os = "visionos",
                target_os = "watchos",
                target_os = "espidf",
                target_os = "vita",
                target_os = "hermit",
                target_os = "nto",
                all(target_arch = "x86", target_os = "android"),
            ))]
            let (accepted, peer_addr) = socket.accept_raw().and_then(|(accepted, peer_addr)| {
                #[cfg(not(any(target_os = "espidf", target_os = "vita")))]
                accepted.set_cloexec(true)?;

                #[cfg(any(
                    all(target_arch = "x86", target_os = "android"),
                    target_os = "aix",
                    target_os = "espidf",
                    target_os = "vita",
                    target_os = "hermit",
                    target_os = "nto",
                ))]
                accepted.set_nonblocking(true)?;

                // On Apple platforms set `NOSIGPIPE`.
                #[cfg(any(
                    target_os = "ios",
                    target_os = "visionos",
                    target_os = "macos",
                    target_os = "tvos",
                    target_os = "watchos",
                ))]
                socket.set_nosigpipe(true)?;

                Ok((accepted, peer_addr))
            })?;

            // The non-blocking state of `listener` is inherited. See
            // https://docs.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-accept#remarks.
            #[cfg(windows)]
            let (accepted, peer_addr) = socket.accept_raw()?;

            Ok((
                UniStream::from_inner(AsyncFd::new(accepted)?),
                peer_addr.try_into()?,
            ))
        }

        loop {
            let accepted = self
                .inner
                .readable()
                .await?
                .try_io(|socket| accept(socket.get_ref()));

            match accepted {
                Ok(ret @ Ok(_)) => {
                    return ret;
                }
                Ok(Err(e))
                    if matches!(
                        e.kind(),
                        io::ErrorKind::ConnectionRefused
                            | io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::ConnectionReset
                    ) =>
                {
                    // This is not a deadly error, too, just continue
                }
                Ok(Err(e)) if matches!(e.raw_os_error(), Some(libc::EMFILE)) => {
                    // This is not a deadly error, but we may wait for a while.
                    sleep(Duration::from_secs(1)).await;
                }
                Ok(Err(e)) => {
                    return Err(e);
                }
                Err(_would_block) => {}
            }
        }
    }

    /// Accepts an incoming connection to this listener, and returns the
    /// accepted stream and the peer address.
    ///
    /// Notes that on multiple calls to [`poll_accept`](Self::poll_accept), only
    /// the waker from the [`Context`] passed to the most recent call is
    /// scheduled to receive a wakeup. Unless you are implementing your own
    /// future accepting connections, you probably want to use the asynchronous
    /// [`accept`](Self::accept) method instead.
    ///
    /// Unlike [`accept`](Self::accept), this method does not handle `EMFILE`
    /// (i.e., too many open files) errors and the caller may need to handle
    /// it by itself.
    pub fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(UniStream, UniAddr)>> {
        loop {
            let mut guard = ready!(self.inner.poll_read_ready(cx))?;

            match guard.try_io(|socket| socket.get_ref().accept()) {
                Ok(Ok((socket, addr))) => {
                    let addr = if let Some(addr) = addr.as_socket() {
                        UniAddr::from(addr)
                    } else if let Some(addr) = addr.as_unix() {
                        UniAddr::from(addr)
                    } else {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            "unsupported address type",
                        )));
                    };

                    return Poll::Ready(Ok((UniStream::from_inner(AsyncFd::new(socket)?), addr)));
                }
                Ok(Err(e))
                    if matches!(
                        e.kind(),
                        io::ErrorKind::ConnectionRefused
                            | io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::ConnectionReset
                    ) =>
                {
                    // This is not a deadly error, too, just continue
                }
                Ok(Err(e)) => {
                    return Poll::Ready(Err(e));
                }
                Err(_would_block) => {}
            }
        }
    }
}

#[derive(Debug)]
/// Marker type: this socket is a (TCP) stream.
pub struct StreamTy;

/// A [`UniSocket`] used as a stream.
pub type UniStream = UniSocket<StreamTy>;

impl fmt::Debug for UniStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UniStream")
            .field("local_addr", &self.local_addr().ok())
            .field("peer_addr", &self.peer_addr().ok())
            .finish()
    }
}

impl TryFrom<tokio::net::TcpStream> for UniStream {
    type Error = io::Error;

    /// Converts a Tokio TCP stream into a unified [`UniStream`].
    ///
    /// # Panics
    ///
    /// This function panics if there is no current Tokio reactor set, or if
    /// the `rt` feature flag is not enabled.
    fn try_from(stream: tokio::net::TcpStream) -> Result<Self, Self::Error> {
        stream
            .into_std()
            .map(Into::into)
            .and_then(AsyncFd::new)
            .map(Self::from_inner)
    }
}

impl TryFrom<std::net::TcpStream> for UniStream {
    type Error = io::Error;

    /// Converts a standard library TCP stream into a unified [`UniStream`].
    ///
    /// # Panics
    ///
    /// This function panics if there is no current Tokio reactor set, or if
    /// the `rt` feature flag is not enabled.
    fn try_from(stream: std::net::TcpStream) -> Result<Self, Self::Error> {
        stream.set_nonblocking(true)?;

        AsyncFd::new(stream.into()).map(Self::from_inner)
    }
}

impl TryFrom<tokio::net::UnixStream> for UniStream {
    type Error = io::Error;

    /// Converts a Tokio Unix stream into a unified [`UniStream`].
    ///
    /// # Panics
    ///
    /// This function panics if there is no current Tokio reactor set, or if
    /// the `rt` feature flag is not enabled.
    fn try_from(stream: tokio::net::UnixStream) -> Result<Self, Self::Error> {
        stream
            .into_std()
            .map(Into::into)
            .and_then(AsyncFd::new)
            .map(Self::from_inner)
    }
}

impl TryFrom<std::os::unix::net::UnixStream> for UniStream {
    type Error = io::Error;

    /// Converts a standard library Unix stream into a unified [`UniStream`].
    ///
    /// # Panics
    ///
    /// This function panics if there is no current Tokio reactor set, or if
    /// the `rt` feature flag is not enabled.
    fn try_from(stream: std::os::unix::net::UnixStream) -> Result<Self, Self::Error> {
        stream.set_nonblocking(true)?;

        AsyncFd::new(stream.into()).map(Self::from_inner)
    }
}

impl UniStream {
    /// Returns the socket address of the remote peer of this socket.
    ///
    /// This function directly corresponds to the `getpeername(2)` function on
    /// Unix.
    ///
    /// # Notes
    ///
    /// This returns an error if the socket is not
    /// [`connect`ed](UniSocket::connect).
    pub fn peer_addr(&self) -> io::Result<UniAddr> {
        self.inner.get_ref().peer_addr().and_then(TryFrom::try_from)
    }

    /// Receives data on the socket from the remote adress to which it is
    /// connected, without removing that data from the queue. On success,
    /// returns the number of bytes peeked.
    ///
    /// Successive calls return the same data. This is accomplished by passing
    /// `MSG_PEEK` as a flag to the underlying `recv` system call.
    pub async fn peek(&mut self, buf: &mut [MaybeUninit<u8>]) -> io::Result<usize> {
        loop {
            let mut guard = self.inner.readable().await?;

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
    /// Notes that on multiple calls to [`poll_peek`](Self::poll_peek), only
    /// the waker from the [`Context`] passed to the most recent call is
    /// scheduled to receive a wakeup. Unless you are implementing your own
    /// future accepting connections, you probably want to use the asynchronous
    /// [`accept`](UniListener::accept) method instead.
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
                Ok(Err(e)) => return Poll::Ready(Err(e)),
                Err(_would_block) => {}
            }
        }
    }

    #[inline]
    /// Receives data on the socket from the remote address to which it is
    /// connected.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. Once a readiness event occurs, the method
    /// will continue to return immediately until the readiness event is
    /// consumed by an attempt to read or write that fails with `WouldBlock` or
    /// `Poll::Pending`.
    pub async fn read(&mut self, buf: &mut [MaybeUninit<u8>]) -> io::Result<usize> {
        self.read_priv(buf).await
    }

    async fn read_priv(&self, buf: &mut [MaybeUninit<u8>]) -> io::Result<usize> {
        loop {
            let mut guard = self.inner.readable().await?;

            match guard.try_io(|inner| inner.get_ref().recv(buf)) {
                Ok(result) => return result,
                Err(_would_block) => {}
            }
        }
    }

    fn poll_read_priv(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        loop {
            let mut guard = match self.inner.poll_read_ready(cx) {
                Poll::Ready(Ok(guard)) => guard,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {
                    return Poll::Pending;
                }
            };

            #[allow(unsafe_code)]
            let unfilled = unsafe { buf.unfilled_mut() };

            match guard.try_io(|inner| {
                let ret = inner.get_ref().recv(unfilled);

                ret
            }) {
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
                Ok(Err(e)) => return Poll::Ready(Err(e)),
                Err(_would_block) => {}
            }
        }
    }

    #[inline]
    /// Sends data on the socket to a connected peer.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. Once a readiness event occurs, the method
    /// will continue to return immediately until the readiness event is
    /// consumed by an attempt to read or write that fails with `WouldBlock` or
    /// `Poll::Pending`.
    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_priv(buf).await
    }

    async fn write_priv(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.inner.writable().await?;

            match guard.try_io(|inner| inner.get_ref().send(buf)) {
                Ok(result) => return result,
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

    /// Shuts down the read, write, or both halves of this connection.
    ///
    /// This function will cause all pending and future I/O on the specified
    /// portions to return immediately with an appropriate value.
    pub fn shutdown(&mut self, shutdown: Shutdown) -> io::Result<()> {
        match self.inner.get_ref().shutdown(shutdown) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotConnected => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Splits a [`UniStream`] into a read half and a write half, which can be
    /// used to read and write the stream concurrently.
    ///
    /// Note: dropping the write half will shutdown the write half of the
    /// stream.
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let this = Arc::new(self);

        (
            OwnedReadHalf::from_inner(this.clone()),
            OwnedWriteHalf::from_inner(this),
        )
    }
}

impl AsyncRead for UniStream {
    #[inline]
    /// Receives data on the socket from the remote address to which it is
    /// connected.
    ///
    /// Notes that on multiple calls to [`poll_read`](Self::poll_read), only
    /// the waker from the [`Context`] passed to the most recent call is
    /// scheduled to receive a wakeup. Unless you are implementing your own
    /// future accepting connections, you probably want to use the asynchronous
    /// [`read`](Self::read) method instead.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.poll_read_priv(cx, buf)
    }
}

impl AsyncWrite for UniStream {
    #[inline]
    /// Sends data on the socket to a connected peer.
    ///
    /// Notes that on multiple calls to [`poll_write`](Self::poll_write), only
    /// the waker from the [`Context`] passed to the most recent call is
    /// scheduled to receive a wakeup. Unless you are implementing your own
    /// future accepting connections, you probably want to use the asynchronous
    /// [`write`](Self::write) method instead.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_priv(cx, buf)
    }

    #[inline]
    /// For TCP and Unix domain sockets, `flush` is a no-op.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// See [`shutdown`](Self::shutdown).
    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.shutdown(Shutdown::Write))
    }
}

#[cfg(feature = "splice-legacy")]
impl tokio_splice2::AsyncReadFd for UniStream {
    fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_read_ready(cx).map_ok(|_| ())
    }

    fn try_io_read<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
        use tokio::io::Interest;

        self.inner.try_io(Interest::READABLE, |_| f())
    }
}

#[cfg(feature = "splice-legacy")]
impl tokio_splice2::AsyncWriteFd for UniStream {
    fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_write_ready(cx).map_ok(|_| ())
    }

    fn try_io_write<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
        use tokio::io::Interest;

        self.inner.try_io(Interest::WRITABLE, |_| f())
    }
}

#[cfg(feature = "splice-legacy")]
impl tokio_splice2::IsNotFile for UniStream {}

wrapper_lite::wrapper!(
    #[wrapper_impl(AsRef<UniStream>)]
    #[derive(Debug)]
    /// An owned read half of a [`UniStream`].
    pub struct OwnedReadHalf(Arc<UniStream>);
);

impl AsyncRead for OwnedReadHalf {
    #[inline]
    /// See [`poll_read`](UniStream::poll_read).
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
    /// An owned write half of a [`UniStream`].
    pub struct OwnedWriteHalf(Arc<UniStream>);
);

impl AsyncWrite for OwnedWriteHalf {
    #[inline]
    /// See [`poll_write`](UniStream::poll_write).
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_write_priv(cx, buf)
    }

    #[inline]
    /// See [`poll_flush`](UniStream::poll_flush).
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// See [`shutdown`](UniStream::shutdown).
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.inner.as_socket_ref().shutdown(Shutdown::Write))
    }
}

impl Drop for OwnedWriteHalf {
    fn drop(&mut self) {
        let _ = self.inner.as_socket_ref().shutdown(Shutdown::Write);
    }
}
