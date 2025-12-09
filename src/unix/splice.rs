//! Linux `splice(2)` support

use std::num::NonZeroUsize;
use std::os::fd::AsFd;
use std::task::{ready, Poll};
use std::{io, mem, task};

use rustix::event::PollFlags;
use rustix::fd::OwnedFd;
use rustix::pipe::{
    fcntl_getpipe_size, fcntl_setpipe_size, pipe_with, splice, PipeFlags, SpliceFlags,
};

use super::{OwnedReadHalf, OwnedWriteHalf, UniStream};

#[derive(Debug)]
/// `splice(2)` operation context.
pub struct Context {
    /// Pipe used to splice data.
    pipe: Pipe,

    /// The `off_in` when splicing from `R` to the pipe, or the `off_out` when
    /// splicing from the pipe to `W`.
    offset: Offset,

    /// Target bytes to splice from `R` to `W`.
    ///
    /// Default is `isize::MAX`, which means read as much as possible. This is
    /// also the maximum length that can be spliced in a single `splice(2)`
    /// call.
    target: usize,

    /// Total bytes drained from `R`.
    drained: usize,

    /// Total bytes pumped to `W`.
    pumped: usize,

    /// The current state of the splice operation.
    state: State,
}

#[derive(Debug)]
enum State {
    Draining,
    Pumping,
    Done,
    Error,
}

impl Context {
    /// Create a new [`Context`] with given pipe.
    pub fn new(pipe: Pipe) -> Self {
        Self {
            pipe,
            offset: Offset::None,
            target: isize::MAX as usize,
            drained: 0,
            pumped: 0,
            state: State::Draining,
        }
    }

    /// Polls copying data from `src` to `dst` using `splice(2)`.
    ///
    /// Notes that on multiple calls to [`poll_copy`](Self::poll_copy), only the
    /// [`Waker`](task::Waker) from the [`task::Context`] passed to the most
    /// recent call is scheduled to receive a wakeup, which is probably not what
    /// you want.
    pub fn poll_copy(
        &mut self,
        cx: &mut task::Context<'_>,
        src: &mut OwnedReadHalf,
        dst: &mut OwnedWriteHalf,
    ) -> Poll<io::Result<()>> {
        macro_rules! ret {
            ($($tt:tt)*) => {
                match $($tt)* {
                    Ok(v) => v,
                    Err(e) => {
                        self.pipe.set_drain_done();
                        self.pipe.set_pump_done();
                        self.state = State::Error;
                        return Poll::Ready(Err(e));
                    }
                }
            };
        }

        loop {
            match self.state {
                State::Draining => {
                    ret!(ready!(self.poll_drain(cx, src.as_ref())));

                    self.state = State::Pumping;
                }
                State::Pumping => match ret!(ready!(self.poll_pump(cx, dst.as_ref()))) {
                    Ret::Continue => self.state = State::Draining,
                    Ret::Done => self.state = State::Done,
                },
                State::Done => {
                    break Poll::Ready(Ok(()));
                }
                State::Error => {
                    break Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Polls after error",
                    )));
                }
            }
        }
    }

    #[inline]
    /// Returns the total bytes transferred so far.
    pub const fn transferred(&self) -> usize {
        self.drained
    }

    /// `poll_drain` moves data from a socket (or file) to a pipe.
    ///
    /// # Invariants
    ///
    /// 1. The pipe must be empty, i.e., all data previously drained must have
    ///    been pumped to the destination.
    ///
    /// # Behaviours
    ///
    /// - This will close the pipe write side when there's no more data to read
    ///   (i.e., EOF).
    /// - This will not keep draining. Instead, it drains once and returns.
    fn poll_drain(
        &mut self,
        cx: &mut task::Context<'_>,
        socket_r: &UniStream,
    ) -> Poll<io::Result<Ret>> {
        let Some(pipe_w) = self.pipe.w.as_fd() else {
            return Poll::Ready(Ok(Ret::Done));
        };

        let target = {
            let Some(target) = self.target.checked_sub(self.drained) else {
                // TODO: panic or error?
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Other,
                    "unexpected: read more than target?",
                )));
            };

            let Some(target) = NonZeroUsize::new(target) else {
                self.pipe.set_drain_done();

                return Poll::Ready(Ok(Ret::Done));
            };

            // Okay, we are sure that the pipe is empty now.
            target.min(self.pipe.size)
        };

        loop {
            let mut guard = ready!(socket_r.as_inner().poll_read_ready(cx))?;

            match splice(
                socket_r,
                self.offset.off_in(),
                pipe_w,
                None,
                target.get(),
                SpliceFlags::NONBLOCK,
            )
            .map(NonZeroUsize::new)
            .map_err(io::Error::from)
            {
                Ok(Some(drained)) => {
                    self.drained += drained.get();

                    return Poll::Ready(Ok(Ret::Continue));
                }
                Ok(None) => {
                    self.pipe.set_drain_done();

                    return Poll::Ready(Ok(Ret::Done));
                }
                Err(e) if matches!(e.kind(), io::ErrorKind::Interrupted) => {}
                Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock) => {
                    if !test_readiness(socket_r, PollFlags::IN)? {
                        guard.clear_ready();

                        continue;
                    }

                    // Actually should not reach here, we have ensured that the pipe is empty...
                    return Poll::Ready(Ok(Ret::Continue));
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    /// `poll_pump` moves data from a pipe to a socket (or file).
    ///
    /// # Behaviours
    ///
    /// - This will close the pipe read side when there's no more data to write
    ///   (i.e., the pipe write side is closed and the draining process is
    ///   done).
    /// - This keeps pumping until all drained data has been written to the
    ///   destination.
    fn poll_pump(
        &mut self,
        cx: &mut task::Context<'_>,
        socket_w: &UniStream,
    ) -> Poll<io::Result<Ret>> {
        let Some(pipe_r) = self.pipe.r.as_fd() else {
            return Poll::Ready(Ok(Ret::Done));
        };

        'et_loop: loop {
            let mut guard = ready!(socket_w.as_inner().poll_write_ready(cx))?;

            loop {
                let Some(remaining) = self.drained.checked_sub(self.pumped) else {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "unexpected: written more than read?",
                    )));
                };

                let Some(remaining) = NonZeroUsize::new(remaining) else {
                    if self.pipe.is_drain_done() {
                        self.pipe.set_pump_done();
                        return Poll::Ready(Ok(Ret::Done));
                    } else {
                        return Poll::Ready(Ok(Ret::Continue));
                    }
                };

                match splice(
                    pipe_r,
                    None,
                    socket_w,
                    self.offset.off_out(),
                    remaining.get(),
                    SpliceFlags::NONBLOCK,
                )
                .map(NonZeroUsize::new)
                .map_err(io::Error::from)
                {
                    Ok(Some(written)) => self.pumped += written.get(),
                    Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock) => {
                        if !test_readiness(socket_w, PollFlags::OUT)? {
                            guard.clear_ready();

                            continue 'et_loop;
                        }
                    }
                    Ok(None) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                    Err(e) if matches!(e.kind(), io::ErrorKind::Interrupted) => {}
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }
        }
    }
}

#[inline]
/// When we call `splice(2)` in blocking mode, although the socket itself has
/// been set to non blocking mode, `splice(2)` operation itself may still block,
/// which is unacceptable in asynchronous context (although some tests can still
/// pass). For optimal compatibility, we set non blocking mode explicitly
/// (`O_NONBLOCK` to the pipe, while `SPLICE_F_NONBLOCK` to `splice(2)`) [like
/// Go does].
///
/// However, since `tokio` will probably rely on *Edge Trigger* mode, once the
/// readiness flag of a socket on the `tokio` side is incorrectly cleared then
/// loses sync, the `tokio` I/O task may sleep forever. Although by increasing
/// the pipe size, limiting the number of bytes read from the src socket to
/// the pipe in a single turn, and not [`drain`ing](Context::poll_drain) before
/// fully [`pumping`](Context::poll_pump) bytes we has read, we can alleviate
/// this problem, we adopt this workaround to ensure correctness: when
/// `splice(2)` returns `EAGAIN`, we poll the socket's readiness manually to
/// confirm whether the socket is indeed not ready. Poll always uses
/// level-triggered mode and it does not require any registration at all.
///
/// Some of benchmarks did show that this workaround has little impact on
/// performance.
///
/// Here's some useful references:
///
/// - [MengJiangProject/redproxy-rs](https://github.com/MengJiangProject/redproxy-rs/blob/4363b868bce8449441fb6364a679948d73270465/src/common/splice.rs)
/// - [Short-read optimization is wrong for O_DIRECT pipes](https://github.com/tokio-rs/tokio/issues/7051)
/// - [Reduce Mio's portable API to only support edge triggered notifications](https://github.com/tokio-rs/mio/issues/928)
/// - [Linux I/O 栈与零拷贝技术全揭秘 (Chinese)](https://strikefreedom.top/archives/linux-io-stack-and-zero-copy#splice)
/// - [Linux epoll 之 LT & ET 模式精粹 (Chinese)](https://strikefreedom.top/archives/linux-epoll-with-level-triggering-and-edge-triggering)
///
/// [like Go does]: https://github.com/golang/go/blob/master/src/internal/poll/splice_linux.go
fn test_readiness(socket: &impl AsFd, flag: PollFlags) -> io::Result<bool> {
    use rustix::event::{poll, PollFd, Timespec};

    let pollfds = &mut [PollFd::new(socket, flag)];

    // Set a timeout of 0, returning immediately.
    poll(pollfds, Some(&Timespec::default()))?;

    Ok(match pollfds[0].revents() {
        PollFlags::ERR | PollFlags::HUP | PollFlags::IN => true,
        PollFlags::NVAL => {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid fd"));
        }
        _ => false,
    })
}

#[derive(Debug)]
enum Offset {
    None,

    #[allow(unused)]
    /// Read offset set.
    In(Option<u64>),

    #[allow(unused)]
    /// Write offset set.
    Out(Option<u64>),
}

impl Offset {
    #[inline]
    fn off_in(&mut self) -> Option<&mut u64> {
        match self {
            Offset::In(off) => off.as_mut(),
            _ => None,
        }
    }

    #[inline]
    fn off_out(&mut self) -> Option<&mut u64> {
        match self {
            Offset::Out(off) => off.as_mut(),
            _ => None,
        }
    }

    #[allow(unused)]
    fn calc_size_to_splice(
        f_len: u64,
        f_offset_start: Option<u64>,
        f_offset_end: Option<u64>,
    ) -> io::Result<u64> {
        match (f_offset_start, f_offset_end) {
            (Some(start), Some(end)) => {
                if start > end || end > f_len {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid offset range",
                    ));
                }
                Ok(end - start)
            }
            (Some(start), None) => {
                if start > f_len {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid offset start",
                    ));
                }
                Ok(f_len - start)
            }
            (None, Some(end)) => {
                if end > f_len {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid offset end",
                    ));
                }
                Ok(end)
            }
            (None, None) => Ok(f_len),
        }
    }
}

/// `MAXIMUM_PIPE_SIZE` is the maximum amount of data we ask the kernel to move
/// in a single call to `splice(2)`.
///
/// We use 1MB as `splice(2)` writes data through a pipe, and 1MB is the default
/// maximum pipe buffer size, which is determined by
/// `/proc/sys/fs/pipe-max-size`.
///
/// Running applications under unprivileged user may have the pages usage
/// limited. See [`pipe(7)`] for details.
///
/// [`pipe(7)`]: https://man7.org/linux/man-pages/man7/pipe.7.html
const MAXIMUM_PIPE_SIZE: usize = 1 << 20;

/// By default, 16 * PAGE_SIZE.
const DEFAULT_PIPE_SIZE: usize = 1 << 16;

#[derive(Debug)]
/// `Pipe`.
pub struct Pipe {
    /// File descriptor for reading from the pipe
    r: Fd,

    /// File descriptor for writing to the pipe
    w: Fd,

    /// Pipe size in bytes.
    size: NonZeroUsize,
}

#[derive(Debug)]
enum Fd {
    /// The file descriptor can be used for reading or writing.
    Running(OwnedFd),

    #[allow(unused)]
    /// The file descriptor is reserved for future use (to be recycled).
    Reserved(OwnedFd),

    /// Make compiler happy.
    Closed,
}

impl Fd {
    #[inline]
    /// Convert the file descriptor to a pending state.
    ///
    /// This is used to indicate that the file descriptor is reserved for future
    /// use.
    fn set_reserved(&mut self) {
        if let Fd::Running(owned_fd) = mem::replace(self, Fd::Closed) {
            *self = Fd::Reserved(owned_fd);
        }
    }

    #[inline]
    const fn as_fd(&self) -> Option<&OwnedFd> {
        match self {
            Fd::Running(fd) => Some(fd),
            _ => None,
        }
    }
}

impl Pipe {
    /// Create a [`Pipe`], with flags `O_CLOEXEC`.
    ///
    /// The default pipe size is set to `MAXIMUM_PIPE_SIZE` bytes.
    ///
    /// # Errors
    ///
    /// See [`pipe(2)`] and [`fcntl(2)`].
    ///
    /// [`pipe(2)`]: https://man7.org/linux/man-pages/man2/pipe.2.html
    /// [`fcntl(2)`]: https://man7.org/linux/man-pages/man2/fcntl.2.html
    pub fn new() -> io::Result<Self> {
        pipe_with(PipeFlags::CLOEXEC | PipeFlags::NONBLOCK)
            .map_err(|e| io::Error::from_raw_os_error(e.raw_os_error()))
            .map(|(r, w)| Self {
                r: Fd::Running(r),
                w: Fd::Running(w),
                size: NonZeroUsize::new(DEFAULT_PIPE_SIZE).unwrap(),
            })
            .and_then(|mut this| {
                // Splice will loop writing MAXIMUM_PIPE_SIZE bytes from the source to the pipe,
                // and then write those bytes from the pipe to the destination.
                // Set the pipe buffer size to MAXIMUM_PIPE_SIZE to optimize that.
                // Ignore errors here, as a smaller buffer size will work,
                // although it will require more system calls.

                this.update_pipe_size(NonZeroUsize::new(MAXIMUM_PIPE_SIZE).unwrap())?;

                Ok(this)
            })
    }

    /// Sets and updates the pipe size.
    ///
    /// ## Errors
    ///
    /// See [`fcntl(2)`].
    ///
    /// [`fcntl(2)`]: https://man7.org/linux/man-pages/man2/fcntl.2.html.
    pub fn update_pipe_size(&mut self, size: NonZeroUsize) -> io::Result<usize> {
        let Some(r) = self.r.as_fd() else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "pipe is not available",
            ));
        };

        match fcntl_setpipe_size(&r, size.get()).map(NonZeroUsize::new) {
            Ok(Some(size)) => {
                self.size = size;

                Ok(self.size.get())
            }
            Ok(None) => {
                self.size = fcntl_getpipe_size(&r)
                    .ok()
                    .and_then(NonZeroUsize::new)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            "Failed to get pipe size while fcntl returned zero",
                        )
                    })?;

                Err(io::Error::new(io::ErrorKind::Other, "fcntl returned zero"))
            }
            Err(e) => {
                self.size = fcntl_getpipe_size(&r)
                    .ok()
                    .and_then(NonZeroUsize::new)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("Failed to get pipe size while fcntl returned error: {e}"),
                        )
                    })?;

                Err(io::Error::from_raw_os_error(e.raw_os_error()))
            }
        }
    }

    #[inline]
    const fn is_drain_done(&self) -> bool {
        matches!(self.w, Fd::Reserved(_) | Fd::Closed)
    }

    #[inline]
    /// Close the pipe write side file descriptor.
    fn set_drain_done(&mut self) {
        self.w.set_reserved();
    }

    #[inline]
    #[allow(unused)]
    const fn is_pump_done(&self) -> bool {
        matches!(self.r, Fd::Reserved(_) | Fd::Closed)
    }

    #[inline]
    /// Close the pipe read side file descriptor.
    fn set_pump_done(&mut self) {
        self.r.set_reserved();
    }

    #[inline]
    /// Returns the capacity of the pipe, in bytes.
    pub const fn size(&self) -> NonZeroUsize {
        self.size
    }
}

enum Ret {
    /// Has drained some data from source to pipe and can continue to pump the
    /// data to the destination; or all drained data has been pumped and can
    /// continue to drain more data.
    Continue,

    /// The draining / pumping process is done.
    ///
    /// For draining, it means no more data to read (i.e., EOF), but there may
    /// still be data in the pipe to pump; for pumping, it means all drained
    /// data has been pumped.
    Done,
}
