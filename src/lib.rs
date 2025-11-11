#![doc = include_str!("../README.md")]

#[cfg(any(test, not(unix)))]
mod default;
#[cfg(unix)]
mod unix;

#[cfg(not(unix))]
pub use self::default::Stream;
#[cfg(unix)]
pub use self::unix::Stream;
