# tokio-uni-stream

[![Test pipeline](https://github.com/hanyu-dev/tokio-uni-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/hanyu-dev/tokio-uni-stream/actions/workflows/ci.yml?query=branch%3Amain)
[![Crates.io](https://img.shields.io/crates/v/tokio-uni-stream)](https://crates.io/crates/tokio-uni-stream)
[![Docs.rs](https://docs.rs/tokio-uni-stream/badge.svg)](https://docs.rs/tokio-uni-stream)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](http://opensource.org/licenses/MIT)

Combines both `TcpStream` and `UnixStream` into a single [`UniStream`] type, and provides a fallback type for non-Unix platforms.

> [!WARNING]
> This crate is renamed and deprecated, use [`uni-socket`](https://crates.io/crates/uni-socket) instead.

## License

Licensed under either of

- Apache License, Version 2.0, (LICENSE-APACHE or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license (LICENSE-MIT or <http://opensource.org/licenses/MIT>)

at your option.
