//! The core's tests on the virtual clock (core/harness.rs): every case
//! says what the sockets and the multiplexers do and when, and checks what
//! the client did, by the C++ tests and docs each case names.

mod connection;
mod outbox;
mod query;
mod routing;
