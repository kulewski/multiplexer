//! Why a call of the library failed: one enum for every class, its
//! variants named after the Python exceptions, and the C++ outcome or
//! exception beside each, so that a page about one library reads for the
//! others.
//!
//! `NotConnected`, `OperationTimedOut` and `OperationFailed` are a query's
//! and a flushing send's three ways to fail, as docs/query.md's table says
//! when each comes. The others are a caller's mistake caught at the call,
//! before anything is queued.

use std::fmt;

use crate::handles::Incoming;

/// Which limit a value went over, for `Error::TooLarge`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Limit {
    /// A message's serialized size, over `MAX_MESSAGE_SIZE`.
    Message,
    /// A key's length in bytes, over `MAX_KEY_SIZE`.
    Key,
}

/// The ways a call fails.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// A query's reply was a BACKEND_ERROR: the backend's handler failed,
    /// its payload saying why. Python `BackendError`; in C++ the reply
    /// itself.
    Backend(Box<Incoming>),
    /// No connection is live, or none came up in time; a pinned lane's
    /// connection is gone; the client was shut down. C++ `NotConnected`,
    /// Python `NotConnected`.
    NotConnected,
    /// A query's stage, or a flushing send, ran out of time with a
    /// connection live. C++ `OperationTimedOut`, Python `OperationTimedOut`.
    OperationTimedOut,
    /// No backend takes the request: the search found nobody, the backend
    /// it found is gone, or an addressed query's instance is gone. C++
    /// `OperationFailed`, Python `OperationFailed`.
    OperationFailed,
    /// A message over `MAX_MESSAGE_SIZE`, or a key over `MAX_KEY_SIZE`,
    /// refused at the call. C++ `std::length_error`, Python `ValueError`.
    TooLarge {
        /// Which limit.
        what: Limit,
        /// The size refused, in bytes.
        size: usize,
        /// The limit, in bytes.
        limit: usize,
    },
    /// An argument the call cannot take: another client's lane or
    /// connection, a key filter for a protocol type or with a pattern over
    /// `MAX_KEY_PATTERN_SIZE`, an address that does not parse. C++
    /// `std::invalid_argument`, Python `ValueError`.
    InvalidArgument(String),
    /// A call that waits, made from one of the client's own callbacks on its
    /// io thread, where it would wait for good. C++ `std::logic_error`,
    /// Python `RuntimeError`.
    OnIoThread,
    /// A call on a client this process inherited across `fork()`. C++ and
    /// Python `UsedAfterFork`.
    UsedAfterFork,
    /// The system refused what a client needs, an epoll instance or a
    /// thread: the reason, as the system gave it. C++ `std::system_error`.
    Io(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Backend(reply) => {
                write!(formatter, "the backend failed: {}", String::from_utf8_lossy(&reply.message.payload))
            }
            Error::NotConnected => write!(formatter, "not connected"),
            Error::OperationTimedOut => write!(formatter, "operation timed out"),
            Error::OperationFailed => write!(formatter, "operation failed: no backend takes the request"),
            Error::TooLarge { what, size, limit } => {
                let name = match what {
                    Limit::Message => "a message of",
                    Limit::Key => "a key of",
                };
                let constant = match what {
                    Limit::Message => "MAX_MESSAGE_SIZE",
                    Limit::Key => "MAX_KEY_SIZE",
                };
                write!(formatter, "{name} {size} bytes, over the limit of {limit} ({constant})")
            }
            Error::InvalidArgument(why) => write!(formatter, "invalid argument: {why}"),
            Error::OnIoThread => write!(formatter, "a call that waits, made on the client's own io thread"),
            Error::UsedAfterFork => write!(formatter, "client used after fork; create a new one in the child"),
            Error::Io(why) => write!(formatter, "the system refused: {why}"),
        }
    }
}

impl std::error::Error for Error {}

/// The result of a call that may fail.
pub type Result<T> = std::result::Result<T, Error>;
