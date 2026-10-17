//! The handles a program holds besides its clients: a `Connection`, one of
//! a client's connections as callers see it (C++ ConnectionWrapper), an
//! `Incoming` message with the connection it came on, and a `Lane`, one
//! connection for a stream of messages (docs/api_cpp.md, "Lanes"). A
//! connection handle and a lane keep no connection alive: they hold what
//! the connection was asked to connect to and its live flag, readable from
//! any thread, and they belong to the client that made them.

use std::net::SocketAddr;

use crate::core::client::{self, Wrapper};
use crate::core::lane;
use crate::message::Message;

/// One of a client's connections: what it was asked to connect to, the
/// address that resolved to, and whether it is live, kept after it is gone.
/// A reply carries the connection it came on, so that an answer can go
/// back the same way.
#[derive(Clone)]
pub struct Connection(pub(crate) Wrapper);

impl Connection {
    /// The host and port the connection was asked for: an address in text,
    /// an IPv6 one without brackets, or a name.
    pub fn target(&self) -> (&str, u16) {
        (self.0.target.0.as_str(), self.0.target.1)
    }

    /// The address in use, or tried last; None before a name resolved.
    pub fn address(&self) -> Option<SocketAddr> {
        self.0.endpoint
    }

    /// Whether the connection is live, from any thread: false once it is
    /// gone or closing.
    pub fn is_live(&self) -> bool {
        self.0.live()
    }
}

impl PartialEq for Connection {
    /// The same connection, or both gone from the same target.
    fn eq(&self, other: &Connection) -> bool {
        self.0.conn == other.0.conn && self.0.target == other.0.target && self.0.owner == other.0.owner
    }
}

impl Eq for Connection {}

impl std::fmt::Debug for Connection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (host, port) = self.target();
        write!(formatter, "Connection({}, live: {})", crate::endpoint::format_endpoint(host, port), self.is_live())
    }
}

/// A message a client read, with the connection it came on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Incoming {
    /// The message.
    pub message: Message,
    /// The connection it came on, the one an answer goes back through.
    pub connection: Connection,
}

impl From<client::Incoming> for Incoming {
    fn from(incoming: client::Incoming) -> Incoming {
        Incoming { message: Message::from(incoming.message), connection: Connection(incoming.connection) }
    }
}

/// One connection for a stream of messages, owned by the caller: a message
/// sent or queried through it goes through its connection while that is
/// live. Empty until first used, when it takes the connection the library
/// chose. A lane that is not pinned lets go at a failover, taking the
/// connection that took what the dead one had not written, and takes the
/// connection a query's reply came through, so that what follows a query
/// follows it. A pinned lane keeps its first connection for good: once that
/// is gone, every send and query through it fails with
/// [`Error::NotConnected`](crate::Error::NotConnected), and a message of
/// its that the dying connection had not written is reported lost. Clones
/// are the same lane; it may be used from any thread.
#[derive(Clone, Debug)]
pub struct Lane(pub(crate) lane::Lane);

impl Lane {
    /// A lane that follows a failover.
    pub fn new() -> Lane {
        Lane(lane::Lane::new(false))
    }

    /// A lane pinned to its first connection.
    pub fn pinned() -> Lane {
        Lane(lane::Lane::new(true))
    }

    /// A lane holding `connection` already, the one a reply came through,
    /// pinned or not.
    pub fn through(connection: &Connection, pinned: bool) -> Lane {
        Lane(lane::Lane::seeded(&connection.0, pinned))
    }

    /// Whether it is pinned.
    pub fn is_pinned(&self) -> bool {
        self.0.pinned()
    }

    /// The connection it holds; None until a message went through, and in
    /// a forked child, where the lane is the parent's.
    pub fn connection(&self) -> Option<Connection> {
        self.0.connection().map(Connection)
    }

    /// Whether the connection it holds is live; false in a forked child.
    pub fn connected(&self) -> bool {
        self.0.connected()
    }

    /// A pinned lane whose connection is gone: nothing goes through it any
    /// more. True for any lane in a forked child, where a send or a query
    /// through it fails with `UsedAfterFork`.
    pub fn closed(&self) -> bool {
        self.0.closed()
    }
}

impl Default for Lane {
    fn default() -> Lane {
        Lane::new()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    //! What other modules' tests build handles from.

    use super::{Connection, Incoming};
    use crate::core::client::Wrapper;
    use crate::message::Message;

    /// A message of id `id` that came on no connection.
    pub(crate) fn incoming(id: u64) -> Incoming {
        let connection = Connection(Wrapper {
            conn: None,
            target: std::sync::Arc::new(("10.0.0.1".to_string(), 1980)),
            endpoint: None,
            living: None,
            owner: 1,
            generation: crate::fork::generation(),
        });
        Incoming { message: Message { id, r#type: 300, ..Default::default() }, connection }
    }
}
