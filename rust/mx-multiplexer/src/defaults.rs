//! The timeouts and limits of multiplexer/defaults.h, which the multiplexer
//! and the C++ and Python libraries are built with, generated from it by
//! rust/generate_defaults.py; do not edit, format.sh runs it. Seconds are
//! `Duration`s here, sizes and counts `usize`, and a negative timeout, no
//! deadline, is `None`. docs/semantics.md has the same table with context.

use std::time::Duration;

/// Unread messages a client library holds before dropping new ones.
pub const DEFAULT_INCOMING_QUEUE_MAX_SIZE: usize = 1024;

/// Protocol frames, a welcome, a heartbeat, a status reply or a routing
/// request, that a connection's full queue still takes past its limit, so
/// that ordinary traffic cannot keep them out; past that they are dropped
/// as any message is, and a peer that sends control requests without
/// reading cannot grow the queue for good.
pub const FORCED_FRAMES_PAST_FULL_QUEUE: usize = 64;

/// Seconds between a connection dropping and the library reconnecting.
pub const AUTO_RECONNECT_TIME: Duration = Duration::from_secs(3);

/// Seconds a client gives the TCP connect to one address before it tries
/// the next its target resolved to, or ends the attempt: an address that
/// drops the SYNs held the attempt for the kernel's own timeout, two
/// minutes on Linux.
pub const CONNECT_ATTEMPT_SECONDS: Duration = Duration::from_secs(5);

/// Seconds for connect, flush, and each stage of a query.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Seconds a message a synchronous flushing send placed waits for room, or
/// for a connection, past the call's own deadline, so that the call times
/// out first rather than see its message dropped (Client::_send_and_wait).
pub const ROOM_GRACE_SECONDS: Duration = Duration::from_millis(10);

/// A negative timeout means wait forever; used by the receive calls.
pub const DEFAULT_READ_TIMEOUT: Option<Duration> = None;

/// Largest frame body accepted; a bigger one closes the connection.
pub const MAX_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

/// Key filters (Multiplexer.proto's KeyFilter), matched on the multiplexer's
/// one thread. The longest key a message may carry, in bytes: a library
/// refuses a longer one where the message is built, and the multiplexer
/// drops one and tells the sender. The longest pattern, in bytes. How many
/// patterns a filter may have is bounded by the frame that carries it alone
/// (KeyPatterns in multiplexer/key_filters.h says what they cost).
pub const MAX_KEY_SIZE: usize = 256;

/// See [`MAX_KEY_SIZE`].
pub const MAX_KEY_PATTERN_SIZE: usize = 1024;

/// Seconds of silence before a side sends a HEARTBIT.
pub const HEARTBIT_INTERVAL: Duration = Duration::from_secs(3);

/// Seconds a client that shuts down goes on reading what its multiplexers
/// still send, waiting for their end of the stream: a socket closed with
/// something unread makes the kernel reset the connection and throw away
/// what it had not sent yet. See Connection::close_gracefully.
pub const CLOSE_READ_SECONDS: Duration = Duration::from_secs(1);

/// Seconds a shutdown() or close() goes on writing what was sent before it,
/// in every client and server class, before the connections close: what a
/// multiplexer that stopped reading, or a connection that did not come up,
/// leaves unwritten by then is dropped and reported. 0 drops it at once.
pub const CLOSE_FLUSH_SECONDS: Duration = Duration::from_secs(1);

/// Seconds without any frame from a non-passive peer before the multiplexer
/// starts the drop, and how much longer it then waits before closing.
pub const NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL: Duration = Duration::from_secs(30);

/// See [`NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL`].
pub const NO_HEARTBIT_SO_REALLY_DROP_INTERVAL: Duration = Duration::from_secs(60);

/// Seconds between the TCP keepalive probes on the multiplexer's accepted
/// connections, which start after NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL of
/// silence and close a connection after NO_HEARTBIT_SO_REALLY_DROP_INTERVAL
/// of unanswered ones (Server::_handle_accept).
pub const KEEPALIVE_PROBE_INTERVAL: Duration = Duration::from_secs(10);

/// A recording session started over the protocol closes itself at this
/// size unless the request says otherwise (RecordingControl.max_bytes).
pub const DEFAULT_REMOTE_RECORDING_MAX_BYTES: u64 = 1024 * 1024 * 1024;
