// Every timeout and limit, compiled into the multiplexer and both client
// libraries (the Python binding exports them as module attributes).
// docs/semantics.md has the same table with context. Changing one means
// rebuilding everything that embeds it, and the heartbeat intervals must
// agree between a multiplexer and its peers.
#ifndef MX_MULTIPLEXER_DEFAULTS_H_
#define MX_MULTIPLEXER_DEFAULTS_H_

#include <cstdint>

namespace multiplexer {

// Unread messages a client library holds before dropping new ones.
static const unsigned int DEFAULT_INCOMING_QUEUE_MAX_SIZE = 1024;
// Protocol frames, a welcome, a heartbeat, a status reply or a routing
// request, that a connection's full queue still takes past its limit, so
// that ordinary traffic cannot keep them out; past that they are dropped
// as any message is, and a peer that sends control requests without
// reading cannot grow the queue for good.
static const unsigned int FORCED_FRAMES_PAST_FULL_QUEUE = 64;
// Seconds between a connection dropping and the library reconnecting.
static const unsigned int AUTO_RECONNECT_TIME = 3;
// Seconds for connect, flush, and each stage of a query.
static const float DEFAULT_TIMEOUT = 10.0;
// Seconds a message a synchronous flushing send placed waits for room, or
// for a connection, past the call's own deadline, so that the call times
// out first rather than see its message dropped (Client::_send_and_wait).
static const float ROOM_GRACE_SECONDS = 0.01f;
// A negative timeout means wait forever; used by the receive calls.
static const float DEFAULT_READ_TIMEOUT = -1;
// Largest frame body accepted; a bigger one closes the connection.
static const unsigned int MAX_MESSAGE_SIZE = 128 * 1024 * 1024;

// Seconds of silence before a side sends a HEARTBIT.
static const float HEARTBIT_INTERVAL = 3.0;
// Seconds a client that shuts down goes on reading what its multiplexers
// still send, waiting for their end of the stream: a socket closed with
// something unread makes the kernel reset the connection and throw away
// what it had not sent yet. See Connection::close_gracefully.
static const float CLOSE_READ_SECONDS = 1.0;
// Seconds a shutdown() or close() goes on writing what was sent before it,
// in every client and server class, before the connections close: what a
// multiplexer that stopped reading, or a connection that did not come up,
// leaves unwritten by then is dropped and reported. 0 drops it at once.
static const float CLOSE_FLUSH_SECONDS = 1.0;
// Seconds without any frame from a non-passive peer before the multiplexer
// starts the drop, and how much longer it then waits before closing.
static const float NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL = 30;
static const float NO_HEARTBIT_SO_REALLY_DROP_INTERVAL = 60;
// Seconds between the TCP keepalive probes on the multiplexer's accepted
// connections, which start after NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL of
// silence and close a connection after NO_HEARTBIT_SO_REALLY_DROP_INTERVAL
// of unanswered ones (Server::_handle_accept).
static const float KEEPALIVE_PROBE_INTERVAL = 10;

// A recording session started over the protocol closes itself at this
// size unless the request says otherwise (RecordingControl.max_bytes).
static const std::uint64_t DEFAULT_REMOTE_RECORDING_MAX_BYTES = 1024ULL * 1024 * 1024;

};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_DEFAULTS_H_
