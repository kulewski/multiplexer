// Every timeout and limit, compiled into the multiplexer and both client
// libraries (the Python binding exports them as module attributes).
// docs/guarantees.md has the same table with context. Changing one means
// rebuilding everything that embeds it, and the heartbeat intervals must
// agree between a multiplexer and its peers.
#ifndef MX_MULTIPLEXER_DEFAULTS_H_
#define MX_MULTIPLEXER_DEFAULTS_H_

#include <boost/cstdint.hpp>

namespace multiplexer {

// Unread messages a client library holds before dropping new ones.
static const unsigned int DEFAULT_INCOMING_QUEUE_MAX_SIZE = 1024;
// Seconds between a connection dropping and the library reconnecting.
static const unsigned int AUTO_RECONNECT_TIME = 3;
// Seconds for connect, flush, and each stage of a query.
static const float DEFAULT_TIMEOUT = 10.0;
// A negative timeout means wait forever; used by the receive calls.
static const float DEFAULT_READ_TIMEOUT = -1;
// Largest frame body accepted; a bigger one closes the connection.
static const unsigned int MAX_MESSAGE_SIZE = 128 * 1024 * 1024;

// Seconds of silence before a side sends a HEARTBIT.
static const float HEARTBIT_INTERVAL = 3.0;
// Seconds without any frame from a non-passive peer before the multiplexer
// starts the drop, and how much longer it then waits before closing.
static const float NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL = 30;
static const float NO_HEARTBIT_SO_REALLY_DROP_INTERVAL = 60;

// A recording session started over the protocol closes itself at this
// size unless the request says otherwise (RecordingControl.max_bytes).
static const boost::uint64_t DEFAULT_REMOTE_RECORDING_MAX_BYTES = 1024ULL * 1024 * 1024;

}; // namespace multiplexer

#endif // MX_MULTIPLEXER_DEFAULTS_H_
