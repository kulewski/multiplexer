"""Filling a connection whose far end reads nothing, whatever this
machine's TCP buffers. A test that needs messages to wait, in a client's
queue or in a multiplexer's, first sends FILL_FRAMES frames of
fill_size() bytes: together twice what the connection's two sockets may
hold at most, the largest send buffer and the largest receive buffer the
kernel allows, so that the sockets are full however far the kernel grew
them, in so few frames that a queue counting messages keeps room for the
test's own. Filled with frames of the test's own size instead, a
connection took hundreds of thousands of them on a machine whose kernel
allows large buffers: past a queue's count and a test's time.
"""

FILL_FRAMES = 32  # frames that fill a connection's two sockets: few beside a queue of 1024
QUEUE = 1024  # a connection's queue in the client libraries, and the multiplexer's by default, in messages


def socket_bytes() -> int:
    """The most the two sockets of one connection may hold: the largest
    send buffer and the largest receive buffer the kernel allows, the third
    field of /proc/sys/net/ipv4/tcp_wmem and tcp_rmem, 8 MiB each where
    /proc does not say."""
    total = 0
    for path in ("/proc/sys/net/ipv4/tcp_wmem", "/proc/sys/net/ipv4/tcp_rmem"):
        try:
            with open(path) as limits:
                total += int(limits.read().split()[2])
        except (OSError, IndexError, ValueError):
            total += 8 << 20  # a guess where /proc does not say
    return total


def fill_size(least: int = 64 * 1024) -> int:
    """The size of each of FILL_FRAMES frames that together carry twice
    socket_bytes(): `least` at the least."""
    return max(least, -(-2 * socket_bytes() // FILL_FRAMES))


def fill_frames() -> list[bytes]:
    """The FILL_FRAMES payloads of fill_size() bytes, to send first."""
    return [b"f" * fill_size()] * FILL_FRAMES


def past_the_queue(payload: bytes) -> list[bytes]:
    """What a frozen multiplexer's connection cannot take: fill_frames(),
    then twice its queue of `payload`. Past them, messages wait for room."""
    return fill_frames() + [payload] * (2 * QUEUE)
