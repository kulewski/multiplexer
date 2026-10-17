"""ClickSource: what a web worker sends its clicks with. One CLICK event per
click, keyed with its shard, shard.0 to shard.3, which a hash of the user
decides, so that a user's clicks always go to the same shard and so to
the same collector; and the type batched, so that the clicks of one shard
go several in one message.

    with ClickSource(addresses) as source:
        source.click("alice", "/pricing")

One ThreadedClient connected to every multiplexer, its io thread writing
what click() hands it; every method is safe from any thread, which is
what a web process's threads need. A click waits at most MAX_DELAY for
others of its shard, and a batch goes once it holds a thousand clicks or
once its first click has waited that long; a flush, close() and the end
of a `with` block let every batch go at once. A delay of 0 sends a click
at once while a connection has nothing to write, and gathers clicks only
while every connection is busy writing; the walkthrough measures both."""

import time
import zlib

from multiplexer.endpoints import Endpoint, parse_endpoint
from multiplexer.threaded_client import DEFAULT_TIMEOUT, ThreadedClient

from clicks_pb2 import Click
from multiplexer_constants import peers, types

SHARDS = 4  # shard.0 to shard.3; every source and every collector must agree
MAX_DELAY = 0.05  # seconds a click waits for others of its shard


def parse_addresses(text: str) -> list[Endpoint]:
    """ "host:port,host:port", an IPv6 address as [address]:port, as the list the library takes."""
    return [parse_endpoint(item) for item in text.split(",")]


def shard_key(user: str, shards: int = SHARDS) -> str:
    """The key of `user`'s clicks: shard.N, N from a CRC-32 of the user's
    name, which every process computes alike, where Python's own hash() of
    a string differs from one process to the next."""
    return f"shard.{zlib.crc32(user.encode()) % shards}"


class ClickSource:
    """One ThreadedClient connected to every multiplexer, CLICK batched by shard."""

    def __init__(
        self, addresses: list[Endpoint], shards: int = SHARDS, batching: bool = True, max_delay: float = MAX_DELAY
    ):
        """Connect to every multiplexer, all at once, waiting ten seconds at
        most; one that is not reachable yet is tried again in the
        background. `batching` False sends every click alone."""
        self.shards = shards
        self._client = ThreadedClient([], type=peers.CLICK_SOURCE)
        if batching:
            # Every key of the type apart, so a batch holds one shard's clicks;
            # its time counts from its first click: the delay, and ten seconds.
            self._client.set_delivery(types.CLICK, batch=True, max_delay=max_delay, timeout=max_delay + DEFAULT_TIMEOUT)
        self._client.connect_all(addresses)

    def click(self, user: str, page: str) -> None:
        """Send one click; returns once the io thread has it."""
        click = Click(user=user, page=page, time_ns=time.time_ns())
        self._client.send_message(click, type=types.CLICK, key=shard_key(user, self.shards))

    def flush(self, timeout: float = 10.0) -> bool:
        """Wait until every click sent before the call is written, the open
        batches let go first, or `timeout` seconds; whether every one was."""
        return self._client.flush_all(timeout)

    @property
    def dropped(self) -> int:
        """Clicks given up on: no multiplexer took them within ten seconds, or the close came first."""
        return self._client.dropped

    def close(self) -> None:
        """Let the batches go and write what was sent, a second at most, then
        disconnect; the source is done."""
        self._client.shutdown()

    def __enter__(self) -> "ClickSource":
        """The source, for a `with` block, at whose end it is closed."""
        return self

    def __exit__(self, *exc: object) -> None:
        """close(), however the block ended."""
        self.close()
