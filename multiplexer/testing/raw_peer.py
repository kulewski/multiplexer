"""A peer that speaks the wire format directly, without the client library.

Frame: little-endian uint32 length, uint32 checksum of the body (the low
32 bits of its XXH3-64, which Python's standard library does not have),
body = a serialized MultiplexerMessage. Handshake: the peer sends CONNECTION_WELCOME
carrying a WelcomeMessage first; the multiplexer answers with its own.
docs/wire_format.md is the reference; this file is its executable form.
"""

import random
import socket
import struct
import time
from typing import Any, Mapping

from multiplexer._native import xxh3_64
from multiplexer.Multiplexer_pb2 import MultiplexerMessage, WelcomeMessage
from multiplexer.multiplexer_constants import types

HEADER = struct.Struct("<II")


def checksum(body: bytes) -> int:
    """The checksum a frame's header carries for `body`: the low 32 bits of
    its XXH3-64 (seed 0)."""
    return xxh3_64(body) & 0xFFFFFFFF


def frame(body: bytes) -> bytes:
    """A complete frame for `body`: the 8-byte header followed by the body."""
    return HEADER.pack(len(body), checksum(body)) + body


class RawPeer:
    """One TCP connection to a multiplexer, driven by hand: connect, then
    handshake(), then send() and receive() as the scenario needs."""

    def __init__(
        self,
        endpoint: tuple[str, int],
        peer_type: int,
        instance_id: int | None = None,
        source_address: str | None = None,
    ):
        self.sock = socket.create_connection(
            endpoint, timeout=10, source_address=(source_address, 0) if source_address else None
        )
        self.instance_id = instance_id or random.randint(1, 2**62)
        self.peer_type = peer_type

    def message(self, payload: bytes, type_: int, **fields: Any) -> MultiplexerMessage:
        """A MultiplexerMessage from this peer: random id, `sender` set,
        `fields` (to=, references=, ...) applied."""
        mxmsg = MultiplexerMessage(id=random.randint(1, 2**62), type=type_, message=payload, **fields)
        mxmsg.sender = self.instance_id
        return mxmsg

    def send(self, payload: bytes, type_: int, **fields: Any) -> int:
        """Send one message and return its id."""
        mxmsg = self.message(payload, type_, **fields)
        self.sock.sendall(frame(mxmsg.SerializeToString()))
        return mxmsg.id

    def send_raw(self, data: bytes) -> None:
        """Write arbitrary bytes, for malformed-input scenarios."""
        self.sock.sendall(data)

    def _read_exactly(self, count: int) -> bytes:
        """Read `count` bytes or raise ConnectionError if the peer closes first."""
        data = b""
        while len(data) < count:
            chunk = self.sock.recv(count - len(data))
            if not chunk:
                raise ConnectionError("connection closed after %d of %d bytes" % (len(data), count))
            data += chunk
        return data

    def receive(self, timeout: float = 10) -> MultiplexerMessage:
        """Read one frame, check its checksum and return the parsed message."""
        self.sock.settimeout(timeout)
        length, expected = HEADER.unpack(self._read_exactly(HEADER.size))
        body = self._read_exactly(length)
        if checksum(body) != expected:
            raise ValueError("checksum mismatch")
        mxmsg = MultiplexerMessage()
        mxmsg.ParseFromString(body)
        return mxmsg

    def receive_type(self, type_: int, timeout: float = 10) -> MultiplexerMessage:
        """Skip messages of other types (heartbeats) until one of `type_`
        arrives, within `timeout` in all: TimeoutError once it passed,
        however many other messages came meanwhile, where each one, a
        heartbeat every 3 s, gave the wait its whole timeout again."""
        deadline = time.monotonic() + timeout
        while True:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError("no message of type %d within %ss" % (type_, timeout))
            mxmsg = self.receive(left)
            if mxmsg.type == type_:
                return mxmsg

    def handshake(
        self, key_filters: Mapping[int, list[str]] | None = None
    ) -> tuple[MultiplexerMessage, WelcomeMessage]:
        """Send our CONNECTION_WELCOME, carrying `key_filters` (message type to
        its patterns) when given, and wait for the multiplexer's; returns its
        envelope and the WelcomeMessage inside (type MULTIPLEXER, its id)."""
        welcome = WelcomeMessage(type=self.peer_type, id=self.instance_id)
        for type_, patterns in (key_filters or {}).items():
            welcome.key_filter.add(type=type_, pattern=patterns)
        self.send(welcome.SerializeToString(), types.CONNECTION_WELCOME)
        answer = self.receive_type(types.CONNECTION_WELCOME)
        theirs = WelcomeMessage()
        theirs.ParseFromString(answer.message)
        return answer, theirs

    def closed_by_peer(self, timeout: float = 3) -> bool:
        """True if the other side closed the connection within `timeout` in
        all: what it sends meanwhile, a heartbeat every 3 s, is read and
        dropped and gives the wait no more time."""
        deadline = time.monotonic() + timeout
        try:
            while True:
                left = deadline - time.monotonic()
                if left <= 0:
                    return False
                self.sock.settimeout(left)
                if not self.sock.recv(65536):
                    return True
        except socket.timeout:
            return False
        except OSError:
            return True

    def close(self) -> None:
        """Close our end."""
        self.sock.close()
