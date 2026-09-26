"""A replica of the cache: a backend on `BaseMultiplexerServer` that holds
the whole cache in memory. A write, a delete or a clear reaches every
replica as an event and is applied; a read is answered by whichever replica
the multiplexer picked.

    python replica.py [ADDRESSES] [--name NAME] [--drain-file PATH]

ADDRESSES is host:port of every multiplexer, comma-separated, default
127.0.0.1:1980. The replica is a BaseMultiplexerServer, the plain one: a
dictionary operation takes microseconds, so nothing here needs a worker
thread, and the loop that talks to the multiplexers is the loop that
serves. Every write carries a stamp, and a replica applies only what is
newer than what it holds, so copies arriving late or twice change nothing.
A replica starts empty and warms as keys are written again: the
multiplexer replays nothing, and the walkthrough says why that is the
honest cache. Creating --drain-file asks the replica to leave the way a
deployment's rolling restart would."""

import argparse
import os
import socket
import time

from multiplexer.servers import BaseMultiplexerServer

from cache_pb2 import CacheClear, CacheEntry, CacheKey, CacheResult, CacheStats, CacheStatsRequest, CacheValue
from multiplexer_constants import peers, types
from mxcache.client import parse_addresses

PURGE_EVERY = 5.0  # seconds between sweeps of expired entries and old deletes
KEEP_DELETES = 300.0  # seconds a delete's stamp is kept, against a late copy of an older write
DRAIN_CHECK_EVERY = 0.5  # seconds between looks for the drain file

Stamp = tuple[int, int]  # a write's (version, writer); larger is newer


class Replica(BaseMultiplexerServer):
    """The cache, and the handler for each of its messages."""

    multiplexer_client_type = peers.CACHE

    def __init__(self, addresses: list[tuple[str, int]], name: str | None = None, drain_file: str | None = None):
        super().__init__(addresses)
        self.name = name or f"{socket.gethostname()}:{os.getpid()}"
        self.drain_file = drain_file
        self.entries: dict[str, tuple[bytes, float, Stamp]] = {}  # key: (value, expires, 0 for never; stamp)
        self.deleted: dict[str, tuple[Stamp, float]] = {}  # key: (the delete's stamp, when to forget it)
        self.cleared: Stamp = (0, 0)  # the last clear's stamp: every write stamped before it is gone
        self.writes = 0  # CACHE_SET, CACHE_DELETE and CACHE_CLEAR applied, and CACHE_ADD decided yes here
        self.reads = 0
        self.hits = 0
        self._purged_at = time.time()
        self._drain_checked_at = 0.0

    def handle_message(self, mxmsg) -> None:
        """A read answered, a write applied, a question about this replica answered."""
        kind = mxmsg.type
        if kind == types.CACHE_GET:
            key = self.parse_message(CacheKey).key
            self.reads += 1
            held = self._live(key)
            if held is not None:
                self.hits += 1
            self.send_message(
                message=CacheValue(found=held is not None, value=held[0] if held else b"", replica=self.name),
                type=types.CACHE_VALUE,
            )
        elif kind == types.CACHE_SET:
            self._set(self.parse_message(CacheEntry))
            self.no_response()
        elif kind == types.CACHE_DELETE:
            key = self.parse_message(CacheKey)
            stamp = (key.version, key.writer)
            if self._newer(key.key, stamp):
                self.entries.pop(key.key, None)
                self.deleted[key.key] = (stamp, time.time() + KEEP_DELETES)
                self.writes += 1
            self.no_response()
        elif kind == types.CACHE_CLEAR:
            clear = self.parse_message(CacheClear)
            stamp = (clear.version, clear.writer)
            if stamp > self.cleared:
                self.cleared = stamp
                self.entries = {key: held for key, held in self.entries.items() if held[2] > stamp}
                self.deleted = {key: gone for key, gone in self.deleted.items() if gone[0] > stamp}
                self.writes += 1
            self.no_response()
        elif kind == types.CACHE_ADD:
            # One replica decides: this one, since the multiplexer picked it.
            # Absent, the entry is applied here and the answer is yes; the
            # client then sends it to every replica. Present with the add's
            # own stamp, it is this add again, sent a second time by the
            # client's library after a lost connection: yes again.
            entry = self.parse_message(CacheEntry)
            stamp = (entry.version, entry.writer)
            held = self._live(entry.key)
            if held is None:
                done = self._set(entry)
            else:
                done = held[2] == stamp
            self.send_message(message=CacheResult(done=done, replica=self.name), type=types.CACHE_RESULT)
        elif kind == types.CACHE_STATS_REQUEST:
            # Not a reply: every replica answers the same request, so the
            # answer is addressed to the asker and carries the request's id
            # in its payload, and references nothing.
            request = self.parse_message(CacheStatsRequest)
            self.send_message(
                message=CacheStats(
                    request_id=request.request_id,
                    replica=self.name,
                    instance_id=self.conn.instance_id,
                    entries=len(self.entries),
                    writes=self.writes,
                    reads=self.reads,
                    hits=self.hits,
                ),
                type=types.CACHE_STATS,
                references=0,
            )
        else:
            self.no_response()

    def periodic_task(self) -> None:
        """After every message and every poll: leave once the drain file exists, looked for twice a second
        rather than on every message, and sweep expired entries and old deletes now and then."""
        now = time.time()
        if self.drain_file and now - self._drain_checked_at >= DRAIN_CHECK_EVERY:
            self._drain_checked_at = now
            if os.path.exists(self.drain_file):
                self.start_draining()
        if now - self._purged_at >= PURGE_EVERY:
            self._purged_at = now
            for key in [key for key, (_, expires, _) in self.entries.items() if expires and expires <= now]:
                del self.entries[key]
            for key in [key for key, (_, forget) in self.deleted.items() if forget <= now]:
                del self.deleted[key]

    def _live(self, key: str) -> tuple[bytes, float, Stamp] | None:
        """The live entry, or None; an expired entry goes on the way."""
        held = self.entries.get(key)
        if held is None:
            return None
        if held[1] and held[1] <= time.time():
            del self.entries[key]
            return None
        return held

    def _newer(self, key: str, stamp: Stamp) -> bool:
        """Whether a write stamped `stamp` is newer than all this replica knows of `key`: its entry, a
        delete of it, a clear. A copy that arrives late, or twice, is not."""
        if stamp <= self.cleared:
            return False
        held = self.entries.get(key)
        if held is not None and held[2] >= stamp:
            return False
        gone = self.deleted.get(key)
        return gone is None or gone[0] < stamp

    def _set(self, entry: CacheEntry) -> bool:
        """Apply a write when it is newer than what is held; whether it was."""
        stamp = (entry.version, entry.writer)
        if not self._newer(entry.key, stamp):
            return False
        self.entries[entry.key] = (entry.value, entry.expires, stamp)
        self.deleted.pop(entry.key, None)
        self.writes += 1
        return True


def main() -> None:
    """Serve until asked to leave."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "addresses", nargs="?", default="127.0.0.1:1980", help="host:port of every multiplexer, comma-separated"
    )
    parser.add_argument("--name", help="how this replica signs its answers; default host:pid")
    parser.add_argument("--drain-file", help="creating this file asks the replica to leave gracefully")
    args = parser.parse_args()
    replica = Replica(parse_addresses(args.addresses), args.name, args.drain_file)
    # The line is for whoever runs the steps; nothing waits for it, and serve_forever() connects.
    print(f"ready: replica {replica.name}, instance {replica.conn.instance_id}", flush=True)
    replica.serve_forever(poll=0.5, drain_seconds=5)
    print(
        f"left with {len(replica.entries)} entries after {replica.writes} writes and {replica.reads} reads", flush=True
    )


if __name__ == "__main__":
    main()
