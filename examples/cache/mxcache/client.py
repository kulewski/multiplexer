"""Cache: the client of the replicated cache. A read is a query to any one
replica; a write, a delete or a clear is an event to every replica, sent
through every multiplexer unless told otherwise, and stamped, so that a
replica applies only what is newer than what it holds; `add` is a query
that one replica decides, and on a yes this client sends the entry to
every replica, the way its other writes go. A read that fails for want of
a replica or a multiplexer is a miss: a cache that cannot be reached is an
empty one, which is what a caller of a cache expects, and the count of
those is kept for whoever wants to know."""

import collections
import random
import threading
import time

from multiplexer.mxclient import MultiplexerClientError
from multiplexer.threaded_client import BackendError, ThreadedClient

from cache_pb2 import CacheClear, CacheEntry, CacheKey, CacheResult, CacheStats, CacheStatsRequest, CacheValue
from multiplexer_constants import peers, types

WRITES_REMEMBERED = 4096  # writes whose delivery errors are counted; older ones are forgotten


def parse_addresses(text: str) -> list[tuple[str, int]]:
    """ "host:port,host:port" as the list the library takes."""
    return [(host, int(port)) for host, port in (item.rsplit(":", 1) for item in text.split(","))]


class Cache:
    """One ThreadedClient connected to every multiplexer; every method is safe from any thread."""

    ALL = ThreadedClient.ALL  # writes through every multiplexer: each replica gets one copy per multiplexer
    ONE = ThreadedClient.ONE  # writes through one multiplexer, round robin: the walkthrough's ordering step

    def __init__(
        self, addresses: list[tuple[str, int]], writes_through: int = ThreadedClient.ALL, timeout: float = 5.0
    ):
        self.writes_through = writes_through
        self.timeout = timeout
        self.unavailable = 0  # reads answered as misses because no replica or no multiplexer could be reached
        self.lost_writes = 0  # writes each multiplexer named said it could deliver to no replica
        self._gathers: dict[int, list[CacheStats]] = {}
        self._refusals: collections.OrderedDict[int, int] = collections.OrderedDict()  # write id: delivery errors
        self._multiplexers = len(addresses)
        self._version = 0
        self._lock = threading.Lock()
        self._client = ThreadedClient(addresses, type=peers.CACHE_CLIENT, on_message=self._on_message)

    def lookup(self, key: str) -> CacheValue | None:
        """One read: the replica's answer, with which replica it was; None when none could be asked."""
        try:
            reply = self._client.query(CacheKey(key=key), type=types.CACHE_GET, timeout=self.timeout)
        except (MultiplexerClientError, BackendError):
            self.unavailable += 1
            return None
        value = CacheValue()
        value.ParseFromString(reply.message)
        return value

    def get(self, key: str) -> bytes | None:
        """The value, or None for a miss of any kind."""
        value = self.lookup(key)
        return value.value if value is not None and value.found else None

    def set(self, key: str, value: bytes, expires: float = 0.0) -> None:
        """Write to every replica; `expires` is unix time, 0 for never."""
        self._write(self._entry(key, value, expires), types.CACHE_SET)

    def delete(self, key: str) -> None:
        """Drop the key on every replica."""
        version, writer = self._stamp()
        self._write(CacheKey(key=key, version=version, writer=writer), types.CACHE_DELETE)

    def clear(self) -> None:
        """Drop everything on every replica."""
        version, writer = self._stamp()
        self._write(CacheClear(version=version, writer=writer), types.CACHE_CLEAR)

    def add(self, key: str, value: bytes, expires: float = 0.0) -> bool:
        """Set unless present, decided by one replica; False when it was present, or when nobody could decide.
        On a yes, the entry goes to every replica from here, in order with this client's later reads and
        writes, which a copy from the deciding replica, through one multiplexer, would not be."""
        entry = self._entry(key, value, expires)
        try:
            reply = self._client.query(entry, type=types.CACHE_ADD, timeout=self.timeout)
        except (MultiplexerClientError, BackendError):
            self.unavailable += 1
            return False
        result = CacheResult()
        result.ParseFromString(reply.message)
        if result.done:
            self._write(entry, types.CACHE_SET)
        return result.done

    def stats(self, wait: float = 0.5) -> list[CacheStats]:
        """Every replica's numbers: one request to all of them, through every multiplexer so that a
        replica that is on one only is asked too, and whatever answered within `wait` seconds."""
        request_id = random.getrandbits(63)
        with self._lock:
            self._gathers[request_id] = []
        self._client.send_message(
            CacheStatsRequest(request_id=request_id), type=types.CACHE_STATS_REQUEST, multiplexer=ThreadedClient.ALL
        )
        time.sleep(wait)
        with self._lock:
            return sorted(self._gathers.pop(request_id), key=lambda stats: stats.replica)

    def close(self) -> None:
        """Shut the client down; the object is done."""
        self._client.shutdown()

    def _stamp(self) -> tuple[int, int]:
        """A stamp newer than every one this client made before: the clock in nanoseconds, one more when
        it has not moved on, or went back; this client's instance id for a tie with another writer."""
        with self._lock:
            self._version = max(time.time_ns(), self._version + 1)
            return self._version, self._client.instance_id

    def _entry(self, key: str, value: bytes, expires: float) -> CacheEntry:
        """A stamped entry."""
        version, writer = self._stamp()
        return CacheEntry(key=key, value=value, expires=expires, version=version, writer=writer)

    def _write(self, message, kind: int) -> None:
        """An event to every replica, its id remembered for the delivery errors it may bring."""
        mxmsg_id = self._client.send_message(message, type=kind, multiplexer=self.writes_through)
        with self._lock:
            self._refusals[mxmsg_id] = 0
            while len(self._refusals) > WRITES_REMEMBERED:
                self._refusals.popitem(last=False)

    def _on_message(self, mxmsg) -> None:
        """On the io thread: a replica's numbers, or a multiplexer saying a message reached nobody. A write
        is lost once each multiplexer this client names said so, one for a write through one; one that is
        down says nothing, so a write lost while it is goes uncounted. A stats request is not a write."""
        if mxmsg.type == types.CACHE_STATS:
            stats = CacheStats()
            stats.ParseFromString(mxmsg.message)
            with self._lock:
                gather = self._gathers.get(stats.request_id)
                # A replica answers the request's second copy only when its library forgot the first.
                if gather is not None and all(held.instance_id != stats.instance_id for held in gather):
                    gather.append(stats)
        elif mxmsg.type == types.DELIVERY_ERROR:
            copies = self._multiplexers if self.writes_through == ThreadedClient.ALL else 1
            with self._lock:
                refused = self._refusals.get(mxmsg.references)
                if refused is None:
                    return
                if refused + 1 < copies:
                    self._refusals[mxmsg.references] = refused + 1
                    return
                del self._refusals[mxmsg.references]
                self.lost_writes += 1
