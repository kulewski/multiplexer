"""The Django cache backend over the replicas:

    CACHES = {
        "default": {
            "BACKEND": "mxcache.backend.MultiplexerCache",
            "LOCATION": "10.0.0.1:1980,10.0.0.2:1980",   # every multiplexer
        }
    }

Every web process then shares one cache, whatever server runs them and
however many there are, which locmem never gives. One threaded client per
process, made on first use, forgotten in a forked child and closed at exit,
serves every thread's backend instance, since Django makes one per thread.
Values are pickled here; the replicas store bytes."""

import atexit
import os
import pickle
import threading
import time

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache

from mxcache.client import Cache, parse_addresses

_clients: dict[tuple[tuple[str, int], ...], Cache] = {}
_lock = threading.Lock()


def _forget_in_child() -> None:
    """The child of a fork gets none of the parent's clients, and no lock a
    parent thread may have held at the fork. Closed in the child, an
    inherited client closes only the child's copies of its connections."""
    global _lock
    for cache in _clients.values():
        cache.close()
    _clients.clear()
    _lock = threading.Lock()


def _close_at_exit() -> None:
    """At exit every client writes what it was sent, a second at most, then
    closes: nothing else would, since each refers to itself through its
    callback and is never freed."""
    with _lock:
        caches = list(_clients.values())
        _clients.clear()
    for cache in caches:
        cache.close()


os.register_at_fork(after_in_child=_forget_in_child)
atexit.register(_close_at_exit)


def shared_cache(addresses: list[tuple[str, int]]) -> Cache:
    """The process's one client for these multiplexers, made on the first call."""
    key = tuple(addresses)
    with _lock:
        if key not in _clients:
            _clients[key] = Cache(list(key))
        return _clients[key]


class MultiplexerCache(BaseCache):
    """Django's cache API: get, set, add, touch, delete, clear here; the base class derives the rest."""

    def __init__(self, location: str, params: dict):
        super().__init__(params)
        self._addresses = parse_addresses(location)

    @property
    def cache(self) -> Cache:
        """The shared client."""
        return shared_cache(self._addresses)

    def _expires(self, timeout) -> float:
        """Django's timeout as the replicas' absolute time, 0 for never."""
        when = self.get_backend_timeout(timeout)
        return 0.0 if when is None else when

    def get(self, key, default=None, version=None):
        """The value from any one replica, `default` when it has none."""
        raw = self.cache.get(self.make_and_validate_key(key, version=version))
        return default if raw is None else pickle.loads(raw)

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        """The value to every replica, pickled; a timeout of 0 deletes the key instead."""
        key = self.make_and_validate_key(key, version=version)
        expires = self._expires(timeout)
        if expires and expires <= time.time():  # a timeout of 0: expire at once
            self.cache.delete(key)
            return
        self.cache.set(key, pickle.dumps(value, pickle.HIGHEST_PROTOCOL), expires)

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        """Set unless present, as one replica decides; whether it was set."""
        key = self.make_and_validate_key(key, version=version)
        expires = self._expires(timeout)
        if expires and expires <= time.time():
            return False
        return self.cache.add(key, pickle.dumps(value, pickle.HIGHEST_PROTOCOL), expires)

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        """A new timeout for the key, whether it was there. A read and a write,
        not one operation: a write that lands between them is undone by this
        one, which writes back the value it read."""
        key = self.make_and_validate_key(key, version=version)
        raw = self.cache.get(key)
        if raw is None:
            return False
        self.cache.set(key, raw, self._expires(timeout))
        return True

    def delete(self, key, version=None):
        """The key gone from every replica. An event: whether it existed is not asked, so this returns True."""
        self.cache.delete(self.make_and_validate_key(key, version=version))
        return True

    def clear(self):
        """Everything gone from every replica."""
        self.cache.clear()

    def close(self, **kwargs):
        """Nothing: the client is the process's, not the request's."""
