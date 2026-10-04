"""One ThreadedClient per web process, made on first use, forgotten in a
forked child: the holder from docs/recipes/web_server.md. A Django process
handles requests on threads and gunicorn forks workers after importing the
application, and the client is safe from any thread but must never be
inherited across a fork."""

import os
import threading

from django.conf import settings
from multiplexer.threaded_client import ThreadedClient

from multiplexer_constants import peers

_client: ThreadedClient | None = None
_lock = threading.Lock()


def _forget_in_child() -> None:
    """The child of a fork gets no client of the parent's, and no lock a
    parent thread may have held at the fork. Shut down in the child, the
    inherited client closes only the child's copies of its connections."""
    global _client, _lock
    if _client is not None:
        _client.shutdown()
    _client = None
    _lock = threading.Lock()


os.register_at_fork(after_in_child=_forget_in_child)


def mx() -> ThreadedClient:
    """The process's client, connected to every multiplexer, made on the first call."""
    global _client
    with _lock:
        if _client is None:
            _client = ThreadedClient(settings.MULTIPLEXER_ADDRESSES, type=peers.WEB)
        return _client


def reset() -> None:
    """Drop the client, for a test that changes the addresses."""
    global _client
    with _lock:
        if _client is not None:
            _client.shutdown()
        _client = None
