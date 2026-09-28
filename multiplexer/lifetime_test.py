"""What ends a client or a server, and what dropping one does: a `with`
block ends every class however the block ended, shutdown() for the
clients and close() for the servers; a ThreadedClient given a callback, as
every AsyncClient and threaded server is, lives, connected, until it is
shut down, dropped or not, as a running thread does, and is freed once
shut down; one given none is freed when dropped. docs/api_python.md,
"Lifetimes"."""

import gc
import unittest
import weakref
from collections.abc import Callable
from typing import Any

from multiplexer.clients import SyncClient
from multiplexer.multiplexer_constants import peers
from multiplexer.mxclient import NotConnected
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import Cluster, runfile
from multiplexer.threaded_client import ThreadedClient
from multiplexer.threaded_server import BaseThreadedMultiplexerServer

RULES = runfile("tests/testing.rules")  # the file the constants were generated from


def connected(server: Any) -> Any:
    """The server, connected, as serve_forever() would first make it."""
    server.connect()
    return server


def connections(count: Callable[[], int]) -> int | str:
    """A connection count, or the name of what asking raised: a threaded
    client or server that has ended refuses the question."""
    try:
        return count()
    except (NotConnected, RuntimeError) as error:
        return type(error).__name__


class LifetimeTest(unittest.TestCase):
    """See the module docstring."""

    @classmethod
    def setUpClass(cls):
        """One multiplexer for every test."""
        cls.cluster = Cluster(1, rules=RULES).__enter__()

    @classmethod
    def tearDownClass(cls):
        """Stop the multiplexer."""
        cls.cluster.__exit__(None, None, None)

    def test_a_with_block_ends_each_class(self):
        """Inside the block the object is connected; after it, ended, also
        when the block raised, the exception passing through."""
        endpoints = self.cluster.endpoints
        classes: dict[str, tuple[Callable[[], Any], Callable[[Any], int]]] = {
            "ThreadedClient": (
                lambda: ThreadedClient(endpoints, type=peers.PYTHON_TEST_CLIENT),
                lambda client: client.connections_count(),
            ),
            "SyncClient": (
                lambda: SyncClient(endpoints, type=peers.PYTHON_TEST_CLIENT),
                lambda client: client.connections_count(),
            ),
            "BaseMultiplexerServer": (
                lambda: connected(BaseMultiplexerServer(endpoints, type=peers.PYTHON_TEST_SERVER)),
                lambda server: server.conn.connections_count(),
            ),
            "BaseThreadedMultiplexerServer": (
                lambda: connected(BaseThreadedMultiplexerServer(endpoints, type=peers.PYTHON_TEST_SERVER)),
                lambda server: server.client.connections_count(),
            ),
        }
        seen = {}
        for name, (make, count) in classes.items():
            with make() as made:
                inside = connections(lambda count=count: count(made))
            after = connections(lambda count=count: count(made))
            with self.assertRaises(ValueError):
                with make() as raised:
                    raise ValueError("the block's own")
            seen[name] = (inside, after, connections(lambda count=count: count(raised)))
        self.assertEqual(
            {
                "ThreadedClient": (1, "NotConnected", "NotConnected"),
                "SyncClient": (1, 0, 0),
                "BaseMultiplexerServer": (1, 0, 0),
                "BaseThreadedMultiplexerServer": (1, "RuntimeError", "RuntimeError"),
            },
            seen,
            "the connection count inside the block, after it, and after a block that raised",
        )

    def test_a_client_given_a_callback_lives_until_shut_down(self):
        """Dropped while running, a ThreadedClient given on_message stays
        alive and connected, its io thread holding the callback that refers
        back to it; shut down, it is freed. One given no callback is freed,
        and shut down by its destructor, when dropped."""
        endpoints = self.cluster.endpoints
        with_callback = weakref.ref(
            ThreadedClient(endpoints, type=peers.PYTHON_TEST_CLIENT, on_message=lambda mxmsg: None)
        )
        without_callback = weakref.ref(ThreadedClient(endpoints, type=peers.PYTHON_TEST_CLIENT))
        gc.collect()
        alive = with_callback()
        assert alive is not None, "a running client given a callback was freed"
        connected_while_dropped = alive.connections_count()
        alive.shutdown()
        del alive
        gc.collect()
        self.assertEqual(
            (1, None, None),
            (connected_while_dropped, with_callback(), without_callback()),
            "the dropped client's connections, then whether each client is still alive",
        )


if __name__ == "__main__":
    unittest.main()
