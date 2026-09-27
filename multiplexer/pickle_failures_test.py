"""query_pickle's failures reach the caller, in every client: a reply
that is no pickle reaches a ThreadedClient's callback as the exception
unpickling raised, where it was raised on the io thread, printed, and the
callback never ran; and `with_connection`, whose result has no payload to
unpickle, raises TypeError, where SyncClient, ThreadedClient and
AsyncClient sent the query and failed on the tuple with AttributeError,
or, with a callback, never called it.
"""

import asyncio
import pickle
import queue
import unittest
from typing import Any

from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, runfile
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")


class NoPickle(BaseMultiplexerServer):
    """A backend answering every request with bytes that do not unpickle."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)

    def handle_message(self, mxmsg: Any) -> None:
        """No pickle back to the requester."""
        self.send_message(message=b"no pickle", type=types.PYTHON_TEST_RESPONSE, flush=True)


class PickleFailuresTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_reply_that_does_not_unpickle_reaches_the_callback(self) -> None:
        with Cluster(1, rules=RULES) as cluster, BackendThread(lambda: NoPickle(cluster.endpoints)):
            cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
            with ThreadedClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT) as client:
                results: "queue.Queue[Any]" = queue.Queue()
                client.query_pickle({"a": 1}, types.PYTHON_TEST_REQUEST, callback=results.put)
                self.assertIsInstance(results.get(timeout=30), pickle.UnpicklingError)

    def test_with_connection_raises_type_error(self) -> None:
        with Cluster(1, rules=RULES) as cluster, BackendThread(lambda: NoPickle(cluster.endpoints)):
            cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
            sync = Client(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
            try:
                with self.assertRaises(TypeError):
                    sync.query_pickle(1, types.PYTHON_TEST_REQUEST, with_connection=True)
            finally:
                sync.shutdown()
            with ThreadedClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT) as threaded:
                with self.assertRaises(TypeError):
                    threaded.query_pickle(1, types.PYTHON_TEST_REQUEST, with_connection=True)
                with self.assertRaises(TypeError):
                    threaded.query_pickle(1, types.PYTHON_TEST_REQUEST, callback=print, with_connection=True)

            async def ask() -> None:
                """The same through an AsyncClient."""
                client = AsyncClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT)
                try:
                    with self.assertRaises(TypeError):
                        await client.query_pickle(1, types.PYTHON_TEST_REQUEST, with_connection=True)
                finally:
                    client.close()

            asyncio.run(ask())


if __name__ == "__main__":
    unittest.main()
