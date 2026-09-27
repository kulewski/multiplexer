"""A timeout of math.inf, sys.maxsize, or of more seconds than a steady
clock can add, waits as long as it takes, in every client: a connect
connects, a query gets its reply, a flushing send and flush_all() return
written, a receive returns the message. Every conversion took such a
timeout past the range of its integer, to a deadline in the past, and the
call gave up at once, a request already sent. The Python threaded server
polls with such a poll too, where Python's own wait raised OverflowError.
And a NaN timeout is no time at all in every client: a synchronous
client's receive gives up at once, where it waited forever. Ordered, not
timed: the backend is connected before the queries and the message is
sent before the receive; nothing waits on a clock but the bound on a
wait that used to never end.
"""

import asyncio
import math
import sys
import threading
import unittest
from typing import Any

from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationTimedOut
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, runfile
from multiplexer.threaded_client import ThreadedClient
from multiplexer.threaded_server import BaseThreadedMultiplexerServer, Request

RULES = runfile("tests/testing.rules")
FOREVER = (math.inf, sys.maxsize, 1e10)  # past the range of the old conversion, or of a clock's nanoseconds


class Echo(BaseMultiplexerServer):
    """A backend answering every request with its payload."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)

    def handle_message(self, mxmsg: Any) -> None:
        """The payload back to the requester."""
        self.send_message(message=mxmsg.message, type=types.PYTHON_TEST_RESPONSE, flush=True)


class ThreadedEcho(BaseThreadedMultiplexerServer):
    """A threaded backend answering every request with its payload."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)

    def handle_message(self, request: Request) -> None:
        """The payload back to the requester."""
        request.reply(request.mxmsg.message, type=types.PYTHON_TEST_RESPONSE)


class InfiniteTimeoutTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_sync_client_waits_as_long_as_it_takes(self) -> None:
        with Cluster(1, rules=RULES) as cluster, BackendThread(lambda: Echo(cluster.endpoints)):
            cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
            client = Client(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
            try:
                for timeout in FOREVER:
                    with self.subTest(timeout=timeout):
                        reply = client.query(b"sync", types.PYTHON_TEST_REQUEST, timeout=timeout)
                        self.assertEqual(b"sync", reply.message)
                        client.send_message(
                            b"to itself",
                            type=types.PYTHON_TEST_RESPONSE,
                            to=client.instance_id,
                            flush=True,
                            timeout=timeout,
                        )
                        received, _ = client.receive_message(timeout=timeout)
                        self.assertEqual(b"to itself", received.message)
            finally:
                client.shutdown()

    def test_a_threaded_client_waits_as_long_as_it_takes(self) -> None:
        with Cluster(1, rules=RULES) as cluster, BackendThread(lambda: Echo(cluster.endpoints)):
            cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
            for timeout in FOREVER:
                with self.subTest(timeout=timeout):
                    with ThreadedClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT, timeout=timeout) as client:
                        reply = client.query(b"threaded", types.PYTHON_TEST_REQUEST, timeout=timeout)
                        self.assertEqual(b"threaded", reply.message)
                        client.send_message(b"flushed", type=types.PYTHON_TEST_REQUEST, flush=True, timeout=timeout)
                        self.assertTrue(client.flush_all(timeout=timeout))

    def test_an_async_client_waits_as_long_as_it_takes(self) -> None:
        with Cluster(1, rules=RULES) as cluster, BackendThread(lambda: Echo(cluster.endpoints)):
            cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)

            async def ask(timeout: float) -> None:
                """Connect, query, send flushing and flush_all with `timeout` each."""
                client = AsyncClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT, timeout=timeout)
                try:
                    reply = await client.query(b"async", types.PYTHON_TEST_REQUEST, timeout=timeout)
                    self.assertEqual(b"async", reply.message)
                    await client.send_message(b"flushed", type=types.PYTHON_TEST_REQUEST, flush=True, timeout=timeout)
                    self.assertTrue(await client.flush_all(timeout=timeout))
                finally:
                    client.close()

            for timeout in FOREVER:
                with self.subTest(timeout=timeout):
                    asyncio.run(ask(timeout))

    def test_a_python_threaded_server_polls_as_long_as_it_takes(self) -> None:
        with Cluster(1, rules=RULES) as cluster:
            served = BackendThread(lambda: ThreadedEcho(cluster.endpoints), poll=math.inf).start()
            cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
            with ThreadedClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT) as client:
                self.assertEqual(b"served", client.query(b"served", types.PYTHON_TEST_REQUEST).message)
            served.stop()  # raises what serve_forever() raised

    def test_a_nan_timeout_is_no_time_at_all(self) -> None:
        with Cluster(1, rules=RULES) as cluster:
            raised: list[BaseException] = []

            def receive() -> None:
                """On a thread of its own, the client's: a receive with nothing to receive and a NaN timeout."""
                client = Client(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
                try:
                    client.receive_message(timeout=math.nan)
                except BaseException as error:
                    raised.append(error)
                finally:
                    client.shutdown()

            reader = threading.Thread(target=receive, daemon=True)  # daemon: the old code never returns
            reader.start()
            reader.join(20)  # a bound on the wait that used to never end
            self.assertFalse(reader.is_alive(), "a NaN timeout waited forever")
            self.assertIsInstance(raised[0], OperationTimedOut)


if __name__ == "__main__":
    unittest.main()
