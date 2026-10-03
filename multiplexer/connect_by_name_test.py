"""The Python clients connect by host name: the name goes to the library,
which resolves it on every attempt and tries each address it has, for the
synchronous client, ThreadedClient and AsyncClient alike; and a backend
given a name replies through the connection it made again to that name,
never through one of its own to the old address, which the multiplexer
took for the same peer, the two replacing each other every 3 s."""

import asyncio
import re
import threading
import unittest

from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, FakePeer, Mx, TestClient
from multiplexer.threaded_client import ThreadedClient
from multiplexer.testing import runfile

RULES = runfile("tests/testing.rules")  # the file the constants were generated from


class ConnectByName(unittest.TestCase):
    def test_localhost_on_both_clients(self):
        with Cluster(1, rules=RULES) as cluster:
            port = cluster.endpoints[0][1]
            with FakePeer(cluster, peers.PYTHON_TEST_SERVER) as backend:
                backend.reply_with(types.PYTHON_TEST_REQUEST, b"by name", types.PYTHON_TEST_RESPONSE)
                threaded = ThreadedClient([("localhost", port)], type=peers.PYTHON_TEST_CLIENT)
                try:
                    self.assertEqual(1, threaded.connections_count())
                    self.assertEqual(b"by name", threaded.query(b"hello", types.PYTHON_TEST_REQUEST, timeout=5).message)
                finally:
                    threaded.shutdown()
                client = Client([("localhost", port)], type=peers.WEBSITE)
                try:
                    self.assertEqual(b"by name", client.query(b"hello", types.PYTHON_TEST_REQUEST, timeout=5).message)
                finally:
                    client.shutdown()

    def test_a_name_that_does_not_resolve_is_retried_not_raised(self):
        """The threaded clients hand the name to the library, as the
        synchronous one does: a name that does not resolve yet leaves the
        client unconnected and retrying, like a port that refuses, instead
        of raising out of the constructor."""
        threaded = ThreadedClient([("mx-not-published-yet.invalid", 1980)], type=peers.PYTHON_TEST_CLIENT)
        try:
            self.assertEqual(0, threaded.connections_count())
        finally:
            threaded.shutdown()

        async def construct() -> int:
            client = AsyncClient([("mx-not-published-yet.invalid", 1980)], type=peers.PYTHON_TEST_CLIENT)
            try:
                return client.connections_count()
            finally:
                await client.aclose()

        self.assertEqual(0, asyncio.run(construct()))

    def test_a_reply_through_a_dead_connection_leaves_one_connection_per_multiplexer(self):
        """A BaseMultiplexerServer given a host name replies through the
        connection its request came on. With that connection dead, the
        reply goes through the connection the client made again to the
        same name, rather than through one of its own to the old address,
        which the multiplexer took for the same peer: it registered the
        backend twice at once, and the two went on replacing each other
        every 3 s for good. The handler, its multiplexer killed and started
        again under it, waits for its client to be back by name before it
        replies (HeldBackend), so that a connection of the reply's own,
        had it made one, is registered by the time the reply arrives.
        Counted, not timed: the multiplexer's log has its registrations of
        the backend."""
        with Cluster(1, rules=RULES) as cluster:
            multiplexer = cluster.mx[0]
            address = cluster.endpoints[0]
            with (
                BackendThread(lambda: HeldBackend([("localhost", address[1])])) as served,
                TestClient(cluster, peers.WEBSITE) as requester,
            ):
                backend = served.backend
                assert backend is not None
                try:
                    request = requester.send(b"hello", types.PYTHON_TEST_REQUEST)
                    self.assertTrue(backend.holding.wait(30), "the backend took up the request")
                    since = multiplexer.log_mark()
                    multiplexer.kill()
                    multiplexer.start()
                    requester.client.disconnect(address)  # back at once, where its reconnect comes 3 s on
                    requester.client.connect(address)
                    cluster.wait_for_peer(peers.WEBSITE)
                finally:
                    backend.release.set()
                self.assertEqual(b"HELLO", reply_to(requester.client, request).message)
                self.assertEqual(
                    1,
                    registrations(multiplexer, backend.conn.instance_id, since),
                    "the backend's connections the multiplexer registered since its restart",
                )


class HeldBackend(BaseMultiplexerServer):
    """Holds a request until `release` is set; then sends an event and
    waits for its write, which, the multiplexer having gone under it,
    waits for the client to come back by name; then replies the default
    way, through the connection the request came on."""

    def __init__(self, addresses):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)
        self.holding = threading.Event()  # the request is being handled
        self.release = threading.Event()  # the test's: go on

    def handle_message(self, mxmsg):
        """Hold, 60 s at most, a failure detector's bound; wait for a
        connection, 30 s at most; reply."""
        self.holding.set()
        self.release.wait(60)
        self.conn.send_message(b"back", type=types.TEST_UNROUTED, flush=True, timeout=30, report_delivery_error=False)
        self.send_message(message=mxmsg.message.upper(), type=types.PYTHON_TEST_RESPONSE)


def reply_to(client: Client, message_id: int, timeout: float = 30.0):
    """The next message `client` reads that references `message_id`;
    OperationTimedOut after `timeout` seconds of nothing, a failure
    detector's bound."""
    while True:
        mxmsg = client.read_message(timeout=timeout)
        if mxmsg.references == message_id:
            return mxmsg


def registrations(multiplexer: Mx, instance_id: int, since: int) -> int:
    """How many connections of the peer `instance_id` the multiplexer's log
    says it registered past `since`, a log_mark(); an "unregistered" line
    is not one."""
    with open(multiplexer.log_path, "rb") as log:
        log.seek(since)
        text = log.read().decode(errors="replace")
    return len(re.findall(r"\bregistered connection id=%d\b" % instance_id, text))


if __name__ == "__main__":
    unittest.main()
