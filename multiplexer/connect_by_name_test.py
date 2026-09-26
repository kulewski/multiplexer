"""The Python clients connect by host name: the name goes to the library,
which resolves it on every attempt and tries each address it has."""

import time
import unittest

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, FakePeer, TestClient
from multiplexer.threaded_client import ThreadedClient
from multiplexer.testing import runfile

RULES = runfile("multiplexer.rules")  # the file the constants were generated from


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

    def test_a_reply_through_a_dead_connection_leaves_one_connection_per_multiplexer(self):
        """A plain backend given a host name replies through the connection
        its request came on. With that connection dead, the reply waits for
        the connection the client makes again to the same name, rather than
        opening one of its own to the old address, which the multiplexer
        would take for the same peer: the two would replace each other
        every 3 s for good."""
        with Cluster(1, rules=RULES) as cluster:
            mx = cluster.mx[0]
            port = cluster.endpoints[0][1]
            with BackendThread(lambda: SlowBackend([("localhost", port)])) as served:
                assert served.backend is not None
                sender = TestClient(cluster, peers.WEBSITE)
                try:
                    sender.send(b"slow", types.PYTHON_TEST_REQUEST)
                    time.sleep(0.4)  # the backend is handling it
                    mx.kill()
                    time.sleep(0.4)
                    mx.start()
                    time.sleep(8)  # the reply went out after the reconnect; two more reconnect periods pass
                    registered = "registered connection id=%d " % served.backend.conn.instance_id
                    with open(mx.log_path, "rb") as log:
                        since_restart = log.read().decode(errors="replace").rsplit("starting MX server", 1)[-1]
                    self.assertEqual(1, since_restart.count(registered), "one connection, registered once")
                finally:
                    sender.shutdown()


class SlowBackend(BaseMultiplexerServer):
    """Answers a request after a second and a half: long enough for its
    multiplexer to be killed and started again under it."""

    def __init__(self, addresses):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)

    def handle_message(self, mxmsg):
        """Sleep, then reply the default way, through the request's connection."""
        time.sleep(1.5)
        self.send_message(message=mxmsg.message.upper(), type=types.PYTHON_TEST_RESPONSE)


if __name__ == "__main__":
    unittest.main()
