"""The Python clients connect by host name: the name goes to the library,
which resolves it on every attempt and tries each address it has."""

import unittest

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, FakePeer
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


if __name__ == "__main__":
    unittest.main()
