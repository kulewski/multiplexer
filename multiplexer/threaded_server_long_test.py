"""The reason the threaded backend exists: a request that takes longer than
the multiplexer's drop interval. With the intervals as shipped (a
heartbeat every 3 s, the drop started after 30 s of silence and completed
at 90 s), a BaseMultiplexerServer blocked in its handler is dropped; a
BaseThreadedMultiplexerServer keeps heartbeating from its io thread, stays
registered, and its reply arrives. About two minutes; tagged slow.
"""

import time
import unittest

from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import BackendThread, Cluster, wait_until
from multiplexer.threaded_client import ThreadedClient
from multiplexer.threaded_server import BaseThreadedMultiplexerServer, Request

HANDLER_SECONDS = 100


class Slow(BaseThreadedMultiplexerServer):
    """Takes HANDLER_SECONDS over every request."""

    multiplexer_client_type = peers.PYTHON_TEST_SERVER

    def handle_message(self, request: Request) -> None:
        time.sleep(HANDLER_SECONDS)
        request.reply(request.mxmsg.message.upper(), type=types.PYTHON_TEST_RESPONSE)


class LongRequestTest(unittest.TestCase):
    def test_the_backend_stays_registered_under_a_long_request(self):
        with Cluster(1) as cluster, BackendThread(lambda: Slow(cluster.endpoints)) as served:
            cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
            client = ThreadedClient(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
            try:
                started = time.monotonic()
                reply = client.query(b"patience", types.PYTHON_TEST_REQUEST, timeout=HANDLER_SECONDS + 30)
                self.assertEqual(b"PATIENCE", reply.message)
                self.assertGreaterEqual(time.monotonic() - started, HANDLER_SECONDS)
                self.assertTrue(
                    any(number == peers.PYTHON_TEST_SERVER for _, _, number in cluster.mx[0].connected_peers()),
                    "the backend was dropped while it worked",
                )
                assert served.backend is not None
                self.assertEqual(0, served.backend.pending)
            finally:
                client.shutdown()


if __name__ == "__main__":
    unittest.main()
