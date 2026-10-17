"""The reason BaseThreadedMultiplexerServer exists: a request that takes
longer than the multiplexer's drop interval. With the intervals as shipped
(a heartbeat every 3 s, the drop started after 30 s of silence and
completed at 90 s), a BaseMultiplexerServer blocked in its handler is
dropped; a BaseThreadedMultiplexerServer keeps heartbeating from its io
thread, stays registered, and its reply arrives. About two minutes; tagged slow.
A backend that was dropped reconnects 3 s later on its own, and its reply
then goes through the new connection, which the peers file lists, so
neither tells that it stayed: the multiplexer's log, which registered it
once, where a drop and the reconnect make that twice, does.
"""

import re
import time
import unittest

from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import BackendThread, Cluster, Mx, wait_until
from multiplexer.threaded_client import ThreadedClient
from multiplexer.threaded_server import BaseThreadedMultiplexerServer, Request
from multiplexer.testing import runfile

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

HANDLER_SECONDS = 100


class Slow(BaseThreadedMultiplexerServer):
    """Takes HANDLER_SECONDS over every request."""

    multiplexer_client_type = peers.PYTHON_TEST_SERVER

    def handle_message(self, request: Request) -> None:
        time.sleep(HANDLER_SECONDS)
        request.reply(request.mxmsg.message.upper(), type=types.PYTHON_TEST_RESPONSE)


def registrations(multiplexer: Mx, instance_id: int, since: int) -> int:
    """How many connections of the peer `instance_id` the multiplexer's log
    says it registered past `since`, a log_mark(); an "unregistered" line
    is not one."""
    with open(multiplexer.log_path, "rb") as log:
        log.seek(since)
        text = log.read().decode(errors="replace")
    return len(re.findall(r"\bregistered connection id=%d\b" % instance_id, text))


class LongRequestTest(unittest.TestCase):
    def test_the_backend_stays_registered_under_a_long_request(self):
        with Cluster(1, rules=RULES) as cluster:
            multiplexer = cluster.mx[0]
            since = multiplexer.log_mark()  # before the backend's first connection
            with BackendThread(lambda: Slow(cluster.endpoints)) as served:
                cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
                backend = served.backend
                assert backend is not None
                client = ThreadedClient(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
                try:
                    started = time.monotonic()
                    reply = client.query(b"patience", types.PYTHON_TEST_REQUEST, timeout=HANDLER_SECONDS + 30)
                    self.assertEqual(b"PATIENCE", reply.message)
                    self.assertGreaterEqual(time.monotonic() - started, HANDLER_SECONDS)
                    self.assertTrue(
                        any(number == peers.PYTHON_TEST_SERVER for _, _, number in multiplexer.connected_peers()),
                        "the backend was dropped while it worked",
                    )
                    self.assertEqual(
                        1,
                        registrations(multiplexer, backend.instance_id, since),
                        "the backend's connections the multiplexer registered: a second is a drop and a reconnect",
                    )
                    # The worker counts the request done once its handler
                    # returned, which the reply it sent from there can beat.
                    wait_until(lambda: backend.pending == 0, 10, "the request done")
                finally:
                    client.shutdown()


if __name__ == "__main__":
    unittest.main()
