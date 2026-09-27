"""The synchronous client's waits let the program's other threads run:
flush_all() and a flushing send, waiting for room or for a connection,
and a connect waiting for the multiplexer's welcome, release the GIL
while the loop runs, where they held it for their whole timeout.
Counted: how far a thread that counts every millisecond gets while each
of them waits half a second.
"""

import threading
import time
import unittest
from typing import Callable

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import NotConnected, OperationTimedOut
from multiplexer.testing import Cluster, FakePeer
from multiplexer.testing import runfile
from multiplexer.testing.buffers import past_the_queue

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST
CHUNK = b"x" * (16 * 1024)
FILLER_TIMEOUT = (
    120  # seconds a message filling a connection may wait for room: none is given up on, however slow the fill
)


def counted_during(wait: Callable[[], object]) -> int:
    """How many times a thread that counts every millisecond, with the GIL,
    counted while `wait()` ran: none while the wait holds the GIL, a few
    at most around it."""
    count = 0
    stop = threading.Event()

    def counting() -> None:
        nonlocal count
        while not stop.is_set():
            count += 1
            time.sleep(0.001)

    thread = threading.Thread(target=counting)
    thread.start()
    try:
        time.sleep(0.05)  # counting
        before = count
        try:
            wait()
        except (NotConnected, OperationTimedOut):
            pass
        return count - before
    finally:
        stop.set()
        thread.join()


class SyncWaitsTest(unittest.TestCase):
    """Each wait half a second, the other thread counting meanwhile."""

    def test_flushes_wait_without_the_gil(self) -> None:
        """flush_all() and a flushing send, against a multiplexer frozen
        with the connection full: each times out after half a second, the
        other thread having counted meanwhile."""
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER):
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                cluster.mx[0].pause()
                try:
                    for payload in past_the_queue(CHUNK):
                        client.send_message(payload, type=EVENT, timeout=FILLER_TIMEOUT)
                    client.send_message(b"waits", type=EVENT, timeout=FILLER_TIMEOUT)
                    self.assertGreater(counted_during(lambda: client.flush_all(0.5)), 20, "flush_all")
                    self.assertGreater(
                        counted_during(lambda: client.send_message(b"last", type=EVENT, flush=True, timeout=0.5)),
                        20,
                        "a flushing send",
                    )
                finally:
                    cluster.mx[0].resume()
            finally:
                client.shutdown()

    def test_a_send_waiting_for_a_connection_waits_without_the_gil(self) -> None:
        """A flushing send through a connection that is gone, with no other
        live, waits for one to come up: half a second, the other thread
        counting meanwhile."""
        with Cluster(1, rules=RULES) as cluster:
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                lane = client.lane()
                client.send_message(b"first", type=EVENT, multiplexer=lane, flush=True)
                connection = lane.connection
                cluster.mx[0].stop()
                self.assertGreater(
                    counted_during(
                        lambda: client.send_message(
                            b"second", type=EVENT, multiplexer=connection, flush=True, timeout=0.5
                        )
                    ),
                    20,
                )
            finally:
                client.shutdown()

    def test_a_connect_waits_without_the_gil(self) -> None:
        """Connecting to a multiplexer frozen with its port open, which
        never sends its welcome, and waiting for that connection: half a
        second each, the other thread counting meanwhile."""
        with Cluster(1, rules=RULES) as cluster:
            host, port = cluster.endpoints[0]
            client = Client([], type=peers.WEBSITE)
            try:
                cluster.mx[0].pause()
                try:
                    self.assertGreater(counted_during(lambda: client.connect_to(host, port, 0.5)), 20, "connect_to")
                    connection = client.async_connect_to(host, port)
                    self.assertGreater(
                        counted_during(lambda: client.wait_for_connection(connection, 0.5)), 20, "wait_for_connection"
                    )
                finally:
                    cluster.mx[0].resume()
            finally:
                client.shutdown()


if __name__ == "__main__":
    unittest.main()
