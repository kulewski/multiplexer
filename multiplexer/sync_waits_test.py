"""The synchronous client's waits let the program's other threads run:
flush_all() and a flushing send, waiting for room or for a connection,
and a connect waiting for the multiplexer's welcome, release the GIL
while the loop runs, where they held it for their whole timeout.
Shown, not timed: each waits for a frozen multiplexer, which another
thread resumes as soon as it runs during the wait, which ends then. With
the switch interval longer than the moment from the wait's start to its
call into the library, the waiting thread gives up the GIL only by
releasing it, so the other thread, which needs the GIL to see that the
wait began, runs during the wait only if the wait released the GIL; a
wait that held it runs out its WAIT seconds and fails. Where the other
thread counted every millisecond and had to count more than 20 in a
half-second wait: a rate, which load could hold under.
"""

import sys
import threading
import unittest
from typing import Callable, TypeVar

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, FakePeer
from multiplexer.testing import runfile
from multiplexer.testing.buffers import past_the_queue

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST
CHUNK = b"x" * (16 * 1024)
FILLER_TIMEOUT = (
    120  # seconds a message filling a connection may wait for room: none is given up on, however slow the fill
)
WAIT = 30.0  # seconds a wait may last: ended by the other thread at once, run out only by one that holds the GIL
SWITCH_INTERVAL = 10.0  # seconds a thread waits for the GIL before the interpreter asks for it: none here

Result = TypeVar("Result")


def ended_from_another_thread(wait: Callable[[], Result], end: Callable[[], object]) -> Result:
    """What `wait()` returns, `end()` called by another thread as soon as
    that thread runs after the wait began. Meanwhile the switch interval is
    SWITCH_INTERVAL, so that this thread gives up the GIL only by releasing
    it: the other thread, which takes the GIL to see that the wait began,
    then runs during the wait only if the wait released the GIL, where the
    interpreter's 5 ms could let it run before the wait's call into the
    library. A wait that held the GIL runs out its time instead, and
    returns or raises as one that did."""
    began = threading.Event()

    def ending() -> None:
        began.wait()
        end()

    previous = sys.getswitchinterval()
    sys.setswitchinterval(SWITCH_INTERVAL)
    try:
        other = threading.Thread(target=ending)
        other.start()
        try:
            began.set()
            return wait()
        finally:
            began.set()  # a wait that raised before it began
            other.join()
    finally:
        sys.setswitchinterval(previous)


class SyncWaitsTest(unittest.TestCase):
    """Each wait ended by the other thread, which resumes the multiplexer."""

    def test_flush_all_waits_without_the_gil(self) -> None:
        """flush_all(), against a multiplexer frozen with the connection
        full, returns True once the other thread resumed the multiplexer
        during the wait."""
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER):
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                cluster.mx[0].pause()
                try:
                    for payload in past_the_queue(CHUNK):
                        client.send_message(payload, type=EVENT, timeout=FILLER_TIMEOUT)
                    client.send_message(b"waits", type=EVENT, timeout=FILLER_TIMEOUT)
                    self.assertTrue(
                        ended_from_another_thread(lambda: client.flush_all(WAIT), cluster.mx[0].resume),
                        "flush_all() ran out of time, nothing having run while it waited",
                    )
                finally:
                    cluster.mx[0].resume()
            finally:
                client.shutdown()

    def test_a_flushing_send_waits_without_the_gil(self) -> None:
        """A flushing send, against a multiplexer frozen with the connection
        full, returns once the other thread resumed the multiplexer during
        the wait; OperationTimedOut had nothing run meanwhile."""
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER):
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                cluster.mx[0].pause()
                try:
                    for payload in past_the_queue(CHUNK):
                        client.send_message(payload, type=EVENT, timeout=FILLER_TIMEOUT)
                    ended_from_another_thread(
                        lambda: client.send_message(b"last", type=EVENT, flush=True, timeout=WAIT),
                        cluster.mx[0].resume,
                    )
                finally:
                    cluster.mx[0].resume()
            finally:
                client.shutdown()

    def test_a_send_waiting_for_a_connection_waits_without_the_gil(self) -> None:
        """A flushing send through a connection that is gone, with no other
        live, waits for one to come up: the client's connection to a second
        multiplexer, frozen, is on its way, and the send returns once the
        other thread resumed that multiplexer during the wait, which then
        welcomes the client; NotConnected had nothing run meanwhile."""
        with Cluster(2, rules=RULES) as cluster:
            client = Client([cluster.endpoints[0]], type=peers.WEBSITE)
            try:
                lane = client.lane()
                client.send_message(b"first", type=EVENT, multiplexer=lane, flush=True)
                connection = lane.connection
                frozen = cluster.mx[1]
                frozen.pause()
                try:
                    client.async_connect_to(*cluster.endpoints[1])  # on its way, never live while frozen
                    cluster.mx[0].stop()
                    ended_from_another_thread(
                        lambda: client.send_message(
                            b"second", type=EVENT, multiplexer=connection, flush=True, timeout=WAIT
                        ),
                        frozen.resume,
                    )
                finally:
                    frozen.resume()
            finally:
                client.shutdown()

    def test_a_connect_waits_without_the_gil(self) -> None:
        """Connecting to a multiplexer frozen with its port open, which
        sends no welcome until it runs, and, from a second client, waiting
        for such a connection: each returns connected once the other thread
        resumed the multiplexer during the wait."""
        with Cluster(1, rules=RULES) as cluster:
            host, port = cluster.endpoints[0]
            connecting = Client([], type=peers.WEBSITE)
            waiting = Client([], type=peers.WEBSITE)
            try:
                cluster.mx[0].pause()
                try:
                    self.assertTrue(
                        ended_from_another_thread(
                            lambda: connecting.connect_to(host, port, WAIT), cluster.mx[0].resume
                        ),
                        "connect_to",
                    )
                finally:
                    cluster.mx[0].resume()
                cluster.mx[0].pause()
                try:
                    connection = waiting.async_connect_to(host, port)
                    self.assertTrue(
                        ended_from_another_thread(
                            lambda: waiting.wait_for_connection(connection, WAIT), cluster.mx[0].resume
                        ),
                        "wait_for_connection",
                    )
                finally:
                    cluster.mx[0].resume()
            finally:
                connecting.shutdown()
                waiting.shutdown()


if __name__ == "__main__":
    unittest.main()
