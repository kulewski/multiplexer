"""A client's on_drop runs in one pass per loop turn, after the handlers and
the bookkeeping the drop came from, the sends' callbacks after it, so it
may call back into the client: send, or shut the client down. A SyncClient
whose on_drop sent while a dead connection handed its queue over polled
the loop from inside that handover, tore the same connection down a second
time and freed the queue being gone through: the process crashed. One
whose on_drop shut the client down let go of the observer that was
running. Counted, not timed: every drop is told once, the client goes on
after, and a flush ends once the drops it saw were told.
"""

import sys
import unittest

from multiplexer.clients import SyncClient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import DropReason, NotConnected
from multiplexer.testing import Cluster, runfile
from multiplexer.testing.buffers import fill_frames

RULES = runfile("tests/testing.rules")
EVENT = types.TEST_EVENT  # whom: ALL to TEST_EVENT_BACKEND
BOUND = 30.0  # what only a failure reaches


class DropObserverTest(unittest.TestCase):
    """See the module docstring."""

    def test_an_observer_that_sends_while_a_dead_connection_hands_its_queue_over(self) -> None:
        """Two multiplexers; A is frozen while the client sends to ALL, so
        that A's copies fill its sockets and a write to A is under way, and
        then killed, which resets the connection. The next call runs A's
        failed write, whose handover gives A's copies up, and its read of
        the reset; on_drop sends for each copy given up on."""
        drops: list[tuple[int, DropReason]] = []
        client: SyncClient | None = None

        def on_drop(message_id: int, reason: DropReason) -> None:
            drops.append((message_id, reason))
            assert client is not None
            client.send_message(b"note", type=EVENT)  # polls the loop first

        with Cluster(2, rules=RULES) as cluster:
            a, b = cluster.mx
            client = SyncClient([a.endpoint, b.endpoint], type=peers.TEST_EVENT_CLIENT, on_drop=on_drop)
            try:
                a.pause()
                sent = [
                    client.send_message(payload, type=EVENT, multiplexer=SyncClient.ALL) for payload in fill_frames()
                ]
                a.kill()
                client.send_message(b"trigger", type=EVENT)  # runs the loop: A's failed write and its handover
                self.assertTrue(client.flush_all(timeout=BOUND), "what went to B is written")
                ids = [message_id for message_id, _ in drops]
                self.assertTrue(drops, "A's copies given up on")
                self.assertEqual(len(ids), len(set(ids)), "each told once")
                self.assertTrue(set(ids) <= set(sent))
                self.assertEqual({DropReason.CONNECTION_LOST}, {reason for _, reason in drops})
                self.assertEqual(len(drops), client.dropped)
                self.assertEqual(1, client.connections_count(), "B's connection goes on")
            finally:
                client.shutdown()

    def test_an_observer_may_shut_the_client_down(self) -> None:
        """A message sent with no time to wait and nothing connected is
        dropped; on_drop shuts the client down and then raises, which goes
        to sys.unraisablehook. The next call's loop runs it; the call then
        finds the client shut down."""
        told: list[DropReason] = []
        unraised: list[BaseException | None] = []
        client = SyncClient([], type=peers.TEST_EVENT_CLIENT)

        def on_drop(message_id: int, reason: DropReason) -> None:
            told.append(reason)
            client.shutdown()
            raise RuntimeError("raised after the shutdown")

        client = SyncClient([], type=peers.TEST_EVENT_CLIENT, on_drop=on_drop)
        hook = sys.unraisablehook
        sys.unraisablehook = lambda unraisable: unraised.append(unraisable.exc_value)
        try:
            client.send_message(b"dropped", type=EVENT, timeout=0)
            self.assertEqual(1, client.dropped)
            with self.assertRaises(NotConnected):
                client.send_message(b"after", type=EVENT, timeout=0)  # its loop runs on_drop first
        finally:
            sys.unraisablehook = hook
            client.shutdown()
        self.assertEqual([DropReason.NO_CONNECTION], told)
        self.assertEqual(1, len(unraised))
        self.assertIsInstance(unraised[0], RuntimeError)

    def test_a_flush_ends_after_the_drops_it_saw_were_told(self) -> None:
        """What a send gave up on at once is told to on_drop by the time a
        flush_all() made after it returns, a flush with nothing to wait
        for included."""
        told: list[DropReason] = []
        client = SyncClient([], type=peers.TEST_EVENT_CLIENT, on_drop=lambda message_id, reason: told.append(reason))
        try:
            client.send_message(b"dropped", type=EVENT, timeout=0)
            client.flush_all(timeout=0)
            self.assertEqual([DropReason.NO_CONNECTION], told)
        finally:
            client.shutdown()


if __name__ == "__main__":
    unittest.main()
