"""recording.tap() raises TapFailed when a multiplexer refuses the TAP,
its taps being off, or does not answer in time, after ending the
subscriptions it made, where it returned an iterator that only ever timed
out, a quiet cluster to the caller. And the records a tapping client reads
while it waits for a status, of a status() on it say, are yielded by its
tap() iterator, where they were thrown away uncounted. Counted, not
timed: the messages whose records the client reads are its own, routed
before its STATUS on the same connection, so their records come ahead of
the status reply; a frozen multiplexer never answers.
"""

import unittest

from multiplexer import recording
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import types
from multiplexer.recording import RECORDING_CONTROLLER, TapFailed
from multiplexer.testing import Cluster, runfile

RULES = runfile("tests/testing.rules")  # the file the constants were generated from


class RecordingTapTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_tap_a_multiplexer_refuses_raises_why(self) -> None:
        """Remote recording on, taps off: TapFailed with the reason."""
        cluster = Cluster(1, rules=RULES, remote_recording=True)
        cluster.mx[0].allow_tap = False
        with cluster:
            client = Client(cluster.endpoints, type=RECORDING_CONTROLLER)
            try:
                with self.assertRaises(TapFailed) as raised:
                    recording.tap(client, timeout=10)
                self.assertIn("taps are off", str(raised.exception))
                self.assertEqual(1, len(raised.exception.statuses))
            finally:
                client.shutdown()

    def test_a_tap_a_multiplexer_does_not_answer_raises_and_is_ended(self) -> None:
        """One of two multiplexers frozen: TapFailed naming the one that did
        not answer, and once it runs again neither has the tap."""
        with Cluster(2, rules=RULES, remote_recording=True) as cluster:
            client = Client(cluster.endpoints, type=RECORDING_CONTROLLER)
            try:
                cluster.mx[1].pause()
                try:
                    with self.assertRaises(TapFailed) as raised:
                        recording.tap(client, timeout=1)
                    self.assertIn("1 of 2 multiplexer(s) did not answer", str(raised.exception))
                finally:
                    cluster.mx[1].resume()
                statuses = recording.status(client, timeout=30)
                self.assertEqual([0, 0], [status.taps for status in statuses])
            finally:
                client.shutdown()

    def test_records_read_while_waiting_for_a_status_reach_the_tap(self) -> None:
        """The client taps, sends three messages, and asks for its status: the
        records of the three, read while it waited for the status, come out
        of its tap iterator."""
        with Cluster(1, rules=RULES, remote_recording=True) as cluster:
            client = Client(cluster.endpoints, type=RECORDING_CONTROLLER)
            try:
                records = recording.tap(client, timeout=10)
                sent = {client.send_message(b"%d" % index, type=types.TEST_UNROUTED, flush=True) for index in range(3)}
                (status,) = recording.status(client)
                self.assertTrue(status.tapping)
                seen: set[int] = set()
                for record in records:
                    if record.HasField("routed") and record.routed.id in sent:
                        seen.add(record.routed.id)
                        if seen == sent:
                            break
            finally:
                client.shutdown()


if __name__ == "__main__":
    unittest.main()
