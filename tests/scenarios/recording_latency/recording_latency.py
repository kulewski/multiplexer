"""recording costs the routed message no write of its own, so nothing a
client could wait for: the multiplexer buffers its records and writes the
file a block at a time, and once a second.

The same client runs the same queries against a multiplexer without
--record and one with it. The write syscalls each multiplexer makes over
the queries, counted in /proc/<pid>/io, differ by fewer than one per ten
records the recording holds, and one per second the queries took, the
writes of the once-a-second flush, and the recording holds every query
and its response. Counted, not timed: a write per record would add one for
each, however fast or loaded the machine; sockets are written with
sendmsg(), which the count leaves out.
"""

import math
import time
import unittest

from multiplexer import recording
from multiplexer.Recording_pb2 import RoutedMessage
from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 300
RECORDS_PER_WRITE = 10  # the fewest records each write the recording adds must carry


class RecordingLatency(unittest.TestCase):
    """Counts the multiplexer's writes with and without recording."""

    def run_queries(self, record: bool) -> tuple[int, float, list[RoutedMessage]]:
        """QUERIES queries against one multiplexer: the write syscalls it
        made while they ran, how many seconds that was, and the routed
        records of its recording, none without one."""
        cfg = harness.CONFIG
        with Cluster(1, record=record) as cluster:
            mx = cluster.mx[0]
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
            )
            backend.wait_for("connected")
            started = time.monotonic()
            before = mx.write_calls()
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                count=QUERIES,
                timeout=30,  # a stage: no request sent twice, which the recording's counts would show
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            self.assertEqual(0, client.wait())
            writes = mx.write_calls() - before
            seconds = time.monotonic() - started
            self.assertEqual(QUERIES, len(client.events_of("response")))
            self.assertEqual(0, backend.stop())
            mx.stop()
            if not record:
                return writes, seconds, []
            records = recording.read(mx.record_file, constants=C)
            return writes, seconds, [record.routed for record in records if record.HasField("routed")]

    def test_recording_adds_no_write_per_message(self):
        plain, _, _ = self.run_queries(record=False)
        recorded, seconds, routed = self.run_queries(record=True)
        delivered = [r for r in routed if r.disposition == RoutedMessage.DELIVERED]
        self.assertEqual(QUERIES, len([r for r in delivered if r.type == C.types.TEST_REQUEST_A]), "every query")
        self.assertEqual(QUERIES, len([r for r in delivered if r.type == C.types.TEST_RESPONSE]), "every response")
        # The clock as data, not as a trigger: the flush that runs once a
        # second while a session is open writes once for each second the
        # queries took, however slow the machine made them.
        flushes = math.ceil(seconds) + 1
        self.assertLess(
            recorded - plain,
            len(routed) / RECORDS_PER_WRITE + flushes,
            "write syscalls over the queries: %d plain, %d recorded, for %d records in %.1f s"
            % (plain, recorded, len(routed), seconds),
        )


if __name__ == "__main__":
    harness.main()
