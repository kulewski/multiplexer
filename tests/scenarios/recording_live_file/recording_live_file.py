"""an open recording session's file holds every record within about a second, while the session goes on.

The multiplexer writes the records through a buffer, written out when it
fills and once a second while the session is open. Three queries pass
through a session, far less than the buffer holds; without the session
closing, the file comes to hold every byte and every record the status
counts, so a reader can follow it live and a multiplexer that dies loses
at most about the last second's records. The buffer used to be written
out only when full or when the session closed.
"""

import os
import unittest

from multiplexer import recording
from multiplexer.Recording_pb2 import Record, RecordingStatus
from multiplexer.clients import SyncClient
from tests import harness
from tests.harness import Cluster, constants as C, spawn, wait_until

# How long the file may take to hold every record: a second, the flush
# interval, and the rest for a loaded machine.
CATCH_UP_SECONDS = 60
# The stream's buffer is written out when full, at 8 KiB in libstdc++; the
# session stays well below, so that only the flush can write the file.
BELOW_THE_BUFFER = 4096


class RecordingLiveFile(unittest.TestCase):
    """The file of a session that is still open."""

    def test_the_file_holds_every_record_while_the_session_is_open(self):
        cfg = harness.CONFIG
        with Cluster(1, remote_recording=True) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
            )
            backend.wait_for("connected")
            controller = SyncClient(cluster.endpoints, type=recording.RECORDING_CONTROLLER)
            try:
                (started,) = recording.start(controller, "live")
                self.assertTrue(started.recording, started)
                client = spawn(
                    "client",
                    cfg.lang("client"),
                    mx=cluster.addresses,
                    type=C.peers.TEST_CLIENT,
                    query=[(C.types.TEST_REQUEST_A, "live {round}")],
                    count=3,
                )
                self.assertEqual(0, client.wait())
                request_ids = [event["references"] for event in client.events_of("response")]
                self.assertEqual(3, len(request_ids))

                def caught_up() -> tuple[RecordingStatus, list[Record]] | None:
                    """The status and the file's records once the file holds every byte and record the status
                    counts; None before."""
                    (status,) = recording.status(controller)
                    if os.path.getsize(started.path) != status.bytes:
                        return None
                    records = list(recording.read(started.path, constants=C))
                    return (status, records) if len(records) == status.records else None

                status, records = wait_until(
                    caught_up, CATCH_UP_SECONDS, "the open session's file to hold every record"
                )
                self.assertTrue(status.recording, "the session is still open")
                self.assertLess(status.bytes, BELOW_THE_BUFFER, "the buffer never filled: the flush wrote the file")
                self.assertTrue(records[0].HasField("header"))
                routed = [record.routed for record in records if record.HasField("routed")]
                for request_id in request_ids:
                    self.assertIn(request_id, [message.id for message in routed], "every request")
                    self.assertIn(request_id, [message.references for message in routed], "and its reply")

                (stopped,) = recording.stop(controller)
                self.assertFalse(stopped.recording)
                self.assertEqual(os.path.getsize(started.path), stopped.bytes, "closed, the file has it all")
            finally:
                controller.shutdown()


if __name__ == "__main__":
    harness.main()
