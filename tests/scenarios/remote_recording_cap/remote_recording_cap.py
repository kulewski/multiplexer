"""a remote session closes itself at its byte cap or its time cap, and a session started with --record can be stopped remotely.

A session started over the protocol has a cap: `max_bytes`, by default one
gibibyte, or `max_seconds`. Reaching either closes the file and the status
says so, so a forgotten session cannot fill a volume. A multiplexer started
with `--record` has a session too, which a controller may stop.
"""

import os
import unittest

from multiplexer import recording
from multiplexer.clients import Client
from tests import harness
from tests.harness import Cluster, constants as C, spawn, wait_until


class RemoteRecordingCap(unittest.TestCase):
    """Sessions that end on their own."""

    def test_byte_cap_closes_the_session(self):
        cfg = harness.CONFIG
        with Cluster(1, remote_recording=True) as cluster:
            controller = Client(cluster.endpoints, type=recording.RECORDING_CONTROLLER)
            try:
                (started,) = recording.start(controller, "capped", max_bytes=3000)
                self.assertTrue(started.recording)
                sender = spawn(
                    "event_client",
                    cfg.lang("event_client"),
                    mx=cluster.addresses,
                    type=C.peers.TEST_EVENT_CLIENT,
                    send=[(C.types.TEST_EVENT, "x" * 100)] * 60,
                )
                self.assertEqual(0, sender.wait())
                # The events went in on the sender's connection, the status
                # request on the controller's: the multiplexer may answer
                # the latter before the last event crossed the cap.
                status = wait_until(
                    lambda: next((s for s in recording.status(controller) if not s.recording), None),
                    10,
                    "the cap to close the session",
                )
                self.assertEqual("max_bytes reached", status.stopped)
                self.assertEqual(started.path, status.path)
                self.assertGreaterEqual(status.bytes, 3000)
                self.assertLess(status.bytes, 3000 + 400, "closed right after the record that crossed the cap")
                self.assertEqual(os.path.getsize(status.path), status.bytes)
                records = list(recording.read(status.path, constants=C))
                self.assertEqual(status.records, len(records))
                self.assertTrue(records[0].HasField("header"))
                (again,) = recording.start(controller, "capped", max_bytes=0)
                self.assertFalse(again.HasField("error"), "a session may follow a capped one")
                self.assertNotEqual(started.path, again.path)
                recording.stop(controller)
            finally:
                controller.shutdown()

    def test_time_cap_closes_the_session(self):
        with Cluster(1, remote_recording=True) as cluster:
            controller = Client(cluster.endpoints, type=recording.RECORDING_CONTROLLER)
            try:
                (started,) = recording.start(controller, "timed", max_seconds=1)
                self.assertTrue(started.recording)
                status = wait_until(
                    lambda: next((s for s in recording.status(controller) if not s.recording), None),
                    5,
                    "the session to close by itself",
                )
                self.assertEqual("max_seconds reached", status.stopped)
                self.assertEqual(started.path, status.path)
                self.assertTrue(next(recording.read(status.path, constants=C)).HasField("header"))
            finally:
                controller.shutdown()

    def test_a_session_from_the_command_line_can_be_stopped_remotely(self):
        with Cluster(1, record=True, remote_recording=True) as cluster:
            controller = Client(cluster.endpoints, type=recording.RECORDING_CONTROLLER)
            try:
                (status,) = recording.status(controller)
                self.assertTrue(status.recording)
                self.assertEqual(cluster.mx[0].record_file, status.path)
                self.assertFalse(status.HasField("label"), "--record has no label")
                (stopped,) = recording.stop(controller)
                self.assertFalse(stopped.recording)
                self.assertEqual("stopped by peer %d" % controller.instance_id, stopped.stopped)
                (started,) = recording.start(controller, "after")
                self.assertTrue(started.recording, "the directory takes a new session")
            finally:
                controller.shutdown()


if __name__ == "__main__":
    harness.main()
