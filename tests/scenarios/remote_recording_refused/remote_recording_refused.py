"""every way a recording request is refused: the feature off, a bad label, a second session, and a stop with nothing to stop.

A multiplexer started without `--recording-dir` and `--allow-tap` does not
even accept a recording controller's connection. With only one of the two,
the other action is refused with a status that says which option is
missing. A label that could be a path is refused. Starting while a session
is open is refused; stopping or untapping with nothing open is not an
error.
"""

import os
import unittest

from multiplexer import recording
from multiplexer.Recording_pb2 import RecordingControl
from multiplexer.clients import Client
from multiplexer.mxclient import NotConnected
from tests import harness
from tests.harness import Cluster, Mx, mxcontrol, output_dir


class RemoteRecordingRefused(unittest.TestCase):
    """Refusals, one per cause."""

    def test_without_the_options_a_controller_is_not_even_accepted(self):
        with Cluster(1) as cluster:
            result = mxcontrol("recording", "status", "-M", cluster.addresses[0], expect=1)
            self.assertIn("no multiplexer reachable", result.stderr)
            controller = Client(cluster.endpoints, type=recording.RECORDING_CONTROLLER)
            try:
                with self.assertRaises(NotConnected):
                    recording.status(controller)
            finally:
                controller.shutdown()

    def test_each_option_allows_only_its_own_action(self):
        rules = harness.default_rules()
        tap_only = Mx(7, rules, allow_tap=True).start()
        files_only = Mx(8, rules, recording_dir=os.path.join(output_dir(), "files_only")).start()
        os.makedirs(files_only.recording_dir, exist_ok=True)
        try:
            controller = Client([tap_only.endpoint], type=recording.RECORDING_CONTROLLER)
            (refused,) = recording.start(controller, "session")
            self.assertIn("--recording-dir", refused.error)
            self.assertFalse(refused.recording)
            (tapped,) = recording.control(controller, RecordingControl.TAP)
            self.assertFalse(tapped.HasField("error"))
            self.assertTrue(tapped.tapping)
            controller.shutdown()

            controller = Client([files_only.endpoint], type=recording.RECORDING_CONTROLLER)
            (refused,) = recording.control(controller, RecordingControl.TAP)
            self.assertIn("--allow-tap", refused.error)
            self.assertFalse(refused.tapping)
            (started,) = recording.start(controller, "session")
            self.assertFalse(started.HasField("error"), started)
            self.assertTrue(started.recording)
            recording.stop(controller)
            controller.shutdown()
        finally:
            tap_only.stop()
            files_only.stop()

    def test_labels_sessions_and_idle_stops(self):
        with Cluster(1, remote_recording=True) as cluster:
            controller = Client(cluster.endpoints, type=recording.RECORDING_CONTROLLER)
            try:
                for label in ["", "../escape", "a/b", "with space", "x" * 65, "dot.rec"]:
                    (status,) = recording.start(controller, label)
                    self.assertIn("label", status.error, repr(label))
                    self.assertFalse(status.recording)
                self.assertEqual([], cluster.recording_files(), "nothing was opened")

                (first,) = recording.start(controller, "x" * 64)
                self.assertFalse(first.HasField("error"))
                (second,) = recording.start(controller, "another")
                self.assertEqual("already recording " + first.path, second.error)
                self.assertTrue(second.recording, "the open session is untouched")
                self.assertEqual(first.path, second.path)

                (stopped,) = recording.stop(controller)
                self.assertFalse(stopped.recording)
                (again,) = recording.stop(controller)
                self.assertFalse(again.HasField("error"), "stopping when idle is not an error")
                self.assertEqual(first.path, again.path, "the last session stays in the status")
                (untapped,) = recording.control(controller, RecordingControl.UNTAP)
                self.assertFalse(untapped.HasField("error"))
                self.assertEqual([first.path], cluster.recording_files())
            finally:
                controller.shutdown()


if __name__ == "__main__":
    harness.main()
