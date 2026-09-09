"""a session kept by `mxcontrol recording start --stay` follows a replica through a restart and ends on SIGINT.

Recording state lives in the multiplexer process, so a replica replaced
mid-session comes back not recording. With `--stay` the command keeps its
connections, asks every multiplexer for its status every couple of seconds,
starts a session on any that has never had one, and stops every session
when it is interrupted.
"""

import os
import subprocess
import unittest

from multiplexer import recording
from multiplexer.clients import Client
from tests import harness
from tests.harness import Cluster, child_env, mx_runfile, output_dir, wait_until


def statuses(cluster: Cluster) -> dict[int, recording.RecordingStatus]:
    """The status of every multiplexer, by instance id, through a fresh controller."""
    controller = Client(cluster.endpoints, type=recording.RECORDING_CONTROLLER)
    try:
        return {status.multiplexer_id: status for status in recording.status(controller)}
    finally:
        controller.shutdown()


def all_recording(cluster: Cluster, except_ids=()) -> dict[int, recording.RecordingStatus] | None:
    """The statuses once both multiplexers record and none of `except_ids` is among them."""
    found = statuses(cluster)
    if len(found) == 2 and all(status.recording for status in found.values()) and not set(found) & set(except_ids):
        return found
    return None


class RemoteRecordingStay(unittest.TestCase):
    """One --stay command over a restart of one of two multiplexers."""

    def test_stay_restarts_the_session_on_a_replaced_replica(self):
        with Cluster(2, remote_recording=True) as cluster:
            log = open(os.path.join(output_dir(), "stay.log"), "w")
            stay = subprocess.Popen(
                [mx_runfile("mxcontrol/mxcontrol"), "recording", "start", "--stay", "--label", "kept"]
                + [arg for address in cluster.addresses for arg in ("-M", address)],
                stdout=log,
                stderr=subprocess.STDOUT,
                env=child_env(native=True),
            )
            try:
                before = wait_until(lambda: all_recording(cluster), 10, "both multiplexers recording")
                self.assertEqual({"kept"}, {status.label for status in before.values()})
                cluster.mx[0].restart()
                after = wait_until(
                    lambda: all_recording(cluster) if set(statuses(cluster)) != set(before) else None,
                    20,
                    "the replaced multiplexer to record again",
                )
                replaced = [multiplexer_id for multiplexer_id in after if multiplexer_id not in before]
                self.assertEqual(1, len(replaced), "one new instance id")
                kept = [multiplexer_id for multiplexer_id in after if multiplexer_id in before]
                self.assertEqual(before[kept[0]].path, after[kept[0]].path, "the surviving replica's session went on")
                self.assertEqual("kept", after[replaced[0]].label)
                self.assertEqual(
                    3, len(cluster.recording_files()), "the replaced replica's old and new file, and the other's"
                )

                stay.send_signal(2)  # SIGINT: stop every session and leave
                self.assertEqual(0, stay.wait(15))
                final = statuses(cluster)
                self.assertFalse(any(status.recording for status in final.values()))
                self.assertTrue(all(status.stopped.startswith("stopped by peer") for status in final.values()), final)
                log.close()
                with open(log.name) as output:
                    text = output.read()
                self.assertIn("no session yet; starting one", text)
                self.assertEqual(2, text.count("not recording; last"), text)
            finally:
                if stay.poll() is None:
                    stay.kill()


if __name__ == "__main__":
    harness.main()
