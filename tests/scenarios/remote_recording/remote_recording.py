"""a recording session is started and stopped on two multiplexers over the protocol, one file each in a shared directory.

Two multiplexers share one recording directory, as replicas on one volume
would. `mxcontrol recording start` reaches both, and each opens a file named
after the label, the time and its own instance id, so the files never
collide; the peers connected at that moment are written first. Traffic is
routed while recording, `status` counts it, `stop` closes both files, and
read_many merges them by time into one session. The Python API does the
same from a test.
"""

import os
import re
import unittest

from multiplexer import recording
from multiplexer.Recording_pb2 import PeerEvent, RoutedMessage
from multiplexer.clients import Client
from tests import harness
from tests.harness import Cluster, constants as C, mxcontrol, spawn

STATUS_LINE = re.compile(r"multiplexer (\d+): (.*)")


def parse(output: str) -> dict[int, str]:
    """{multiplexer id: the rest of its status line} from mxcontrol's output."""
    return {int(match.group(1)): match.group(2) for match in map(STATUS_LINE.match, output.splitlines()) if match}


class RemoteRecording(unittest.TestCase):
    """Sessions on a two-multiplexer cluster, driven by mxcontrol and by the Python API."""

    def test_mxcontrol_starts_and_stops_sessions_on_every_replica(self):
        cfg = harness.CONFIG
        with Cluster(2, remote_recording=True) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
            )
            backend_id = backend.wait_for("connected", connections=2)["instance_id"]
            addresses = ["-M", cluster.addresses[0], "-M", cluster.addresses[1]]

            idle = parse(mxcontrol("recording", "status", *addresses).stdout)
            self.assertEqual(2, len(idle))
            self.assertTrue(all(line.startswith("not recording") for line in idle.values()), idle)

            started = parse(mxcontrol("recording", "start", "--label", "session", *addresses).stdout)
            self.assertEqual(sorted(idle), sorted(started))
            paths = {}
            for multiplexer_id, line in started.items():
                match = re.match(r"recording (\S+) \((\d+) records, (\d+) bytes, label session\)", line)
                self.assertIsNotNone(match, line)
                paths[multiplexer_id] = match.group(1)
                self.assertEqual(os.path.dirname(match.group(1)), cluster.recording_dir)
                self.assertIn(".%d.rec" % multiplexer_id, match.group(1), "the file is named after its multiplexer")
                self.assertTrue(os.path.basename(match.group(1)).startswith("session."))
            self.assertEqual(2, len(set(paths.values())), "one file per multiplexer, never the same name")

            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                query=[(C.types.TEST_REQUEST_A, "hello")],
            )
            self.assertEqual(0, client.wait())
            request_id = client.events_of("response")[0]["references"]

            counted = parse(mxcontrol("recording", "status", *addresses).stdout)
            records = sum(int(re.search(r"\((\d+) records", line).group(1)) for line in counted.values())
            self.assertGreaterEqual(records, 2 + 2 + 2, "two headers, the backend twice, the request and its reply")

            stopped = parse(mxcontrol("recording", "stop", *addresses).stdout)
            for multiplexer_id, line in stopped.items():
                self.assertRegex(
                    line,
                    r"^not recording; last %s \(\d+ records, \d+ bytes\) stopped by peer \d+$"
                    % re.escape(paths[multiplexer_id]),
                )

            self.assertEqual(sorted(paths.values()), cluster.recording_files())
            merged = list(recording.read_many(cluster.recording_files(), constants=C))
            headers = [record for record in merged if record.HasField("header")]
            self.assertEqual(sorted(paths), sorted(header.header.multiplexer_id for header in headers))
            self.assertEqual({"session"}, {header.header.label for header in headers})
            self.assertTrue(
                all(record.multiplexer_id in paths for record in merged), "every record says which multiplexer"
            )
            snapshots = [
                (record.multiplexer_id, record.peer.peer_id)
                for record in merged
                if record.HasField("peer") and record.peer.kind == PeerEvent.CONNECTED
            ]
            for multiplexer_id in paths:
                self.assertIn((multiplexer_id, backend_id), snapshots, "the already connected backend is written first")
            request = [record for record in merged if record.HasField("routed") and record.routed.id == request_id]
            self.assertEqual(1, len(request), "the request went through one multiplexer")
            self.assertEqual(
                (RoutedMessage.DELIVERED, b"hello"), (request[0].routed.disposition, request[0].routed.payload)
            )
            reply = [
                record for record in merged if record.HasField("routed") and record.routed.references == request_id
            ]
            self.assertEqual(request[0].multiplexer_id, reply[0].multiplexer_id)
            self.assertLess(merged.index(request[0]), merged.index(reply[0]))

            dump = mxcontrol("dump_recording", "--rules", cluster.mx[0].rules, *cluster.recording_files()).stdout
            lines = dump.splitlines()
            self.assertEqual(2, sum(1 for line in lines if " header " in line), "both files, merged")
            self.assertEqual(
                sorted(paths),
                sorted(int(re.search(r" mx=(\d+) ", line).group(1)) for line in lines if " header " in line),
            )
            self.assertTrue(all(" mx=" in line for line in lines), "every line says which multiplexer")
            self.assertIn("label=session", lines[0])
            stamps = [float(line.split()[0]) for line in lines]
            self.assertEqual(stamps, sorted(stamps), "merged by time")
            self.assertIn("routed DELIVERED type=TEST_REQUEST_A id=%d" % request_id, dump)

            again = parse(mxcontrol("recording", "start", "--label", "session", *addresses).stdout)
            self.assertEqual(4, len(cluster.recording_files()), "a new session, new files, same label")
            self.assertTrue(all(line.startswith("recording ") for line in again.values()), again)
            mxcontrol("recording", "stop", *addresses)

    def test_python_api_drives_the_same_sessions(self):
        with Cluster(2, remote_recording=True) as cluster:
            controller = Client(cluster.endpoints, type=recording.RECORDING_CONTROLLER)
            try:
                statuses = recording.status(controller)
                self.assertEqual(2, len(statuses))
                self.assertFalse(any(status.recording for status in statuses))
                self.assertFalse(any(status.HasField("error") for status in statuses))

                started = recording.start(controller, "api", payload_limit=4)
                self.assertEqual(2, len(started))
                for status in started:
                    self.assertFalse(status.HasField("error"), status)
                    self.assertTrue(status.recording)
                    self.assertEqual("api", status.label)
                    self.assertTrue(status.path.startswith(cluster.recording_dir + "/api."))
                self.assertEqual(2, len({status.path for status in started}))

                stopped = recording.stop(controller)
                self.assertEqual(
                    {status.multiplexer_id for status in started}, {status.multiplexer_id for status in stopped}
                )
                for status in stopped:
                    self.assertFalse(status.recording)
                    self.assertEqual("stopped by peer %d" % controller.instance_id, status.stopped)
                    header = next(recording.read(status.path, constants=C))
                    self.assertEqual(
                        (status.multiplexer_id, "api", 4),
                        (header.header.multiplexer_id, header.header.label, header.header.payload_limit),
                    )
            finally:
                controller.shutdown()


if __name__ == "__main__":
    harness.main()
