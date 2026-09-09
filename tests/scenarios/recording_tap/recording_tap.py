"""a peer taps two multiplexers and receives every record live, tagged with the multiplexer that routed it; a slow tap loses records but nothing else.

`mxcontrol recording tap` subscribes on every connection and writes the
records it receives as one stream, each carrying the multiplexer's id;
nothing is written on the multiplexers' side. A tap that stops reading fills
its outgoing queue: the multiplexer drops its records, counts them in the
status, and keeps routing.
"""

import os
import subprocess
import unittest

from multiplexer import recording
from multiplexer.Recording_pb2 import RecordingControl
from multiplexer.clients import Client
from multiplexer.mxclient import OperationTimedOut
from tests import harness
from tests.harness import Cluster, child_env, constants as C, mx_runfile, output_dir, spawn, wait_until


class RecordingTap(unittest.TestCase):
    """Taps from mxcontrol and from the Python API."""

    def test_mxcontrol_tap_streams_from_every_multiplexer(self):
        cfg = harness.CONFIG
        with Cluster(2, remote_recording=True) as cluster:
            controller = Client(cluster.endpoints, type=recording.RECORDING_CONTROLLER)
            out = os.path.join(output_dir(), "tap.rec")
            log = open(os.path.join(output_dir(), "tap.stderr.log"), "wb")
            tap = subprocess.Popen(
                [mx_runfile("mxcontrol/mxcontrol"), "recording", "tap", "--out", out]
                + [arg for address in cluster.addresses for arg in ("-M", address)],
                stderr=log,
                env=child_env(native=True),
            )
            try:
                wait_until(
                    lambda: all(status.taps == 1 for status in recording.status(controller))
                    and len(recording.status(controller)) == 2,
                    10,
                    "both multiplexers to have the tap",
                )
                backend = spawn(
                    "backend",
                    cfg.lang("backend"),
                    mx=cluster.addresses,
                    type=C.peers.TEST_BACKEND_A,
                    serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                )
                backend_id = backend.wait_for("connected", connections=2)["instance_id"]
                sender = spawn(
                    "event_client",
                    cfg.lang("event_client"),
                    mx=cluster.addresses,
                    type=C.peers.TEST_EVENT_CLIENT,
                    all=True,
                    send=[(C.types.TEST_EVENT, "through both")],
                )
                self.assertEqual(0, sender.wait())
                event_id = sender.events_of("sent")[0]["id"]

                def streamed():
                    records = list(recording.read(out, check_rules=False)) if os.path.exists(out) else []
                    routed = [r for r in records if r.HasField("routed") and r.routed.id == event_id]
                    return records if len(routed) == 2 else None

                records = wait_until(streamed, 10, "the event's record from both multiplexers")
                multiplexers = {status.multiplexer_id for status in recording.status(controller)}
                self.assertEqual(multiplexers, {r.multiplexer_id for r in records if r.routed.id == event_id})
                self.assertEqual(
                    {backend_id},
                    {r.peer.peer_id for r in records if r.HasField("peer")} & {backend_id},
                    "the backend's arrival was streamed",
                )
                self.assertFalse(any(r.HasField("header") for r in records), "a tap has no file header")
                self.assertFalse(
                    any(r.routed.payload == b"" for r in records if r.HasField("routed") and r.routed.id == event_id)
                )
                self.assertEqual([], cluster.recording_files(), "nothing written on the multiplexers' side")

                tap.send_signal(2)  # SIGINT: untap and leave
                self.assertEqual(0, tap.wait(10))
                log.close()
                with open(log.name) as stderr:
                    text = stderr.read()
                self.assertIn("records written", text)
                self.assertEqual(2, text.count(": tapping"), text)
                self.assertTrue(
                    all(status.taps == 0 for status in recording.status(controller)), "untapped on the way out"
                )
            finally:
                if tap.poll() is None:
                    tap.kill()
                controller.shutdown()

    def test_a_tap_that_stops_reading_loses_records_and_nothing_else(self):
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
            controller = Client(cluster.endpoints, type=recording.RECORDING_CONTROLLER)
            file_controller = Client(cluster.endpoints, type=recording.RECORDING_CONTROLLER)
            try:
                (session,) = recording.start(file_controller, "beside-the-tap")
                stream = recording.tap(controller, timeout=1)
                # The generator sent TAP; not reading now is what a slow consumer does.
                client = spawn(
                    "client",
                    cfg.lang("client"),
                    mx=cluster.addresses,
                    type=C.peers.TEST_CLIENT,
                    query=[(C.types.TEST_REQUEST_A, "hi")],
                    count=200,
                    payload_size=20000,
                    parallel=4,
                )
                self.assertEqual(0, client.wait(60))
                self.assertEqual(800, len(client.events_of("response")), "routing went on regardless")

                received = 0
                try:
                    for _ in stream:
                        received += 1
                except OperationTimedOut:
                    pass
                self.assertGreater(received, 0)
                self.assertLess(received, 1600, "not everything fit the queue")
                (status,) = recording.status(controller)
                self.assertTrue(status.tapping)
                self.assertGreater(status.dropped, 0)
                self.assertGreaterEqual(status.dropped + received, 1600, "dropped plus received covers the traffic")

                (stopped,) = recording.stop(file_controller)
                routed = [r for r in recording.read(session.path, constants=C) if r.HasField("routed")]
                self.assertEqual(
                    1600,
                    len([r for r in routed if r.routed.type in (C.types.TEST_REQUEST_A, C.types.TEST_RESPONSE)]),
                    "the file session lost nothing",
                )
                (untapped,) = recording.control(controller, RecordingControl.UNTAP)
                self.assertFalse(untapped.tapping)
                self.assertEqual(0, untapped.taps)
            finally:
                controller.shutdown()
                file_controller.shutdown()


if __name__ == "__main__":
    harness.main()
