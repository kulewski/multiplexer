"""The example's test on the library's harness (docs/api_python.md,
"Testing"): real multiplexers from `Cluster`, the C++ worker as a
process, a synthetic tone through it, a stream's affinity to one worker
and its move to another when that one is killed, a drain under a stream,
and the consumer driven by Channels' `WebsocketCommunicator` against
Django configured for the cluster; and the committed constants checked
against the rules file. Needs the worker built (`AUDIO_WORKER`, or
bazel-bin/worker/worker) and Channels, which test.sh arranges; the
multiplexers are the mxcontrol the package installed unless MXCONTROL
names another. `python -m unittest -v test`."""

import math
import os
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "channels"))  # the channel layer the gateway runs on
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "web"))

from multiplexer.mxclient import OperationFailed, OperationTimedOut  # noqa: E402
from multiplexer.testing import Cluster, mxcontrol_path, wait_until  # noqa: E402
from multiplexer.threaded_client import ThreadedClient  # noqa: E402

from audio_pb2 import AudioFrame, AudioProcessed  # noqa: E402
from multiplexer_constants import peers, types  # noqa: E402

RULES = os.path.join(HERE, "audio.rules")
WORKER = os.environ.get("AUDIO_WORKER", os.path.join(HERE, "bazel-bin", "worker", "worker"))
# A worker dies with the test process however that ends, where util-linux's
# setpriv is there: one left behind would reconnect to its ports for good
# and could join a later run's multiplexer.
WITH_THE_TEST = ["setpriv", "--pdeathsig", "KILL", "--"] if shutil.which("setpriv") else []
FRAME = 480
SAMPLE_RATE = 48000
BANDS = 32


def endpoints_text(cluster: Cluster) -> str:
    """The cluster's multiplexers as one worker argument or one settings value."""
    return ",".join(f"{host}:{port}" for host, port in cluster.endpoints)


def tone(hz: float, amplitude: float = 0.5, frame_index: int = 0) -> bytes:
    """One frame of a sine, as the PCM a browser would send; at full scale, both extremes."""
    start = frame_index * FRAME
    samples = [
        max(-32768, min(32767, round(32768 * amplitude * math.sin(2 * math.pi * hz * (start + i) / SAMPLE_RATE))))
        for i in range(FRAME)
    ]
    return struct.pack(f"<{FRAME}h", *samples)


def band_of(hz: float) -> int:
    """The spectrum band a frequency falls in: the worker's formula, 60 Hz to 12 kHz over 32 bands."""
    return max(0, min(BANDS - 1, int(BANDS * math.log(hz / 60.0) / math.log(200.0))))


class Worker:
    """The C++ worker as a process in a group of its own, so that a
    wrapper named by AUDIO_WORKER goes with it; its instance id read from
    its ready line."""

    def __init__(self, cluster: Cluster, name: str):
        self.process = subprocess.Popen(
            WITH_THE_TEST + [WORKER, endpoints_text(cluster), name],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            start_new_session=True,
        )
        assert self.process.stdout is not None
        ready = self.process.stdout.readline()
        if not ready.startswith("ready:"):
            self.kill()
            raise AssertionError(f"the worker did not start: {ready!r}")
        self.name = name
        self.instance_id = int(ready.rsplit(" ", 1)[1])

    def kill(self) -> None:
        """As a machine dies: no goodbye. Nothing if it is gone already."""
        if self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGKILL)
        self.reap()

    def drain(self) -> str:
        """As a deployment asks: SIGTERM, which the worker turns into a
        drain; the line it prints once the drain has begun."""
        os.killpg(self.process.pid, signal.SIGTERM)
        assert self.process.stdout is not None
        return self.process.stdout.readline()

    def reap(self) -> tuple[int, str]:
        """Wait for the exit, and let go of the pipe; the exit status, and the rest of the output."""
        status = self.process.wait(timeout=10)
        assert self.process.stdout is not None
        if self.process.stdout.closed:
            return status, ""  # reaped already
        rest = self.process.stdout.read()
        self.process.stdout.close()
        return status, rest


class WorkerTest(unittest.TestCase):
    """Two multiplexers, two workers, a client asking directly, as the gateway does."""

    @classmethod
    def setUpClass(cls):
        # Cleanups, not tearDownClass: they run when a step here fails too.
        cls.cluster = Cluster(2, rules=RULES).__enter__()
        cls.addClassCleanup(cls.cluster.__exit__, None, None, None)
        cls.workers = []
        cls.addClassCleanup(lambda: [worker.kill() for worker in cls.workers])
        cls.workers.extend(Worker(cls.cluster, name) for name in ("alpha", "beta"))
        cls.cluster.wait_for_peer(peers.DSP, count=2)
        cls.client = ThreadedClient(cls.cluster.endpoints, type=peers.CHANNELS)
        cls.addClassCleanup(cls.client.shutdown)

    def ask(self, frame: AudioFrame, to: int = 0):
        """One frame through the broker; the answer parsed, and who answered."""
        reply = self.client.query(frame.SerializeToString(), type=types.AUDIO_FRAME, timeout=5, to=to)
        return AudioProcessed.FromString(reply.message), reply.from_

    def test_a_frame_comes_back_with_its_spectrum(self):
        hz = 11 * SAMPLE_RATE / 512  # on a bin of the worker's FFT
        frame = AudioFrame(participant=7, seq=3, effect="none", pcm=tone(hz), captured_at=123.5)
        processed, _ = self.ask(frame)
        self.assertEqual((7, 3, 123.5), (processed.participant, processed.seq, processed.captured_at))
        self.assertEqual(len(frame.pcm), len(processed.pcm))
        self.assertEqual(frame.pcm, processed.pcm, "none: the same samples, as bytes")
        full = AudioFrame(participant=7, seq=4, effect="none", pcm=tone(hz, amplitude=1.0))
        self.assertEqual(full.pcm, self.ask(full)[0].pcm, "full scale too, both extremes")
        self.assertEqual(BANDS, len(processed.spectrum))
        loudest = max(range(BANDS), key=lambda band: processed.spectrum[band])
        self.assertEqual(band_of(hz), loudest, "the tone's band is the loudest")
        self.assertGreater(processed.spectrum[loudest], 200)
        self.assertIn(processed.worker, ("alpha", "beta"))
        self.assertLess(processed.micros, 5000, "microseconds, not milliseconds")

    def test_an_effect_changes_the_frame(self):
        plain, _ = self.ask(AudioFrame(participant=8, seq=0, effect="none", pcm=tone(440)))
        robot, _ = self.ask(AudioFrame(participant=8, seq=1, effect="robot", pcm=tone(440)))
        self.assertNotEqual(plain.pcm, robot.pcm)

    def test_a_stream_sticks_to_a_worker_and_moves_when_it_dies(self):
        processed, worker_id = self.ask(AudioFrame(participant=9, seq=0, effect="echo", pcm=tone(440)))
        first = next(worker for worker in self.workers if worker.instance_id == worker_id)
        self.assertEqual(first.name, processed.worker)
        for seq in range(1, 6):  # addressed: every frame to the same worker, whatever the round robin would do
            processed, answered_by = self.ask(
                AudioFrame(participant=9, seq=seq, effect="echo", pcm=tone(440)), to=worker_id
            )
            self.assertEqual(worker_id, answered_by)
        first.kill()
        wait_until(
            lambda: all(
                sum(1 for _, _, number in mx.connected_peers() if number == peers.DSP) == 1 for mx in self.cluster.mx
            ),
            10,
            "the dead worker gone from both multiplexers",
        )
        with self.assertRaises(OperationFailed):  # addressed to an instance that is gone: at once, not by timeout
            self.ask(AudioFrame(participant=9, seq=6, effect="echo", pcm=tone(440)), to=worker_id)
        processed, answered_by = self.ask(AudioFrame(participant=9, seq=7, effect="echo", pcm=tone(440)))
        self.assertNotEqual(worker_id, answered_by, "the other worker, the only one the multiplexers have")
        self.assertEqual(next(w for w in self.workers if w is not first).name, processed.worker)
        self.workers.remove(first)
        self.workers.append(Worker(self.cluster, first.name))  # back, for the tests that follow
        self.cluster.wait_for_peer(peers.DSP, count=2)

    def test_a_drained_worker_serves_its_stream_then_leaves_on_the_confirmation(self):
        """SIGTERM under a stream addressed to the worker: the frames are
        answered while it drains, it leaves once both multiplexers have
        confirmed, well within the three seconds it allows itself, and then
        the stream's next frame is refused at once. A frame caught on its
        way at the close may be lost to its timeout first, as in the gateway."""
        extra = Worker(self.cluster, "gamma")
        self.workers.append(extra)
        self.cluster.wait_for_peer(peers.DSP, count=3)
        answered, failures = [0], []

        def stream() -> None:
            for seq in range(1000):
                frame = AudioFrame(participant=11, seq=seq, effect="echo", pcm=tone(440))
                try:
                    self.client.query(
                        frame.SerializeToString(), type=types.AUDIO_FRAME, timeout=0.5, to=extra.instance_id
                    )
                except (OperationFailed, OperationTimedOut) as error:
                    failures.append(type(error))
                    if isinstance(error, OperationFailed):
                        return
                    continue
                answered[0] += 1
                time.sleep(0.01)

        streaming = threading.Thread(target=stream)
        streaming.start()
        wait_until(lambda: answered[0] >= 10, 5, "the stream on gamma")
        started = time.monotonic()
        self.assertEqual("leaving\n", extra.drain(), "the drain begun")
        during = answered[0]  # every frame answered from here on was answered while the worker drained
        status, output = extra.reap()
        took = time.monotonic() - started
        streaming.join(10)
        self.assertEqual((0, "left\n"), (status, output), "a drain, not a crash")
        self.assertLess(took, 1.5, "ended by the confirmation, not by the three seconds")
        self.assertGreater(answered[0], during, "addressed frames served while draining")
        self.assertIn(failures, ([OperationFailed], [OperationTimedOut, OperationFailed]), "refused at once once gone")
        wait_until(
            lambda: all(
                sum(1 for _, _, number in mx.connected_peers() if number == peers.DSP) == 2 for mx in self.cluster.mx
            ),
            10,
            "gamma gone from both multiplexers",
        )


class GatewayTest(unittest.IsolatedAsyncioTestCase):
    """The consumer through Channels' communicator: frames in, the room's frames out, with the worker behind."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(1, rules=RULES).__enter__()
        cls.addClassCleanup(cls.cluster.__exit__, None, None, None)
        cls.worker = Worker(cls.cluster, "alpha")
        cls.addClassCleanup(cls.worker.kill)
        cls.cluster.wait_for_peer(peers.DSP)
        os.environ["MX_ADDRESSES"] = endpoints_text(cls.cluster)
        os.environ["DJANGO_SETTINGS_MODULE"] = "webapp.settings"
        import django

        django.setup()

    async def asyncTearDown(self):
        from channels.layers import get_channel_layer

        from mxchannels import MultiplexerChannelLayer

        layer = get_channel_layer()
        assert isinstance(layer, MultiplexerChannelLayer)
        await layer.close()

    async def test_a_frame_reaches_everyone_in_the_room_with_its_numbers(self):
        from channels.testing import WebsocketCommunicator

        from room.consumers import DOWN, UP
        from webapp.asgi import application

        origin = [(b"origin", b"http://localhost")]
        alice = WebsocketCommunicator(application, "/ws/audio/studio/", headers=origin)
        bob = WebsocketCommunicator(application, "/ws/audio/studio/", headers=origin)
        hellos = {}
        for name, socket in (("alice", alice), ("bob", bob)):
            connected, _ = await socket.connect()
            self.assertTrue(connected)
            hellos[name] = await socket.receive_json_from()
            self.assertEqual(["none", "telephone", "robot", "echo"], hellos[name]["effects"])
        await alice.send_json_to({"effect": "telephone"})
        first_sent = time.monotonic()
        await alice.send_to(bytes_data=UP.pack(0, 1000.0) + tone(1000))
        for socket in (alice, bob):
            frame = await socket.receive_from(timeout=5)
            assert isinstance(frame, bytes), "binary, not text"
            participant, seq, captured_at, gateway_us, worker_us, worker = DOWN.unpack_from(frame)
            self.assertEqual((hellos["alice"]["participant"], 0, 1000.0), (participant, seq, captured_at))
            self.assertEqual("alpha", worker.rstrip(b"\0").decode())
            self.assertLess(worker_us, 5000)
            self.assertLess(gateway_us, 500000)
            self.assertEqual(DOWN.size + BANDS + 2 * FRAME, len(frame))
        self.assertTrue(await alice.receive_nothing(0.2), "one frame in, one frame out per socket")
        # Alice's clock, as the page's: capture times in ms, the first frame's 1000. A frame captured
        # 300 ms before it reached the gateway has waited: dropped. The next, on time, goes through.
        now = 1000.0 + (time.monotonic() - first_sent) * 1000
        await alice.send_to(bytes_data=UP.pack(1, now - 300) + tone(1000))
        await alice.send_to(bytes_data=UP.pack(2, now) + tone(1000))
        frame = await bob.receive_from(timeout=5)
        assert isinstance(frame, bytes)
        self.assertEqual(2, DOWN.unpack_from(frame)[1], "the stale one never reached the room")
        for socket in (alice, bob):
            await socket.disconnect()


class ConstantsTest(unittest.TestCase):
    """The committed constants are what mxcontrol writes from the rules file
    as it is now: a change to the file that left them behind fails here."""

    def test_the_constants_are_generated_from_the_rules_file(self):
        out = tempfile.mkdtemp()
        written = [os.path.join(out, name) for name in ("multiplexer_constants.py", "multiplexer_constants.pyi")]
        command = [mxcontrol_path(), "generate_constants", "audio.rules", "--python", written[0], "--pyi", written[1]]
        subprocess.run(command, cwd=HERE, check=True, capture_output=True)  # the file's name as the header records it
        for path in written:
            with open(path) as generated, open(os.path.join(HERE, os.path.basename(path))) as committed:
                self.assertEqual(committed.read(), generated.read(), os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
