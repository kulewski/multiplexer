"""mxcontrol streamlogs never stops reading its input for a multiplexer that
stopped reading: with one of two multiplexers frozen (SIGSTOP, its
connections open), every chunk still reaches the other one at once, and the
stream is read to its end. Before, each chunk waited up to 10 s to be
written to every multiplexer, and meanwhile nothing read the pipe, so the
program whose log it was blocked on its next log line, 32 entries getting
through every 10 s. The stream's chunks are large there, FILL_FRAMES of
them carrying twice the kernel's socket buffer limits, so that whatever
this machine buffers for the frozen multiplexer, the stream outgrows it in
a few chunks. And with no log receiver connected, the
DELIVERY_ERROR every chunk draws is said in streamlogs' own words, 'no log
receiver took the log stream', where streamlogs logged the library's line
about a message it had no callback for.
"""

import os
import subprocess
import tempfile
import threading
import unittest
from typing import IO, Iterable

from multiplexer.Multiplexer_pb2 import LogEntriesMessage
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, mx_runfile, runfile, wait_until
from multiplexer.testing.buffers import FILL_FRAMES, fill_size
from multiplexer.testing.fakes import FakePeer

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")
RULES = runfile("tests/testing.rules")
CHUNK = 32  # entries in a LOGS_STREAM message, streamlogs' default
NOBODY = "no log receiver took the log stream"
TEXT = "x" * 1000  # about a kilobyte an entry


def varint(value: int) -> bytes:
    """`value` as a base-128 varint, the length before each entry."""
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def entries(count: int, first: int, text: str = TEXT) -> bytes:
    """`count` log entries as the stream holds them, each its length and
    then the entry, numbered from `first`."""
    holder = LogEntriesMessage()
    out = bytearray()
    for index in range(first, first + count):
        body = holder.log.add(id=index + 1, text=text).SerializeToString()
        out += varint(len(body)) + body
    return bytes(out)


def write_all(pipe: IO[bytes], parts: Iterable[bytes]) -> None:
    """Write `parts` into `pipe`, on a thread of its own: a streamer that
    stops reading blocks it, not the test."""
    try:
        for part in parts:
            pipe.write(part)
        pipe.flush()
    except (BrokenPipeError, ValueError):
        pass  # the streamer was ended under it


class StreamLogsTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_frozen_multiplexer_does_not_stop_the_stream(self) -> None:
        with Cluster(2, rules=RULES) as cluster:
            frozen = cluster.mx[0]
            receivers = [
                FakePeer(cluster, peers.LOG_RECEIVER_EXAMPLE, name="receiver-%d" % index, endpoints=[endpoint])
                for index, endpoint in enumerate(cluster.endpoints)
            ]
            addresses = ["--multiplexer=%s:%d" % endpoint for endpoint in cluster.endpoints]
            log = tempfile.TemporaryFile(dir=os.environ.get("TEST_TMPDIR"))
            with receivers[0], receivers[1], log:
                streamer = subprocess.Popen([MXCONTROL, "streamlogs"] + addresses, stdin=subprocess.PIPE, stderr=log)
                assert streamer.stdin is not None
                try:
                    # The streamer connects without waiting, and a chunk goes to the
                    # connections up by then: chunks, one at a time, until one came
                    # through the multiplexer to be frozen, whose connection is up then.
                    cluster.wait_for_peer(peers.LOG_STREAMER)
                    sent = 0
                    while not receivers[0].messages(types.LOGS_STREAM):
                        self.assertLess(sent, 50, "no chunk came through the first multiplexer")
                        streamer.stdin.write(entries(CHUNK, sent * CHUNK))
                        streamer.stdin.flush()
                        sent += 1
                        receivers[1].wait_for(types.LOGS_STREAM, sent, timeout=20)
                    # Chunks of fill_size() bytes, then two of the usual size.
                    large = "x" * (fill_size() // CHUNK)
                    stream = [entries(CHUNK, (sent + index) * CHUNK, large) for index in range(FILL_FRAMES)]
                    stream.append(entries(2 * CHUNK, (sent + FILL_FRAMES) * CHUNK))
                    chunks = FILL_FRAMES + 2
                    frozen.pause()
                    try:
                        threading.Thread(target=write_all, args=(streamer.stdin, stream), daemon=True).start()
                        receivers[1].wait_for(types.LOGS_STREAM, sent + chunks, timeout=60)
                    finally:
                        frozen.resume()
                    streamer.stdin.close()
                    self.assertEqual(0, streamer.wait(timeout=60))
                finally:
                    if streamer.poll() is None:
                        streamer.kill()
                        streamer.wait()

    def test_a_stream_no_log_receiver_takes_is_said_so(self) -> None:
        chunks = 40
        with Cluster(1, rules=RULES) as cluster:
            log = tempfile.TemporaryFile(dir=os.environ.get("TEST_TMPDIR"))
            with log:
                streamer = subprocess.Popen(
                    [MXCONTROL, "streamlogs", "--multiplexer=%s:%d" % cluster.endpoints[0]],
                    stdin=subprocess.PIPE,
                    stderr=log,
                )
                assert streamer.stdin is not None

                def said() -> bool:
                    """Whether streamlogs has said that no log receiver took a chunk."""
                    log.seek(0)
                    return NOBODY.encode() in log.read()

                try:
                    cluster.wait_for_peer(peers.LOG_STREAMER)
                    streamer.stdin.write(entries(chunks * CHUNK, 0))
                    streamer.stdin.flush()
                    wait_until(said, 30, "streamlogs says no log receiver took the stream")
                    streamer.stdin.close()
                    self.assertEqual(0, streamer.wait(timeout=60))
                finally:
                    if streamer.poll() is None:
                        streamer.kill()
                        streamer.wait()
                log.seek(0)
                text = log.read().decode()
        self.assertNotIn("no on_message callback", text)
        self.assertNotIn("arrived after the connection began closing", text)
        lines = [line for line in text.splitlines() if NOBODY in line]
        self.assertLessEqual(len(lines), chunks // 4, "lines for %d chunks nobody took:\n%s" % (chunks, text))


if __name__ == "__main__":
    unittest.main()
