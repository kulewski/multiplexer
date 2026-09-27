"""The commands that read length-delimited streams, dump_recording a
recording and streamlogs a binary log on stdin: a whole stream is read to
its end and the command exits 0; one that breaks off, an entry cut short,
is read up to there, said on stderr, and the command exits 1, where it
exited 0 as if the stream had ended there. The streams are written here by
hand."""

import os
import subprocess
import tempfile
import unittest

from multiplexer.Multiplexer_pb2 import LogEntriesMessage
from multiplexer.Recording_pb2 import Record
from multiplexer.testing import Cluster, mx_runfile, runfile

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")
RULES = runfile("tests/testing.rules")


def varint(value: int) -> bytes:
    """`value` as a base-128 varint, the length before each entry."""
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def framed(message) -> bytes:
    """`message` as the stream holds it: its length, then the message."""
    body = message.SerializeToString()
    return varint(len(body)) + body


def recording() -> list[bytes]:
    """A header and two routed messages, a second apart, each framed."""
    header = Record(timestamp_us=1_000_000)
    header.header.multiplexer_id = 7
    first = Record(timestamp_us=2_000_000)
    first.routed.id = 11
    second = Record(timestamp_us=3_000_000)
    second.routed.id = 12
    return [framed(record) for record in (header, first, second)]


def log_stream() -> list[bytes]:
    """Three log entries, each framed."""
    entries = LogEntriesMessage()
    return [framed(entries.log.add(id=index + 1, text="entry %d" % index)) for index in range(3)]


def cut(frames: list[bytes]) -> bytes:
    """The frames with the last one cut in half."""
    return b"".join(frames[:-1]) + frames[-1][: len(frames[-1]) // 2]


class DumpRecordingTest(unittest.TestCase):
    """See the module docstring."""

    def dump(self, data: bytes) -> subprocess.CompletedProcess:
        """`mxcontrol dump_recording` on a file holding `data`."""
        path = os.path.join(tempfile.mkdtemp(dir=os.environ.get("TEST_TMPDIR")), "session.rec")
        with open(path, "wb") as recording_file:
            recording_file.write(data)
        return subprocess.run([MXCONTROL, "dump_recording", path], capture_output=True, text=True, timeout=30)

    def test_a_whole_recording_is_printed_and_exits_0(self):
        result = self.dump(b"".join(recording()))
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(3, len(result.stdout.splitlines()), result.stdout)

    def test_a_recording_cut_short_is_printed_up_to_the_cut_and_exits_1(self):
        result = self.dump(cut(recording()))
        self.assertEqual(1, result.returncode, result.stderr)
        self.assertEqual(2, len(result.stdout.splitlines()), "the records before the cut")
        self.assertIn("cut short or garbled", result.stderr)


class StreamLogsTest(unittest.TestCase):
    """See the module docstring."""

    def stream(self, data: bytes) -> subprocess.CompletedProcess:
        """`mxcontrol streamlogs` with `data` on stdin, through a multiplexer."""
        with Cluster(1, rules=RULES) as cluster:
            host, port = cluster.endpoints[0]
            return subprocess.run(
                [MXCONTROL, "streamlogs", "--multiplexer", "%s:%d" % (host, port)],
                input=data,
                capture_output=True,
                timeout=30,
            )

    def test_a_whole_stream_is_sent_and_exits_0(self):
        result = self.stream(b"".join(log_stream()))
        self.assertEqual(0, result.returncode, result.stderr)

    def test_a_stream_cut_short_exits_1(self):
        result = self.stream(cut(log_stream()))
        self.assertEqual(1, result.returncode, result.stderr)
        self.assertIn(b"broke off", result.stderr)


if __name__ == "__main__":
    unittest.main()
