"""recording.read() on a file that ends partway through a record, as one
a session is still writing, or a multiplexer that died left, can: every
whole record, then TruncatedRecording, whether the file ends inside a
record's length, inside its body, or inside its body where the bytes
before the cut parse on their own; where it raised IndexError or
DecodeError, or yielded the cut record as a whole one. read_many() merges
the other files to their ends before it raises, where it gave up on every
file at the first broken one. And `python -m multiplexer.recording`
prints up to the break and exits 1, as mxcontrol dump_recording does.
"""

import contextlib
import io
import os
import tempfile
import unittest

from multiplexer import multiplexer_constants
from multiplexer.Recording_pb2 import Record
from multiplexer.recording import TruncatedRecording, main, read, read_many

PAYLOAD = b"x" * 200  # a routed record past 127 bytes: its length takes two bytes


def varint(value: int) -> bytes:
    """`value` as a base-128 varint, as a recording prefixes each record with its length."""
    encoded = bytearray()
    while value >= 0x80:
        encoded.append(value & 0x7F | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def framed(record: Record) -> bytes:
    """`record` as a recording holds it: its length, then its bytes."""
    body = record.SerializeToString()
    return varint(len(body)) + body


def header(multiplexer_id: int) -> Record:
    """The first record of a file, with the rules the constants come from."""
    record = Record(timestamp_us=1)
    record.header.multiplexer_id = multiplexer_id
    record.header.rules_fingerprint = multiplexer_constants.RULES_FINGERPRINT
    return record


def routed(timestamp_us: int) -> Record:
    """A routed message's record at `timestamp_us`, its payload PAYLOAD."""
    record = Record(timestamp_us=timestamp_us)
    record.routed.payload = PAYLOAD
    return record


class RecordingReadTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def write(self, name: str, data: bytes) -> str:
        """`data` in a file of the test's own; its path."""
        path = os.path.join(self.directory.name, name)
        with open(path, "wb") as file:
            file.write(data)
        return path

    def whole(self, multiplexer_id: int, timestamps: list[int]) -> bytes:
        """A file's bytes: its header, then a routed record at each of `timestamps`."""
        return framed(header(multiplexer_id)) + b"".join(framed(routed(when)) for when in timestamps)

    def read_until_broken(self, path: str) -> list[Record]:
        """What read() yields before it raises TruncatedRecording, which it must."""
        records: list[Record] = []
        with self.assertRaises(TruncatedRecording):
            for record in read(path):
                records.append(record)
        return records

    def test_a_torn_tail_ends_the_records_and_says_so(self) -> None:
        last = framed(routed(40))
        cut_body = Record(timestamp_us=40).SerializeToString()  # the bytes before the routed field
        tails = {
            "inside the length": last[:1],
            "inside the body": last[: len(last) // 2],
            "where the bytes before the cut parse": last[:2] + cut_body,
        }
        for where, tail in tails.items():
            with self.subTest(where=where):
                path = self.write("torn.rec", self.whole(7, [10, 20, 30]) + tail)
                records = self.read_until_broken(path)
                self.assertEqual([1, 10, 20, 30], [record.timestamp_us for record in records])

    def test_a_whole_file_reads_to_its_end(self) -> None:
        path = self.write("whole.rec", self.whole(7, [10, 20]))
        self.assertEqual([1, 10, 20], [record.timestamp_us for record in read(path)])

    def test_read_many_merges_the_other_files_to_their_ends(self) -> None:
        torn = self.write("torn.rec", self.whole(7, [10, 30]) + framed(routed(50))[:1])
        whole = self.write("whole.rec", self.whole(8, [20, 40, 60, 80]))
        merged: list[Record] = []
        with self.assertRaises(TruncatedRecording) as raised:
            for record in read_many([torn, whole]):
                merged.append(record)
        self.assertEqual([1, 1, 10, 20, 30, 40, 60, 80], [record.timestamp_us for record in merged])
        self.assertIn(torn, str(raised.exception))

    def test_the_command_prints_up_to_the_break_and_exits_1(self) -> None:
        path = self.write("torn.rec", self.whole(7, [10, 20]) + framed(routed(30))[:1])
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            status = main([path])
        self.assertEqual(1, status)
        self.assertEqual(3, len(out.getvalue().splitlines()))
        self.assertIn(path, err.getvalue())


if __name__ == "__main__":
    unittest.main()
