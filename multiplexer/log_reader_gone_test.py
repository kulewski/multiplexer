"""A multiplexer that logs into a pipe outlives the pipe's reader, as one
logging into `mxcontrol streamlogs`, tee or logger does when that ends: its
binary log stream (--logging-fd) is dropped, once, with a line in its log
saying so, and it serves on, where it died of SIGPIPE on its next log line.
Each SIGHUP makes the multiplexer write a log line."""

import os
import unittest

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers
from multiplexer.testing import Mx, runfile, wait_until

RULES = runfile("tests/testing.rules")  # the file the constants were generated from
DROPPED = "the binary log stream's reader is gone"
RELOADED = "received SIGHUP"


def count_in_log(mx: Mx, text: str) -> int:
    """How many times the multiplexer's log holds `text` so far."""
    with open(mx.log_path, "rb") as log:
        return log.read().count(text.encode())


class LogReaderGoneTest(unittest.TestCase):
    def test_a_multiplexer_whose_log_reader_exits_serves_on(self) -> None:
        reading, writing = os.pipe()
        mx = Mx(0, rules=RULES, logging_fd=writing).start()
        try:
            os.close(writing)  # the multiplexer has its own copy
            os.close(reading)  # and now nobody reads
            for reloads in (1, 2):
                mx.reload_rules()
                wait_until(
                    lambda reloads=reloads: not mx.running or count_in_log(mx, RELOADED) == reloads,
                    10,
                    "SIGHUP number %d logged" % reloads,
                )
                self.assertTrue(mx.running, "the multiplexer died, exit %s" % (mx.proc and mx.proc.returncode))
            self.assertEqual(1, count_in_log(mx, DROPPED), "the stream is dropped once")
            client = Client([mx.endpoint], type=peers.WEBSITE)
            try:
                wait_until(
                    lambda: any(peer_type == peers.WEBSITE for _, _, peer_type in mx.connected_peers()),
                    10,
                    "the client registered",
                )
            finally:
                client.shutdown()
            self.assertEqual(0, mx.stop())
        finally:
            mx.kill()


if __name__ == "__main__":
    unittest.main()
