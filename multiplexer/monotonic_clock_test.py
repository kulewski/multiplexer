"""Python deadlines are on the monotonic clock, as the C++ side's are on
steady_clock: a step of the wall clock, an hour either way, neither cuts
nor stretches a synchronous call's TimeoutTicker, ends a backend's drain
before its period, in either server class, nor ends a harness wait, where
each read time.time() and did. The wall clock is stepped by a mock of
time.time(), so nothing is waited out.
"""

import time
import unittest
from unittest import mock

from multiplexer.Multiplexer_pb2 import Routing
from multiplexer.multiplexer_constants import peers
from multiplexer.mxclient import TimeoutTicker
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import wait_until
from multiplexer.threaded_server import BaseThreadedMultiplexerServer

HOUR = 3600.0
# A drain that keeps the `all` path open: events keep coming, so only its
# period can end it.
OPEN_PATH = Routing(any=False)


class SteppedWallClock:
    """time.time() as the test steps it: the real wall clock, `offset`
    seconds off."""

    def __init__(self) -> None:
        self.real = time.time
        self.offset = 0.0

    def __call__(self) -> float:
        """The stepped wall clock's reading."""
        return self.real() + self.offset


class MonotonicClockTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self) -> None:
        self.clock = SteppedWallClock()
        patcher = mock.patch("time.time", self.clock)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_timeout_ticker_keeps_its_deadline(self) -> None:
        """A minute's ticker has its minute after the wall clock steps an
        hour forward, where it had run out, and no more after a step back,
        where it had an hour more."""
        ticker = TimeoutTicker(60)
        self.clock.offset = HOUR
        self.assertGreater(ticker(), 30)
        self.assertTrue(ticker.permit())
        self.clock.offset = -HOUR
        self.assertLessEqual(ticker(), 60)

    def test_a_drain_lasts_its_period(self) -> None:
        """A drain of a minute that keeps a path open is not over when the
        wall clock steps an hour forward, in either server class, where the
        step ended it. The period is set as serve_forever(drain_seconds=60)
        sets it, with no multiplexer to serve."""
        with BaseMultiplexerServer([], type=peers.PYTHON_TEST_SERVER, drain_routing=OPEN_PATH) as server:
            server._drain_seconds = 60
            server.start_draining()
            self.clock.offset = HOUR
            self.assertFalse(server.drained())
        self.clock.offset = 0.0
        with BaseThreadedMultiplexerServer([], type=peers.PYTHON_TEST_SERVER, drain_routing=OPEN_PATH) as threaded:
            threaded._drain_seconds = 60
            threaded.start_draining()
            self.clock.offset = HOUR
            self.assertFalse(threaded.drained())

    def test_a_harness_wait_keeps_its_deadline(self) -> None:
        """wait_until() with a minute to go looks again after the wall clock
        steps an hour forward, at its first look, and returns what the third
        look finds, where the step timed it out."""
        looks: list[int] = []

        def third_look() -> bool:
            """True at the third look; the wall clock steps at the first."""
            looks.append(len(looks))
            self.clock.offset = HOUR
            return len(looks) >= 3

        self.assertTrue(wait_until(third_look, 60, "the third look", interval=0))
        self.assertEqual(3, len(looks))


if __name__ == "__main__":
    unittest.main()
