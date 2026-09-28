"""Sends through ThreadedClient and AsyncClient against real multiplexers:
a flushing send ends when its frame is written; a flushing send to every
connection ends when one copy is written, so a multiplexer frozen with its
socket open does not hold every sender to the timeout; and a message over
the size limit is refused at the call, before anything is queued, where a
query used to wait forever. What waits for room, and that nothing polls,
threaded_client_test.cc checks by counting."""

import asyncio
import contextlib
import time
import unittest
from typing import Iterator

from multiplexer._native import MAX_MESSAGE_SIZE
from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster
from multiplexer.testing import runfile
from multiplexer.testing.buffers import fill_frames
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST


class FlushingSendTest(unittest.TestCase):
    """A flushing send's end."""

    def test_a_flushing_send_ends_when_written(self):
        """A hundred flushing sends in a row, each returning once written."""
        with Cluster(1, rules=RULES) as cluster:
            client = ThreadedClient(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
            try:
                for index in range(100):
                    client.send_message(b"%d" % index, type=EVENT, flush=True, timeout=5)
            finally:
                client.shutdown()

    def test_a_frozen_multiplexer_does_not_hold_a_send_to_every_connection(self):
        """One of two multiplexers frozen with its socket open: once its
        socket buffer is full, a flushing send through ALL still ends as
        soon as the other copy is written, rather than at its timeout, so
        all of them together take less than one send's timeout."""
        with Cluster(2, rules=RULES) as cluster:
            client = ThreadedClient(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
            try:
                cluster.mx[1].pause()
                started = time.monotonic()
                for payload in fill_frames():  # more than the frozen one's sockets take
                    client.send_message(payload, type=EVENT, multiplexer=ThreadedClient.ALL, flush=True, timeout=30)
                self.assertLess(time.monotonic() - started, 30, "no send waited for its timeout")
            finally:
                cluster.mx[1].resume()
                client.shutdown()


class TooLargeTest(unittest.TestCase):
    """A message over MAX_MESSAGE_SIZE, refused where it is sent."""

    too_large = b"x" * (MAX_MESSAGE_SIZE + 1)
    # The timeout every call is given. A call that waited for it before
    # refusing took all of it; one refused at the call takes a small part
    # of it on any machine, building and measuring the message included,
    # so half of it tells the two apart where a bound of its own, in
    # seconds, would time the machine.
    timeout = 120.0

    @contextlib.contextmanager
    def refused_at_the_call(self) -> Iterator[None]:
        """The block, one call given `timeout`, raises ValueError in less
        than half of it."""
        started = time.monotonic()
        with self.assertRaises(ValueError):
            yield
        self.assertLess(time.monotonic() - started, self.timeout / 2, "at the call, not after its timeout")

    def test_every_client_refuses_it_at_the_call(self):
        """Each client's queries and sends, flushing or not, raise
        ValueError, each before half its timeout is over."""
        with Cluster(1, rules=RULES) as cluster:
            threaded = ThreadedClient(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
            synchronous = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                with self.refused_at_the_call():
                    threaded.query(self.too_large, EVENT, timeout=self.timeout)
                with self.refused_at_the_call():
                    threaded.send_message(self.too_large, type=EVENT, timeout=self.timeout)
                with self.refused_at_the_call():
                    threaded.send_message(self.too_large, type=EVENT, flush=True, timeout=self.timeout)
                with self.refused_at_the_call():
                    synchronous.send_message(self.too_large, type=EVENT, timeout=self.timeout)
                with self.refused_at_the_call():
                    synchronous.query(self.too_large, EVENT, timeout=self.timeout)

                async def through_asyncio() -> None:
                    client = AsyncClient(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
                    try:
                        with self.refused_at_the_call():
                            await client.query(self.too_large, EVENT, timeout=self.timeout)
                        with self.refused_at_the_call():
                            await client.send_message(self.too_large, type=EVENT, timeout=self.timeout)
                    finally:
                        await client.aclose()

                asyncio.run(through_asyncio())
            finally:
                threaded.shutdown()
                synchronous.shutdown()


if __name__ == "__main__":
    unittest.main()
