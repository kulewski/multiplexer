"""Sends through ThreadedClient and AsyncClient against real multiplexers:
a flushing send ends when its frame is written; a flushing send to every
connection ends when one copy is written, so a multiplexer frozen with its
socket open does not hold every sender to the timeout; and a message over
the size limit is refused at the call, before anything is queued, where a
query used to wait forever. What waits for room, and that nothing polls,
threaded_client_test.cc checks by counting."""

import asyncio
import time
import unittest

from multiplexer._native import MAX_MESSAGE_SIZE
from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster
from multiplexer.testing import runfile
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
                chunk = b"x" * (256 * 1024)
                started = time.monotonic()
                for index in range(60):  # 15 MB: more than the frozen one's socket takes
                    client.send_message(chunk, type=EVENT, multiplexer=ThreadedClient.ALL, flush=True, timeout=30)
                self.assertLess(time.monotonic() - started, 30, "no send waited for its timeout")
            finally:
                cluster.mx[1].resume()
                client.shutdown()


class TooLargeTest(unittest.TestCase):
    """A message over MAX_MESSAGE_SIZE, refused where it is sent."""

    too_large = b"x" * (MAX_MESSAGE_SIZE + 1)

    def test_every_client_refuses_it_at_the_call(self):
        with Cluster(1, rules=RULES) as cluster:
            threaded = ThreadedClient(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
            synchronous = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                started = time.monotonic()
                with self.assertRaises(ValueError):
                    threaded.query(self.too_large, EVENT, timeout=5)
                with self.assertRaises(ValueError):
                    threaded.send_message(self.too_large, type=EVENT)
                with self.assertRaises(ValueError):
                    threaded.send_message(self.too_large, type=EVENT, flush=True)
                with self.assertRaises(ValueError):
                    synchronous.send_message(self.too_large, type=EVENT)
                with self.assertRaises(ValueError):
                    synchronous.query(self.too_large, EVENT, timeout=5)

                async def through_asyncio() -> None:
                    client = AsyncClient(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
                    try:
                        with self.assertRaises(ValueError):
                            await client.query(self.too_large, EVENT, timeout=5)
                        with self.assertRaises(ValueError):
                            await client.send_message(self.too_large, type=EVENT)
                    finally:
                        await client.aclose()

                asyncio.run(through_asyncio())
                self.assertLess(time.monotonic() - started, 5, "at the call, not after a timeout")
            finally:
                threaded.shutdown()
                synchronous.shutdown()


if __name__ == "__main__":
    unittest.main()
