"""Every end call writes what was sent before it, within its timeout:
SyncClient.shutdown(), ThreadedClient.shutdown() and AsyncClient.close(),
which the server classes' close() end through, go on writing before the
connections close, where the clients dropped what they had not written
and only the servers flushed first. With timeout=0 what is left is
dropped and reported at once.
"""

import asyncio
import threading
import unittest

from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import DropReason
from multiplexer.testing import Cluster
from multiplexer.testing import runfile
from multiplexer.testing.buffers import past_the_queue
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST
CHUNK = b"x" * (16 * 1024)
CLIENTS = ("SyncClient", "ThreadedClient")


class Drops:
    """What on_drop heard, (message id, reason) in order; from any thread."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.heard: list[tuple[int, DropReason]] = []

    def __call__(self, message_id: int, reason: DropReason) -> None:
        with self.lock:
            self.heard.append((message_id, reason))

    def now(self) -> list[tuple[int, DropReason]]:
        with self.lock:
            return list(self.heard)


def make_client(name: str, cluster: Cluster, drops: Drops) -> "Client | ThreadedClient":
    """The client `name` on `cluster`, reporting to `drops`."""
    if name == "SyncClient":
        return Client(cluster.endpoints, type=peers.WEBSITE, on_drop=drops)
    return ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT, on_drop=drops)


class ShutdownTest(unittest.TestCase):
    """The only multiplexer frozen with the client's connection full, so
    that messages wait in the client when it shuts down."""

    def test_shutdown_writes_what_was_sent_before_it(self) -> None:
        """The multiplexer reads again half a second into the shutdown:
        nothing is dropped."""
        for name in CLIENTS:
            with self.subTest(client=name):
                drops = Drops()
                with Cluster(1, rules=RULES) as cluster:
                    client = make_client(name, cluster, drops)
                    cluster.mx[0].pause()
                    try:
                        for payload in past_the_queue(CHUNK):
                            client.send_message(payload, type=EVENT, timeout=60)
                        resume = threading.Timer(0.5, cluster.mx[0].resume)
                        resume.start()
                        try:
                            client.shutdown(timeout=60)
                        finally:
                            resume.join()
                    finally:
                        cluster.mx[0].resume()
                    self.assertEqual([], drops.now())
                    self.assertEqual(0, client.dropped)

    def test_an_async_close_writes_what_was_sent_before_it(self) -> None:
        """AsyncClient.aclose() the same way, its sends returning once the
        io thread had the messages."""
        drops = Drops()
        with Cluster(1, rules=RULES) as cluster:

            async def send_and_close() -> int:
                client = AsyncClient(cluster.endpoints, peers.TEST_ACTIVE_CLIENT, on_drop=drops)
                cluster.mx[0].pause()
                resume = None
                try:
                    for payload in past_the_queue(CHUNK):
                        await client.send_message(payload, type=EVENT, timeout=60)
                    resume = threading.Timer(0.5, cluster.mx[0].resume)
                    resume.start()
                    await client.aclose(timeout=60)
                finally:
                    if resume is not None:
                        resume.join()
                    cluster.mx[0].resume()
                return client.dropped

            self.assertEqual(0, asyncio.run(send_and_close()))
        self.assertEqual([], drops.now())

    def test_shutdown_with_0_drops_at_once(self) -> None:
        """timeout=0, the multiplexer frozen until the shutdown returned:
        what the client had not written is dropped and reported, each
        message once, as SHUT_DOWN."""
        for name in CLIENTS:
            with self.subTest(client=name):
                drops = Drops()
                with Cluster(1, rules=RULES) as cluster:
                    client = make_client(name, cluster, drops)
                    cluster.mx[0].pause()
                    try:
                        sent = [
                            client.send_message(payload, type=EVENT, timeout=60) for payload in past_the_queue(CHUNK)
                        ]
                        client.shutdown(timeout=0)
                    finally:
                        cluster.mx[0].resume()
                    heard = drops.now()
                    self.assertTrue(heard, "a frozen multiplexer left something to drop")
                    self.assertEqual({DropReason.SHUT_DOWN}, {reason for _, reason in heard})
                    self.assertEqual(len(heard), len({message_id for message_id, _ in heard}))
                    self.assertTrue({message_id for message_id, _ in heard} <= set(sent))
                    self.assertEqual(len(heard), client.dropped)


if __name__ == "__main__":
    unittest.main()
