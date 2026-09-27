"""A query's on_received hears the instance id of the backend that
acknowledged the request with notify_start(): on every Python client, once
and before the reply, on the thread each one calls back on; and again, with
the other backend's id, when a retry reached another backend, which tells
the caller the request may be running twice.
"""

import asyncio
import threading
import unittest
from typing import Any, Callable

from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, FakePeer, runfile
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

REQUEST = types.PYTHON_TEST_REQUEST
RESPONSE = types.PYTHON_TEST_RESPONSE


def acknowledging(peer: FakePeer, answer: bool) -> Callable[[MultiplexerMessage], Any]:
    """A handler for `peer` that acknowledges the request first, as a long
    handler does, then answers with its payload in capitals, or never
    answers."""

    def handle(mxmsg: MultiplexerMessage) -> Any:
        assert peer.backend is not None
        peer.backend.notify_start()
        return mxmsg.message.upper() if answer else None

    return handle


class Told:
    """What the callbacks told, in order, each with the thread it ran on; from any thread."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.heard: list[tuple[Any, int]] = []

    def __call__(self, what: Any) -> None:
        with self.lock:
            self.heard.append((what, threading.get_ident()))

    def now(self) -> list[tuple[Any, int]]:
        with self.lock:
            return list(self.heard)


class OnReceivedTest(unittest.TestCase):
    def test_every_client_hears_the_backend_once_before_the_reply(self) -> None:
        """SyncClient on the calling thread inside query(), ThreadedClient
        on its io thread in both forms, AsyncClient on the loop, before the
        await resumes."""
        caller = threading.get_ident()
        with Cluster(1, rules=RULES) as cluster:
            backend = FakePeer(cluster, peers.PYTHON_TEST_SERVER)
            backend.on(REQUEST, acknowledging(backend, answer=True), RESPONSE)
            with backend:
                acknowledged = backend.instance_id

                with self.subTest(client="SyncClient"):
                    told = Told()
                    with Client(cluster.endpoints, type=peers.WEBSITE) as client:
                        reply = client.query(b"sync", REQUEST, on_received=told)
                    self.assertEqual(b"SYNC", reply.message)
                    self.assertEqual([(acknowledged, caller)], told.now())

                with self.subTest(client="ThreadedClient"):
                    told = Told()
                    answered = threading.Event()
                    with ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT) as client:
                        reply = client.query(b"blocking", REQUEST, on_received=told)
                        told("returned")

                        def on_reply(result: Any) -> None:
                            told(result.message)
                            answered.set()

                        client.query(b"callback", REQUEST, callback=on_reply, on_received=told)
                        self.assertTrue(answered.wait(10))
                    self.assertEqual(b"BLOCKING", reply.message)
                    heard = told.now()
                    self.assertEqual([acknowledged, "returned", acknowledged, b"CALLBACK"], [what for what, _ in heard])
                    io_thread = heard[0][1]
                    self.assertNotEqual(caller, io_thread)
                    self.assertEqual([io_thread, caller, io_thread, io_thread], [thread for _, thread in heard])

                with self.subTest(client="AsyncClient"):
                    told = Told()

                    async def ask() -> MultiplexerMessage:
                        async with AsyncClient(cluster.endpoints, peers.TEST_ACTIVE_CLIENT) as client:
                            reply = await client.query(b"async", REQUEST, on_received=told)
                            told("resumed")
                            return reply

                    self.assertEqual(b"ASYNC", asyncio.run(ask()).message)
                    self.assertEqual([(acknowledged, caller), ("resumed", caller)], told.now())

    def test_a_retry_that_reached_another_backend_is_heard_too(self) -> None:
        """SyncClient, whose query is Python's own: the request goes
        through the first multiplexer, whose backend acknowledges it and
        never answers; the callback takes that multiplexer away, and the
        request goes again through the second, whose backend acknowledges
        and answers."""
        with Cluster(2, rules=RULES) as cluster:
            first, second = cluster.endpoints
            holding = FakePeer(cluster, peers.PYTHON_TEST_SERVER, name="holding", endpoints=[first])
            holding.on(REQUEST, acknowledging(holding, answer=False), RESPONSE)
            answering = FakePeer(cluster, peers.PYTHON_TEST_SERVER, name="answering", endpoints=[second])
            answering.on(REQUEST, acknowledging(answering, answer=True), RESPONSE)
            with holding, answering, Client([], type=peers.WEBSITE) as client:
                through_first = client.connect(first)
                client.connect(second)
                heard: list[int] = []

                def on_received(backend: int) -> None:
                    heard.append(backend)
                    if len(heard) == 1:
                        cluster.mx[0].kill()

                reply = client.query(b"question", REQUEST, multiplexer=through_first, on_received=on_received)
                self.assertEqual(b"QUESTION", reply.message)
                self.assertEqual([holding.instance_id, answering.instance_id], heard)


if __name__ == "__main__":
    unittest.main()
