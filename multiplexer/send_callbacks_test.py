"""A send's callback, the same in every client and with every send: called
once, with 1 once the message was written, the first copy for ALL, or 0
once it was given up on or the client shut down first, where the
synchronous client took no callback and the threaded one ignored a
callback given without flush=True. The synchronous client calls it inside
a call that runs the loop, the threaded one on its io thread, and
flush_all() returns once the callbacks of what it waited for have run.
"""

import gc
import threading
import unittest
from typing import Any

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import DropReason, Lane
from multiplexer.testing import Cluster
from multiplexer.testing import runfile
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST
CLIENTS = ("SyncClient", "ThreadedClient")


class Heard:
    """What a callback heard, in order: its one argument, or a tuple of
    them; from any thread."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.heard: list[Any] = []

    def __call__(self, *arguments: Any) -> None:
        with self.lock:
            self.heard.append(arguments[0] if len(arguments) == 1 else arguments)

    def now(self) -> list[Any]:
        with self.lock:
            return list(self.heard)


def make_client(name: str, cluster: Cluster, drops: Heard) -> "Client | ThreadedClient":
    """The client `name` on `cluster`, reporting to `drops`."""
    if name == "SyncClient":
        return Client(cluster.endpoints, type=peers.WEBSITE, on_drop=drops)
    return ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT, on_drop=drops)


def destinations(client: "Client | ThreadedClient") -> list[tuple[str, "int | Lane"]]:
    """Every kind of `multiplexer=` a send takes: one connection, every one,
    a lane and a pinned lane."""
    every = Client.ALL if isinstance(client, Client) else ThreadedClient.ALL
    one = Client.ONE if isinstance(client, Client) else ThreadedClient.ONE
    return [("ONE", one), ("ALL", every), ("lane", client.lane()), ("pinned lane", client.lane(pinned=True))]


class SendCallbackTest(unittest.TestCase):
    """Each send heard once, by its own callback."""

    def test_a_written_message_is_heard_with_1_before_flush_all_returns(self) -> None:
        """Every kind of send, flushing or not, through two multiplexers:
        the callback hears 1 once, for ALL too, by the time flush_all()
        returns, and nothing is dropped."""
        with Cluster(2, rules=RULES) as cluster:
            for name in CLIENTS:
                drops = Heard()
                client = make_client(name, cluster, drops)
                try:
                    for flush in (False, True):
                        for where, multiplexer in destinations(client):
                            with self.subTest(client=name, flush=flush, multiplexer=where):
                                heard = Heard()
                                client.send_message(
                                    b"written", type=EVENT, multiplexer=multiplexer, flush=flush, callback=heard
                                )
                                self.assertTrue(client.flush_all(10))
                                self.assertEqual([1], heard.now())
                    self.assertEqual([], drops.now())
                finally:
                    client.shutdown()

    def test_a_message_given_up_on_is_heard_with_0(self) -> None:
        """No multiplexer reachable: the message is held past its timeout,
        dropped and reported, and its callback hears 0, once, by the time
        flush_all() returns."""
        for name in CLIENTS:
            for flush in (False, True):
                with self.subTest(client=name, flush=flush):
                    drops = Heard()
                    with Cluster(1, rules=RULES) as cluster:
                        client = make_client(name, cluster, drops)
                        try:
                            cluster.mx[0].stop()
                            heard = Heard()
                            sent = client.send_message(b"held", type=EVENT, flush=flush, timeout=0.3, callback=heard)
                            client.flush_all(10)  # ends once the message was given up on
                            self.assertEqual([0], heard.now())
                            self.assertEqual([(sent, DropReason.NO_CONNECTION)], drops.now())
                        finally:
                            client.shutdown()

    def test_a_message_left_at_shutdown_is_heard_with_0(self) -> None:
        """No multiplexer reachable and the client shut down before the
        message's timeout: its callback hears 0, once, by the time
        shutdown() returns."""
        for name in CLIENTS:
            for flush in (False, True):
                with self.subTest(client=name, flush=flush):
                    drops = Heard()
                    with Cluster(1, rules=RULES) as cluster:
                        client = make_client(name, cluster, drops)
                        cluster.mx[0].stop()
                        heard = Heard()
                        sent = client.send_message(b"held", type=EVENT, flush=flush, timeout=30, callback=heard)
                        client.shutdown()
                        self.assertEqual([0], heard.now())
                        self.assertEqual([(sent, DropReason.SHUT_DOWN)], drops.now())

    def test_a_callback_may_send(self) -> None:
        """The callback of one send makes another, inside the synchronous
        client's loop or on the threaded client's io thread: the second is
        written and heard in turn."""
        with Cluster(1, rules=RULES) as cluster:
            for name in CLIENTS:
                with self.subTest(client=name):
                    client = make_client(name, cluster, Heard())
                    try:
                        second = Heard()

                        def send_second(written: int, client: Any = client, second: Heard = second) -> None:
                            client.send_message(b"second", type=EVENT, callback=second)

                        client.send_message(b"first", type=EVENT, callback=send_second)
                        self.assertTrue(client.flush_all(10))  # the first heard, so the second sent
                        self.assertTrue(client.flush_all(10))
                        self.assertEqual([1], second.now())
                    finally:
                        client.shutdown()

    def test_a_collected_synchronous_client_calls_back_nothing(self) -> None:
        """A SyncClient dropped with a send still followed is freed calling
        nothing into Python, its callback included, as it tells on_drop
        nothing then."""
        drops = Heard()
        heard = Heard()
        with Cluster(1, rules=RULES) as cluster:
            client = Client(cluster.endpoints, type=peers.WEBSITE, on_drop=drops)
            cluster.mx[0].stop()
            client.send_message(b"held", type=EVENT, timeout=30, callback=heard)
            del client
            gc.collect()
        self.assertEqual([], heard.now())
        self.assertEqual([], drops.now())


if __name__ == "__main__":
    unittest.main()
