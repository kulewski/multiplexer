"""Addressed queries, lanes and pinning through ThreadedClient and
AsyncClient, against two real multiplexers and scripted peers: the same
promises multiplexer/testing/lanes_test.py checks for the synchronous
client, through the io thread and through the event loop.
"""

import asyncio
import random
import threading
import time
import unittest

from multiplexer.aio import AsyncClient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import NotConnected, OperationFailed, OperationTimedOut
from multiplexer.testing import Cluster, FakePeer
from multiplexer.threaded_client import ThreadedClient

REQUEST = types.PYTHON_TEST_REQUEST
RESPONSE = types.PYTHON_TEST_RESPONSE


def answer(mxmsg):
    """The fakes' script: the payload upper-cased, after a pause when it says so."""
    if mxmsg.message.startswith(b"slow"):
        time.sleep(1.0)
    return mxmsg.message.upper()


class ThreadedClientLanesTest(unittest.TestCase):
    """ThreadedClient: a cluster of two per test, since several tests kill one."""

    def setUp(self):
        self.cluster = Cluster(2).__enter__()
        self.peer = FakePeer(self.cluster, peers.PYTHON_TEST_SERVER, name="first").start()
        self.other = FakePeer(self.cluster, peers.PYTHON_TEST_SERVER, name="second").start()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER, count=2)
        for peer in (self.peer, self.other):
            peer.on(REQUEST, answer, RESPONSE)
        self.client = ThreadedClient(self.cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)

    def tearDown(self):
        self.client.shutdown()
        self.peer.stop()
        self.other.stop()
        self.cluster.__exit__(None, None, None)

    def test_only_the_addressee_gets_an_addressed_query(self):
        """Blocking and callback forms, both probes; the other fake of the type sees nothing."""
        for index in range(6):
            probe = types.PING if index % 2 else types.BACKEND_FOR_PACKET_SEARCH
            reply = self.client.query(b"n%d" % index, REQUEST, to=self.peer.instance_id, probe=probe)
            self.assertEqual((b"N%d" % index, self.peer.instance_id), (reply.message, reply.from_))
        results = []
        done = threading.Semaphore(0)

        def collect(result):
            results.append(result)
            done.release()

        self.client.query(b"cb", REQUEST, to=self.peer.instance_id, callback=collect, with_connection=True)
        self.assertTrue(done.acquire(timeout=10))
        reply, connection = results[0]
        self.assertEqual(b"CB", reply.message)
        self.assertIs(self.cluster.multiplexer_at(connection.endpoint), self.peer.via(self.peer.messages(REQUEST)[-1]))
        self.assertEqual([], self.other.received)

    def test_an_instance_that_left_is_a_failure_not_a_detour(self):
        gone = random.getrandbits(63) | 1
        started = time.monotonic()
        with self.assertRaises(OperationFailed):
            self.client.query(b"lost", REQUEST, to=gone, timeout=10)
        self.assertLess(time.monotonic() - started, 3)
        self.assertEqual([], self.peer.received)
        self.assertEqual([], self.other.received)

    def test_a_multiplexer_dying_under_the_wait_costs_the_query_nothing(self):
        """The lane says which multiplexer carried the request; kill it while
        the fake sleeps: the reply comes through the other. The fake saw the
        request once or twice: the client locates the addressee and sends
        again, but the fake's reply to the first request, going back through
        the other multiplexer, may end the query first."""
        lane = self.client.lane()
        self.client.query(b"warm-up", REQUEST, to=self.peer.instance_id, multiplexer=lane)
        victim = self.cluster.multiplexer_at(lane.connection.endpoint)
        killer = threading.Timer(0.3, victim.kill)
        killer.start()
        started = time.monotonic()
        reply = self.client.query(b"slow", REQUEST, to=self.peer.instance_id, multiplexer=lane, timeout=10)
        killer.join()
        self.assertEqual(b"SLOW", reply.message)
        self.assertLess(time.monotonic() - started, 6)
        arrivals = self.peer.arrivals(REQUEST, matching=lambda m: m.message == b"slow")
        self.assertEqual([victim], [via for _, via in arrivals[:1]])
        self.assertIn(len(arrivals), (1, 2))
        self.assertTrue(all(via is not victim for _, via in arrivals[1:]))
        self.assertIsNot(victim, self.cluster.multiplexer_at(lane.connection.endpoint), "the lane followed")

    def test_a_lane_keeps_a_stream_on_one_multiplexer_and_follows_a_failover(self):
        lane = self.client.lane()
        for index in range(200):
            self.client.send_message(b"lane-%d" % index, type=REQUEST, multiplexer=lane)
        chunks = self._all_chunks(200, lambda m: m.message.startswith(b"lane-"))
        self.assertEqual([b"lane-%d" % index for index in range(200)], [m.message for m in chunks], "in order")
        ways = {self.via(m) for m in chunks}
        self.assertEqual(1, len(ways), "all the same way")
        (way,) = ways
        self.assertIs(way, self.cluster.multiplexer_at(lane.connection.endpoint))
        way.kill()
        for index in range(200, 400):
            self.client.send_message(b"lane-%d" % index, type=REQUEST, multiplexer=lane, flush=True, timeout=10)
        # The gap at the failover: a chunk written into the killed
        # multiplexer's socket before its closure was noticed is lost. The
        # rest arrives in order, the same way, and the lane moved.
        self._all_chunks(1, lambda m: m.message == b"lane-399")
        rest = self._all_chunks(1, lambda m: m.message.startswith(b"lane-") and int(m.message[5:]) >= 200)
        self.assertGreaterEqual(len(rest), 198, "more than the failover gap lost")
        self.assertEqual(sorted(rest, key=lambda m: int(m.message[5:])), rest, "in order after the gap")
        self.assertEqual(1, len({self.via(m) for m in rest}))
        self.assertIsNot(way, self.via(rest[0]))
        self.assertIs(self.via(rest[-1]), self.cluster.multiplexer_at(lane.connection.endpoint), "the lane moved")

    def _all_chunks(self, count, matching):
        """The messages both fakes received that `matching` selects, in
        arrival order, once `count` of them came; the rules spread
        PYTHON_TEST_REQUEST round robin over the two fakes."""
        deadline = time.monotonic() + 15
        while True:
            found = self.peer.messages(REQUEST, matching) + self.other.messages(REQUEST, matching)
            found.sort(key=lambda m: (self.via(m).index, int(m.message.split(b"-")[1])))  # arrival order per way
            if len(found) >= count:
                return found
            self.assertLess(time.monotonic(), deadline, "only %d of %d chunks arrived" % (len(found), count))
            time.sleep(0.02)

    def via(self, mxmsg):
        """Which multiplexer a message came through, whichever fake got it."""
        try:
            return self.peer.via(mxmsg)
        except KeyError:
            return self.other.via(mxmsg)

    def test_a_pinned_lane_fails_instead_of_following(self):
        lane = self.client.lane(pinned=True)
        for index in range(100):
            self.client.send_message(b"pin-%d" % index, type=REQUEST, multiplexer=lane, flush=True)
        chunks = self._all_chunks(100, lambda m: m.message.startswith(b"pin-"))
        ways = {self.via(m) for m in chunks}
        self.assertEqual(1, len(ways))
        (way,) = ways
        way.kill()
        with self.assertRaises(NotConnected):
            for index in range(100, 200):
                self.client.send_message(b"pin-%d" % index, type=REQUEST, multiplexer=lane, flush=True, timeout=5)
        self.assertTrue(lane.closed)
        with self.assertRaises(NotConnected):
            self.client.query(b"pinned", REQUEST, to=self.peer.instance_id, multiplexer=lane, timeout=5)
        with self.assertRaises(NotConnected):
            self.client.send_message(b"queued", type=REQUEST, multiplexer=lane)
        self.assertEqual({way}, {self.via(m) for m in self._all_chunks(1, lambda m: m.message.startswith(b"pin-"))})

    def test_a_typed_query_leaves_the_lane_where_the_answer_came_from(self):
        lane = self.client.lane()
        reply, connection = self.client.query(b"first", REQUEST, multiplexer=lane, with_connection=True)
        self.assertEqual(b"FIRST", reply.message)
        self.assertEqual(connection.endpoint, lane.connection.endpoint)
        for index in range(50):
            self.client.send_message(b"then-%d" % index, type=REQUEST, multiplexer=lane)
        chunks = self._all_chunks(50, lambda m: m.message.startswith(b"then-"))
        way = self.cluster.multiplexer_at(connection.endpoint)
        self.assertTrue(all(self.via(m) is way for m in chunks), "the sends followed the query")

    def test_a_connection_is_preferred_and_a_pinned_lane_seeded_with_it_is_kept(self):
        reply, connection = self.client.query(b"first", REQUEST, with_connection=True)
        way = self.cluster.multiplexer_at(connection.endpoint)
        pinned = self.client.lane(pinned=True, connection=connection)
        self.client.send_message(b"pin-1", type=REQUEST, multiplexer=pinned, flush=True)
        self.client.send_message(b"pin-2", type=REQUEST, multiplexer=connection, flush=True)
        self.assertEqual(b"PIN-3", self.client.query(b"pin-3", REQUEST, multiplexer=pinned).message)
        self.assertEqual(b"PIN-4", self.client.query(b"pin-4", REQUEST, multiplexer=connection).message)
        chunks = self._all_chunks(4, lambda m: m.message.startswith(b"pin-"))
        self.assertTrue(all(self.via(m) is way for m in chunks))
        way.kill()
        with self.assertRaises(NotConnected):
            self.client.send_message(b"pin-5", type=REQUEST, multiplexer=pinned, flush=True, timeout=5)
        with self.assertRaises(NotConnected):
            self.client.query(b"pin-6", REQUEST, multiplexer=pinned, timeout=5)
        self.client.send_message(b"pin-7", type=REQUEST, multiplexer=connection, flush=True, timeout=10)
        self.assertEqual(b"PIN-8", self.client.query(b"pin-8", REQUEST, multiplexer=connection, timeout=10).message)
        later = self._all_chunks(2, lambda m: m.message in (b"pin-7", b"pin-8"))
        self.assertTrue(all(self.via(m) is not way for m in later), "the bare connection fell back")

    def test_a_lane_outlives_nothing_and_holds_nothing(self):
        """A lane dropped mid-query costs nothing; a lane kept after the
        client shut down holds no connection alive and reports closed."""
        import gc
        import weakref

        lane = self.client.lane(pinned=True)
        results = []
        done = threading.Semaphore(0)
        self.client.query(
            b"slow",
            REQUEST,
            to=self.peer.instance_id,
            multiplexer=lane,
            callback=lambda r: (results.append(r), done.release()),
        )
        weak_lane = weakref.ref(lane)
        del lane
        self.assertTrue(done.acquire(timeout=10))
        self.assertEqual(b"SLOW", results[0].message)
        gc.collect()
        self.assertIsNone(weak_lane(), "the query released the lane")
        kept = self.client.lane()
        self.client.send_message(b"kept", type=REQUEST, multiplexer=kept, flush=True)
        self.assertTrue(kept.connected)
        self.client.shutdown()
        self.assertFalse(kept.connected, "the lane holds the connection weakly")
        with self.assertRaises(NotConnected):
            self.client.send_message(b"after", type=REQUEST, multiplexer=kept, flush=True)


class AsyncClientLanesTest(unittest.IsolatedAsyncioTestCase):
    """AsyncClient: the same surface, awaited."""

    async def asyncSetUp(self):
        self.cluster = Cluster(2).__enter__()
        self.peer = FakePeer(self.cluster, peers.PYTHON_TEST_SERVER, name="first").start()
        self.other = FakePeer(self.cluster, peers.PYTHON_TEST_SERVER, name="second").start()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER, count=2)
        for peer in (self.peer, self.other):
            peer.on(REQUEST, answer, RESPONSE)
        self.client = AsyncClient(self.cluster.endpoints, peers.PYTHON_TEST_CLIENT)

    async def asyncTearDown(self):
        await self.client.aclose()
        self.peer.stop()
        self.other.stop()
        self.cluster.__exit__(None, None, None)

    async def test_addressed_queries_and_lanes(self):
        reply = await self.client.query(b"one", REQUEST, to=self.peer.instance_id)
        self.assertEqual((b"ONE", self.peer.instance_id), (reply.message, reply.from_))
        reply, connection = await self.client.query(
            b"two", REQUEST, to=self.peer.instance_id, probe=types.PING, with_connection=True
        )
        self.assertEqual(b"TWO", reply.message)
        self.assertIs(self.cluster.multiplexer_at(connection.endpoint), self.peer.via(self.peer.messages(REQUEST)[-1]))
        with self.assertRaises(OperationFailed):
            await self.client.query(b"lost", REQUEST, to=random.getrandbits(63) | 1, timeout=10)
        self.assertEqual([], self.other.received)
        lane = self.client.lane()
        await asyncio.gather(
            *(self.client.send_message(b"lane-%d" % index, type=REQUEST, multiplexer=lane) for index in range(50))
        )
        chunks = self.peer.messages(REQUEST, lambda m: m.message.startswith(b"lane-")) + self.other.messages(
            REQUEST, lambda m: m.message.startswith(b"lane-")
        )
        deadline = time.monotonic() + 10
        while len(chunks) < 50:
            self.assertLess(time.monotonic(), deadline)
            await asyncio.sleep(0.02)
            chunks = self.peer.messages(REQUEST, lambda m: m.message.startswith(b"lane-")) + self.other.messages(
                REQUEST, lambda m: m.message.startswith(b"lane-")
            )
        ways = set()
        for mxmsg in chunks:
            try:
                ways.add(self.peer.via(mxmsg))
            except KeyError:
                ways.add(self.other.via(mxmsg))
        self.assertEqual({self.cluster.multiplexer_at(lane.connection.endpoint)}, ways)
        reply = await self.client.query(b"on-the-lane", REQUEST, to=self.peer.instance_id, multiplexer=lane)
        self.assertEqual(b"ON-THE-LANE", reply.message)
        self.assertIn(self.peer.via(self.peer.messages(REQUEST)[-1]), ways)

    async def test_a_pinned_lane_fails_once_its_multiplexer_is_gone(self):
        pinned = self.client.lane(pinned=True)
        await self.client.send_message(b"first", type=REQUEST, multiplexer=pinned)
        way = self.cluster.multiplexer_at(pinned.connection.endpoint)
        way.kill()
        with self.assertRaises(NotConnected):
            for _ in range(20):
                await self.client.send_message(b"more", type=REQUEST, multiplexer=pinned, timeout=5)
        with self.assertRaises(NotConnected):
            await self.client.query(b"more", REQUEST, to=self.peer.instance_id, multiplexer=pinned, timeout=5)
        reply = await self.client.query(b"still", REQUEST, to=self.peer.instance_id, timeout=10)
        self.assertEqual(b"STILL", reply.message)


if __name__ == "__main__":
    unittest.main()
