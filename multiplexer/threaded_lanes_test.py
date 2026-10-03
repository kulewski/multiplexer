"""Addressed queries, lanes and pinning through ThreadedClient and
AsyncClient, against two real multiplexers and scripted peers: the same
promises multiplexer/testing/lanes_test.py checks for the synchronous
client, through the io thread and through the event loop. And what a
dying connection had not written, which goes, in order, to one other
multiplexer, the lane following it, for the synchronous client too.

Ordered by events and counts, never by a pause: a multiplexer is killed
once a fake holds the request it carried, and what was handed over is
looked at once it has all arrived and a marker behind it has too. A query
that must not wait for a timeout gets a long one and must end before
half of it is over.
"""

import asyncio
import random
import threading
import time
import unittest

from multiplexer.aio import AsyncClient
from multiplexer.clients import SyncClient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import NotConnected, OperationFailed
from multiplexer.testing import Cluster, FakePeer, wait_until
from multiplexer.testing.buffers import fill_frames
from multiplexer.threaded_client import ThreadedClient
from multiplexer.testing import runfile

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

REQUEST = types.PYTHON_TEST_REQUEST
RESPONSE = types.PYTHON_TEST_RESPONSE

# The timeout of a query that must not wait for one to run out, and the
# bound of a wait only a failure reaches. A query that waited for its stage
# to run out took all of it; one that went on at once takes a small part
# of it on any machine, so half of it tells the two apart.
LONG_TIMEOUT = 60.0
# The messages a test leaves unwritten behind a frozen multiplexer, after
# fill_frames() has filled its connection's sockets.
STRANDED = 300


def answer(mxmsg):
    """The fakes' script: the payload upper-cased."""
    return mxmsg.message.upper()


def hold_the_first_slow(*fakes, answered=True):
    """Script `fakes` so that the first b"slow" any of them gets is held
    until the test sets `released`, `holding` set once it is, and then
    answered as by `answer` or, not `answered`, never, as a request lost
    with its multiplexer is; every other message, a b"slow" sent again
    included, is answered at once. Returns (holding, released, held),
    `held` the list that receives the held request's id. The test acts
    while the request is held, kill_when_held() killing its multiplexer,
    however long a loaded machine takes to get there: no pause guesses it."""
    lock = threading.Lock()
    holding, released = threading.Event(), threading.Event()
    held = []

    def script(mxmsg):
        """Hold the first b"slow"; `answer` the rest at once."""
        with lock:
            holds = mxmsg.message == b"slow" and not held
            if holds:
                held.append(mxmsg.id)
        if not holds:
            return answer(mxmsg)
        holding.set()
        released.wait(LONG_TIMEOUT)
        return answer(mxmsg) if answered else None

    for fake in fakes:
        fake.on(REQUEST, script, RESPONSE)
    return holding, released, held


def kill_when_held(victim, holding, released):
    """`victim` killed once a fake holds the request, which it then lets
    go: kill() returns once the process is gone, so whatever the fake does
    with the request finds that connection closed."""
    holding.wait(LONG_TIMEOUT)
    victim.kill()
    released.set()


class ThreadedClientLanesTest(unittest.TestCase):
    """ThreadedClient: a cluster of two per test, since several tests kill one."""

    def setUp(self):
        self.cluster = Cluster(2, rules=RULES).__enter__()
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
        """Blocking and callback forms; the other fake of the type sees nothing."""
        for index in range(6):
            reply = self.client.query(b"n%d" % index, REQUEST, to=self.peer.instance_id)
            self.assertEqual((b"N%d" % index, self.peer.instance_id), (reply.message, reply.sender))
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
        """OperationFailed, which the delivery errors bring, well before
        the timeout, which would bring OperationTimedOut; no fake of the
        type sees the request."""
        gone = random.getrandbits(63) | 1
        started = time.monotonic()
        with self.assertRaises(OperationFailed):
            self.client.query(b"lost", REQUEST, to=gone, timeout=LONG_TIMEOUT)
        self.assertLess(time.monotonic() - started, LONG_TIMEOUT / 2, "the delivery errors, not a timeout")
        self.assertEqual([], self.peer.received)
        self.assertEqual([], self.other.received)

    def test_an_addressee_that_declines_searches_is_located(self):
        """A backend that declines every search, as a saturated one does,
        behind the second multiplexer only, and a lane on the first: the
        addressed request meets a delivery error there, the client locates
        the backend with a PING, which it answers whatever its search
        policy, and the reply comes through the second."""
        declining = FakePeer(
            self.cluster, peers.PYTHON_TEST_SERVER, name="declining", endpoints=[self.cluster.mx[1].endpoint]
        ).start()
        declining.on(REQUEST, answer, RESPONSE)
        declining.declining_searches = True
        client = ThreadedClient([self.cluster.mx[0].endpoint], type=peers.PYTHON_TEST_CLIENT)
        try:
            lane = client.lane()
            client.query(b"warm-up", REQUEST, multiplexer=lane)  # the lane takes the only connection, the first's
            self.assertTrue(client.connect(self.cluster.mx[1].endpoint))
            reply, connection = client.query(
                b"saturated", REQUEST, to=declining.instance_id, multiplexer=lane, with_connection=True
            )
            self.assertEqual(b"SATURATED", reply.message)
            self.assertIs(self.cluster.mx[1], self.cluster.multiplexer_at(connection.endpoint))
            self.assertEqual([b"saturated"], [m.message for m in declining.messages(REQUEST)])
        finally:
            client.shutdown()
            declining.stop()

    def test_a_multiplexer_dying_under_the_wait_costs_the_query_nothing(self):
        """The lane says which multiplexer carried the request; kill it while
        the fake holds the request: the reply comes through the other,
        well before the query's timeout. The fake saw the request once or
        twice: the client locates the addressee and sends again, but the
        fake's reply to the first request, going back through the other
        multiplexer, may end the query first."""
        holding, released, _ = hold_the_first_slow(self.peer)
        lane = self.client.lane()
        self.client.query(b"warm-up", REQUEST, to=self.peer.instance_id, multiplexer=lane)
        victim = self.cluster.multiplexer_at(lane.connection.endpoint)
        killer = threading.Thread(target=kill_when_held, args=(victim, holding, released))
        killer.start()
        started = time.monotonic()
        reply = self.client.query(b"slow", REQUEST, to=self.peer.instance_id, multiplexer=lane, timeout=LONG_TIMEOUT)
        killer.join()
        self.assertEqual(b"SLOW", reply.message)
        self.assertLess(time.monotonic() - started, LONG_TIMEOUT / 2, "the locate phase, not a timeout")
        arrivals = self.peer.arrivals(REQUEST, matching=lambda m: m.message == b"slow")
        self.assertEqual([victim], [via for _, via in arrivals[:1]])
        self.assertIn(len(arrivals), (1, 2))
        self.assertTrue(all(via is not victim for _, via in arrivals[1:]))
        self.assertIsNot(victim, self.cluster.multiplexer_at(lane.connection.endpoint), "the lane followed")

    def test_a_typed_query_is_sent_again_when_the_lane_s_multiplexer_dies(self):
        """The same for a typed query, one with no addressee: the fakes never
        answer the first request, held until its multiplexer is killed and
        then dropped, as a request lost with its multiplexer is, so only the
        resend through the other multiplexer can end the query, well
        before its first stage runs out, which takes the whole timeout; the
        lane followed, and the client is back on the killed multiplexer once
        it returns. The resend used to go through the dying connection,
        which the lane still held, and the failed assertion in it left the
        client without a reconnect."""
        holding, released, lost = hold_the_first_slow(self.peer, self.other, answered=False)
        lane = self.client.lane()
        self.client.query(b"warm-up", REQUEST, multiplexer=lane)
        victim = self.cluster.multiplexer_at(lane.connection.endpoint)
        killer = threading.Thread(target=kill_when_held, args=(victim, holding, released))
        killer.start()
        started = time.monotonic()
        reply = self.client.query(b"slow", REQUEST, multiplexer=lane, timeout=LONG_TIMEOUT)
        killer.join()
        self.assertEqual(b"SLOW", reply.message)
        self.assertNotIn(reply.references, lost, "the resend's answer")
        self.assertLess(
            time.monotonic() - started, LONG_TIMEOUT / 2, "sent again at once, not after a reconnect or a search"
        )
        self.assertIsNot(victim, self.cluster.multiplexer_at(lane.connection.endpoint), "the lane followed")
        victim.start()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_CLIENT)  # the client reconnected to it

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
        self._markers_through(lane)
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

    def _markers_through(self, lane):
        """A marker to each fake through `lane`, once both arrived: the
        multiplexer forwards to each fake in the order it received, so
        each has every chunk the lane sent it before, where one fake's
        latest chunk says nothing of the other's, which come through a
        connection of their own."""
        for fake in (self.peer, self.other):
            self.client.send_message(b"marker", type=RESPONSE, to=fake.instance_id, multiplexer=lane, flush=True)
        for fake in (self.peer, self.other):
            fake.wait_for(RESPONSE, matching=lambda mxmsg: mxmsg.message == b"marker")

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
        # A send the client writes into the killed multiplexer's socket
        # before it notices the closure goes nowhere; from then on the
        # pinned lane refuses every one.
        with self.assertRaises(NotConnected):
            for _ in range(100):
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

        _, released, _ = hold_the_first_slow(self.peer)
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
        released.set()  # answered only now, the lane dropped while the query waited
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
        self.cluster = Cluster(2, rules=RULES).__enter__()
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
        self.assertEqual((b"ONE", self.peer.instance_id), (reply.message, reply.sender))
        reply, connection = await self.client.query(b"two", REQUEST, to=self.peer.instance_id, with_connection=True)
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

    async def test_a_typed_query_is_sent_again_when_the_lane_s_multiplexer_dies(self):
        """As for ThreadedClient: only the resend can answer, it does, well
        before the timeout, the lane moved, the client is back once the
        killed multiplexer returns."""
        holding, released, lost = hold_the_first_slow(self.peer, self.other, answered=False)
        lane = self.client.lane()
        await self.client.query(b"warm-up", REQUEST, multiplexer=lane)
        victim = self.cluster.multiplexer_at(lane.connection.endpoint)
        killer = threading.Thread(target=kill_when_held, args=(victim, holding, released))
        killer.start()
        started = time.monotonic()
        reply = await self.client.query(b"slow", REQUEST, multiplexer=lane, timeout=LONG_TIMEOUT)
        killer.join()
        self.assertEqual(b"SLOW", reply.message)
        self.assertNotIn(reply.references, lost, "the resend's answer")
        self.assertLess(
            time.monotonic() - started, LONG_TIMEOUT / 2, "sent again at once, not after a reconnect or a search"
        )
        self.assertIsNot(victim, self.cluster.multiplexer_at(lane.connection.endpoint), "the lane followed")
        victim.start()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_CLIENT)

    async def test_a_pinned_lane_fails_once_its_multiplexer_is_gone(self):
        """Flushing sends, which return once the lane has its connection
        and wrote to it, then raise once that connection is gone."""
        pinned = self.client.lane(pinned=True)
        await self.client.send_message(b"first", type=REQUEST, multiplexer=pinned, flush=True)
        way = self.cluster.multiplexer_at(pinned.connection.endpoint)
        way.kill()
        with self.assertRaises(NotConnected):
            for _ in range(20):
                await self.client.send_message(b"more", type=REQUEST, multiplexer=pinned, flush=True, timeout=5)
        with self.assertRaises(NotConnected):
            await self.client.query(b"more", REQUEST, to=self.peer.instance_id, multiplexer=pinned, timeout=5)
        reply = await self.client.query(b"still", REQUEST, to=self.peer.instance_id, timeout=10)
        self.assertEqual(b"STILL", reply.message)


class OrphanedMessagesTest(unittest.TestCase):
    """A connection that dies with frames it had not written yet hands all
    of them, in order, to one other connection, and the lane they went
    through follows them there, through a second failover too: not a copy
    to every connection, which with ANY routing is a request handled
    twice, nor each to the next connection round robin, which spread a
    stream's unwritten tail over every multiplexer left, the lane's next
    message going to yet another one."""

    def test_the_unwritten_messages_go_to_one_other_multiplexer_and_the_lane_follows(self):
        with Cluster(3, rules=RULES) as cluster:
            client = ThreadedClient(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
            try:
                self._hand_over(cluster, client, lambda: None)
            finally:
                client.shutdown()

    def test_the_synchronous_client_hands_them_over_the_same_way(self):
        """The synchronous client notices the dead connection inside a call:
        flush_all() runs the loop until the handed-over messages are out."""
        with Cluster(3, rules=RULES) as cluster:
            with SyncClient(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT) as client:
                self._hand_over(cluster, client, lambda: client.flush_all(timeout=20))

    def test_a_lane_follows_its_messages_through_a_second_failover(self):
        """The lane's messages go from mx[0] to another multiplexer, S, the
        lane sending nothing meanwhile; then S dies with another lane's
        messages unwritten, which go on to a third, T. The first lane's
        next message follows them through both failovers, to T, where it
        found S dead and took the next connection round robin."""
        with Cluster(4, rules=RULES) as cluster:
            client = ThreadedClient(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
            fakes = []
            try:
                lane = self._lane_on(cluster, client, cluster.mx[0])
                fakes = [
                    FakePeer(
                        cluster, peers.PYTHON_TEST_SERVER, name="behind-%d" % index, endpoints=[mx.endpoint]
                    ).start()
                    for index, mx in enumerate(cluster.mx[1:], 1)
                ]
                self._strand(client, lane, cluster.mx[0], b"first")
                taker = cluster.mx[1 + self._taker(cluster, client, fakes, b"first", lambda: None)]
                other = self._lane_on(cluster, client, taker)
                self._strand(client, other, taker, b"second")
                second = self._taker(cluster, client, fakes, b"second", lambda: None)
                client.send_message(b"after", type=REQUEST, multiplexer=lane, flush=True)
                self.assertIs(
                    cluster.mx[1 + second],
                    cluster.multiplexer_at(lane.connection.endpoint),
                    "the first lane followed its messages through both failovers",
                )
            finally:
                for fake in fakes:
                    fake.stop()
                client.shutdown()

    def _hand_over(self, cluster, client, run_loop):
        """A lane on mx[0] with STRANDED messages unwritten behind a frozen
        multiplexer, mx[0] killed: all of them reach one of the two others,
        in order, and so does the lane's next message, after them.
        `run_loop` lets the client notice the dead connection and write
        what it handed over."""
        lane = self._lane_on(cluster, client, cluster.mx[0])
        behind = [
            FakePeer(cluster, peers.PYTHON_TEST_SERVER, name="behind-%d" % index, endpoints=[mx.endpoint]).start()
            for index, mx in ((1, cluster.mx[1]), (2, cluster.mx[2]))
        ]
        try:
            self._strand(client, lane, cluster.mx[0], b"tail")
            run_loop()
            taker = self._taker(cluster, client, behind, b"tail", run_loop)
            client.send_message(b"after", type=REQUEST, multiplexer=lane, flush=True)
            self.assertIs(
                cluster.mx[1 + taker],
                cluster.multiplexer_at(lane.connection.endpoint),
                "the lane followed its messages",
            )
            behind[taker].wait_for(REQUEST, matching=lambda mxmsg: mxmsg.message == b"after")
            self.assertEqual(
                b"after", behind[taker].messages(REQUEST)[-1].message, "the lane's next message came after them, there"
            )
        finally:
            for fake in behind:
                fake.stop()

    @staticmethod
    def _lane_on(cluster, client, mx):
        """A lane whose connection is `mx`'s: a new lane takes the next
        connection, round robin, so one of a few tries is."""
        for _ in range(2 * len(cluster.mx)):
            lane = client.lane()
            client.send_message(b"warm-up", type=REQUEST, multiplexer=lane, flush=True)
            if cluster.multiplexer_at(lane.connection.endpoint) is mx:
                return lane
        raise AssertionError("no lane on the multiplexer in %d tries" % (2 * len(cluster.mx)))

    @staticmethod
    def _strand(client, lane, mx, label):
        """`mx` frozen, fill_frames() through `lane`, then STRANDED
        messages `label:<n>:`, and `mx` killed: the fill carries more than
        the connection's sockets hold, whatever this machine's buffers, so
        every labelled message is still in the client's queue, behind the
        rest of the fill, when the connection dies."""
        mx.pause()  # frames queue up behind a socket nobody reads
        for payload in fill_frames():
            client.send_message(payload, type=REQUEST, multiplexer=lane)
        for index in range(STRANDED):
            client.send_message(label + b":%d:" % index, type=REQUEST, multiplexer=lane)
        mx.kill()

    def _taker(self, cluster, client, fakes, label, run_loop):
        """The index of the one fake that received the messages `_strand`
        labelled `label`, all of them, in the order sent; no other got any.
        Checked once every one has arrived and then a marker, sent through
        every live connection, has reached each fake behind one: what the
        dead connection handed to a connection went before that
        connection's marker, so a copy or a share handed to another
        connection is in too. `run_loop` writes the markers of a client
        that writes only inside its calls."""
        prefix = label + b":"

        def labelled():
            """Per fake, the payloads labelled `label` it received, in arrival order."""
            return [
                [mxmsg.message for mxmsg in fake.messages(REQUEST, lambda mxmsg: mxmsg.message.startswith(prefix))]
                for fake in fakes
            ]

        wait_until(
            lambda: len(set().union(*labelled())) == STRANDED,
            LONG_TIMEOUT,
            "the %d messages labelled %r" % (STRANDED, label),
        )
        marker = label + b"-marker"
        client.send_message(marker, type=REQUEST, multiplexer=client.ALL, flush=True)
        run_loop()
        for fake in fakes:
            if all(cluster.multiplexer_at(endpoint).running for endpoint in fake.endpoints):
                fake.wait_for(REQUEST, matching=lambda mxmsg: mxmsg.message == marker)
        arrived = labelled()
        takers = [index for index, payloads in enumerate(arrived) if payloads]
        self.assertEqual(1, len(takers), "the messages labelled %r went to one multiplexer" % label)
        self.assertEqual(
            [prefix + b"%d:" % index for index in range(STRANDED)], arrived[takers[0]], "all of them, in order"
        )
        return takers[0]


if __name__ == "__main__":
    unittest.main()
