"""Addressed queries, lanes and pinning through the synchronous client,
against two real multiplexers and scripted peers: the addressee alone gets
an addressed query, an instance that left is a failure and not a detour to
another instance, an addressee that declines searches is located all the
same, a multiplexer dying under the wait costs the query nothing, a lane
keeps a stream on one multiplexer and follows a failover, a pinned lane
fails instead, and FakePeer.via() says which way each message came.
"""

import random
import threading
import time
import unittest

from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import NotConnected, OperationFailed, OperationTimedOut
from multiplexer.testing import Cluster, FakePeer, TestClient
from multiplexer.testing import runfile

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

REQUEST = types.PYTHON_TEST_REQUEST
RESPONSE = types.PYTHON_TEST_RESPONSE


def answer(mxmsg):
    """The fakes' script: the payload upper-cased, after a pause when it says so."""
    if mxmsg.message.startswith(b"slow"):
        time.sleep(1.0)
    return mxmsg.message.upper()


class AddressedQueryTest(unittest.TestCase):
    """Two multiplexers; two fakes of one type, so that a query by type could
    land on either and an addressed one must not."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(2, rules=RULES).__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.__exit__(None, None, None)

    def setUp(self):
        self.first = FakePeer(self.cluster, peers.PYTHON_TEST_SERVER, name="first").start()
        self.second = FakePeer(self.cluster, peers.PYTHON_TEST_SERVER, name="second").start()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER, count=2)
        for peer in (self.first, self.second):
            peer.on(REQUEST, answer, RESPONSE)
        self.client = TestClient(self.cluster, peers.WEBSITE)

    def tearDown(self):
        self.client.shutdown()
        self.first.stop()
        self.second.stop()

    def test_only_the_addressee_gets_an_addressed_query(self):
        """Ten queries addressed to the second fake are answered by it; the
        first, of the same type, never sees one."""
        for index in range(10):
            reply = self.client.query(b"n%d" % index, REQUEST, to=self.second.instance_id)
            self.assertEqual(
                (RESPONSE, b"N%d" % index, self.second.instance_id), (reply.type, reply.message, reply.from_)
            )
        self.assertEqual(10, len(self.second.messages(REQUEST)))
        self.assertEqual([], self.first.received)

    def test_an_instance_that_left_is_a_failure_not_a_detour(self):
        """Addressed to an instance nobody has: OperationFailed within a
        fraction of the timeout, and no instance of the type sees a request."""
        gone = random.getrandbits(63) | 1
        started = time.monotonic()
        with self.assertRaises(OperationFailed):
            self.client.query(b"lost", REQUEST, to=gone, timeout=10)
        self.assertLess(time.monotonic() - started, 3, "a delivery error, not a timeout")
        self.assertEqual([], self.first.received)
        self.assertEqual([], self.second.received)

    def test_the_reply_and_the_connection_come_back_together(self):
        """with_connection=True returns the connection the reply came through,
        which names a multiplexer of the cluster."""
        reply, connection = self.client.query(b"where", REQUEST, to=self.first.instance_id, with_connection=True)
        self.assertEqual(b"WHERE", reply.message)
        self.assertTrue(connection)
        (arrival,) = self.first.arrivals(REQUEST)
        self.assertIs(self.cluster.multiplexer_at(connection.endpoint), arrival[1])


class AddressedQueryUnderFailureTest(unittest.TestCase):
    """The cases that need a multiplexer killed or a peer behind one only:
    a cluster per test."""

    def test_a_multiplexer_dying_under_the_wait_costs_the_query_nothing(self):
        """The request went through a multiplexer that is killed while the
        fake is still working on it: the reply comes anyway, through the
        other multiplexer."""
        with Cluster(2, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:
            peer.on(REQUEST, answer, RESPONSE)
            client = TestClient(cluster, peers.WEBSITE)
            try:
                lane = client.lane()
                client.query(b"warm-up", REQUEST, to=peer.instance_id, multiplexer=lane)
                victim = cluster.multiplexer_at(lane.connection.endpoint)
                killer = threading.Timer(0.3, victim.kill)
                killer.start()
                started = time.monotonic()
                reply = client.query(b"slow", REQUEST, to=peer.instance_id, multiplexer=lane, timeout=10)
                killer.join()
                self.assertEqual(b"SLOW", reply.message)
                self.assertLess(time.monotonic() - started, 6, "the locate phase, not a timeout")
                # The fake saw the request once or twice: the client locates
                # it at once and sends the request a second and last time
                # through the other multiplexer, but the fake's reply to the
                # first, going back through the other multiplexer, may end
                # the query first.
                arrivals = peer.arrivals(REQUEST, matching=lambda m: m.message == b"slow")
                self.assertIn(len(arrivals), (1, 2))
                self.assertIs(victim, arrivals[0][1])
                self.assertTrue(all(via is not victim for _, via in arrivals[1:]))
                self.assertIsNot(victim, cluster.multiplexer_at(lane.connection.endpoint), "the lane followed")
            finally:
                client.shutdown()

    @staticmethod
    def kill_when_held(victim, holding: threading.Event, released: threading.Event) -> None:
        """`victim` killed once the backend holds the request, which it then
        answers: kill() returns once the process is gone, so the backend's
        reply finds that connection closed and takes the other."""
        holding.wait(30)
        victim.kill()
        released.set()

    def test_a_lost_connection_searches_and_the_first_reply_answers(self):
        """A typed query whose multiplexer dies under the wait is not sent
        again: the request may have been routed first, as it was here, and
        the query moves on to the search at once. The backend holds the
        request until its multiplexer is killed, then answers it through the
        other one, during the search, and that reply answers the query, as a
        reply to the request does in every stage. The backend saw the request
        once, where it got it a second time, resent at once. Ordered by
        events: the kill waits for the backend to hold the request."""
        holding, released = threading.Event(), threading.Event()

        def held(mxmsg):
            """Holds "slower" until the test has killed its multiplexer."""
            if mxmsg.message == b"slower":
                holding.set()
                released.wait(30)
            return mxmsg.message.upper()

        with Cluster(2, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:
            peer.on(REQUEST, held, RESPONSE)
            client = TestClient(cluster, peers.WEBSITE)
            try:
                lane = client.lane()
                client.query(b"warm-up", REQUEST, multiplexer=lane)
                victim = cluster.multiplexer_at(lane.connection.endpoint)
                killer = threading.Thread(target=self.kill_when_held, args=(victim, holding, released))
                killer.start()
                reply = client.query(b"slower", REQUEST, multiplexer=lane, timeout=30)
                killer.join()
                self.assertEqual(b"SLOWER", reply.message)
                arrivals = peer.arrivals(REQUEST, matching=lambda m: m.message == b"slower")
                self.assertEqual(arrivals[0][0].id, reply.references, "the answer to the request")
                self.assertEqual(1, len(arrivals), "the request went out once, its reply ending the search")
            finally:
                client.shutdown()

    def test_a_lost_connection_is_not_connected_and_the_message_went_once(self):
        """_send_and_receive() sends its message once: when the connection
        that wrote it dies under the wait, it raises NotConnected, where it
        sent the message again with a new id. A MultiplexerMessage the
        caller passed in keeps its own."""

        holding, released = threading.Event(), threading.Event()

        def held(mxmsg):
            """Holds "held" until the test has killed its multiplexer."""
            if mxmsg.message == b"held":
                holding.set()
                released.wait(30)
            return mxmsg.message.upper()

        with Cluster(2, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:
            peer.on(REQUEST, held, RESPONSE)
            client = TestClient(cluster, peers.WEBSITE)
            try:
                lane = client.lane()
                client.query(b"warm-up", REQUEST, multiplexer=lane)
                victim = cluster.multiplexer_at(lane.connection.endpoint)
                mxmsg = client.client.new_message(message=b"held", type=REQUEST)
                own = mxmsg.id
                killer = threading.Thread(target=self.kill_when_held, args=(victim, holding, released))
                killer.start()
                attempts: dict = {}
                with self.assertRaises(NotConnected):
                    client.client._send_and_receive(mxmsg, multiplexer=lane, timeout=5, attempts=attempts)
                killer.join()
                self.assertEqual([own], list(attempts), "sent once")
                self.assertEqual(own, mxmsg.id, "the caller's message, untouched")
            finally:
                client.shutdown()

    def test_the_asymmetric_moment_is_bridged_by_the_locate_phase(self):
        """The addressee is behind one multiplexer only while the client is
        on both: the request through the wrong one comes back as a delivery
        error, the PING finds the right one, and the reply comes."""
        with Cluster(2, rules=RULES) as cluster:
            peer = FakePeer(cluster, peers.PYTHON_TEST_SERVER, endpoints=[cluster.mx[1].endpoint]).start()
            peer.on(REQUEST, answer, RESPONSE)
            client = TestClient(cluster, peers.WEBSITE)
            try:
                for index in range(6):  # round robin: some go through the wrong multiplexer first
                    reply = client.query(b"n%d" % index, REQUEST, to=peer.instance_id, timeout=5)
                    self.assertEqual(b"N%d" % index, reply.message)
                self.assertTrue(all(via is cluster.mx[1] for _, via in peer.arrivals(REQUEST)))
                self.assertEqual(6, len(peer.messages(REQUEST)))
            finally:
                client.shutdown()
                peer.stop()

    def test_an_addressee_that_declines_searches_is_located(self):
        """A backend that declines every search, the fake here as a
        saturated one would, behind one multiplexer only: a typed query
        sent the other way, whose search it declines, cannot find it; an
        addressed one locates it with a PING, which it answers whatever its
        search policy, and gets the reply."""
        with Cluster(2, rules=RULES) as cluster:
            peer = FakePeer(cluster, peers.PYTHON_TEST_SERVER, endpoints=[cluster.mx[1].endpoint]).start()
            peer.on(REQUEST, answer, RESPONSE)
            client = TestClient(cluster, peers.WEBSITE)
            try:
                # A connection to the multiplexer the addressee is not behind:
                # connect() again returns the wrapper of the (new) connection.
                wrong = client.client.connect(cluster.mx[0].endpoint)
                peer.declining_searches = True
                with self.assertRaises(OperationTimedOut):
                    client.query(b"typed", REQUEST, multiplexer=client.lane(connection=wrong), timeout=1.5)
                reply = client.query(
                    b"saturated", REQUEST, to=peer.instance_id, multiplexer=client.lane(connection=wrong), timeout=5
                )
                self.assertEqual(b"SATURATED", reply.message)
                self.assertEqual([b"saturated"], [m.message for m in peer.messages(REQUEST)])
            finally:
                client.shutdown()
                peer.stop()


class LaneTest(unittest.TestCase):
    """A stream through one lane goes one way; killed under it, the lane
    moves or, pinned, fails."""

    def test_a_lane_keeps_a_stream_on_one_multiplexer_and_follows_a_failover(self):
        with Cluster(2, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:
            client = TestClient(cluster, peers.WEBSITE)
            try:
                for index in range(20):
                    client.send(b"loose-%d" % index, REQUEST)
                peer.wait_for(REQUEST, count=20)
                self.assertEqual(
                    {cluster.mx[0], cluster.mx[1]},
                    {via for _, via in peer.arrivals(REQUEST)},
                    "without a lane the stream is spread over both",
                )
                lane = client.lane()
                for index in range(200):
                    client.send(b"lane-%d" % index, REQUEST, multiplexer=lane)
                chunks = peer.wait_for(REQUEST, count=200, matching=lambda m: m.message.startswith(b"lane-"))
                self.assertEqual([b"lane-%d" % index for index in range(200)], [m.message for m in chunks], "in order")
                self.assertEqual(1, len({peer.via(m) for m in chunks}), "all the same way")
                first_way = peer.via(chunks[0])
                self.assertIs(first_way, cluster.multiplexer_at(lane.connection.endpoint))
                first_way.kill()
                for index in range(200, 400):
                    client.send(b"lane-%d" % index, REQUEST, multiplexer=lane)
                # The gap at the failover: a chunk written into the killed
                # multiplexer's socket before its closure was noticed is
                # lost; the rest arrives in order, the other way.
                peer.wait_for(REQUEST, matching=lambda m: m.message == b"lane-399")
                rest = peer.messages(
                    REQUEST, matching=lambda m: m.message.startswith(b"lane-") and int(m.message[5:]) >= 200
                )
                self.assertGreaterEqual(len(rest), 198, "more than the failover gap lost")
                self.assertEqual(sorted(rest, key=lambda m: int(m.message[5:])), rest, "in order after the gap")
                self.assertEqual(
                    {cluster.mx[1] if first_way is cluster.mx[0] else cluster.mx[0]}, {peer.via(m) for m in rest}
                )
                self.assertIs(peer.via(rest[-1]), cluster.multiplexer_at(lane.connection.endpoint), "the lane moved")
            finally:
                client.shutdown()

    def test_a_pinned_lane_fails_instead_of_following(self):
        with Cluster(2, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:
            peer.on(REQUEST, answer, RESPONSE)
            client = TestClient(cluster, peers.WEBSITE)
            try:
                lane = client.lane(pinned=True)
                for index in range(100):
                    client.send(b"pin-%d" % index, REQUEST, multiplexer=lane)
                chunks = peer.wait_for(REQUEST, count=100)
                way = peer.via(chunks[0])
                self.assertEqual(1, len({peer.via(m) for m in chunks}))
                way.kill()
                with self.assertRaises(NotConnected):
                    for index in range(100, 200):
                        client.send(b"pin-%d" % index, REQUEST, multiplexer=lane)
                self.assertTrue(lane.closed)
                with self.assertRaises(NotConnected):
                    client.query(b"pinned", REQUEST, to=peer.instance_id, multiplexer=lane, timeout=5)
                with self.assertRaises(NotConnected):
                    client.send(b"queued", REQUEST, multiplexer=lane, flush=False)
                self.assertEqual({way}, {via for _, via in peer.arrivals(REQUEST)}, "nothing went the other way")
            finally:
                client.shutdown()

    def test_a_pinned_lane_refuses_an_addressee_behind_the_other_multiplexer(self):
        with Cluster(2, rules=RULES) as cluster:
            peer = FakePeer(cluster, peers.PYTHON_TEST_SERVER, endpoints=[cluster.mx[1].endpoint]).start()
            peer.on(REQUEST, answer, RESPONSE)
            client = TestClient(cluster, peers.WEBSITE)
            try:
                wrong = client.client.connect(cluster.mx[0].endpoint)
                started = time.monotonic()
                with self.assertRaises(OperationFailed):
                    client.query(
                        b"x",
                        REQUEST,
                        to=peer.instance_id,
                        multiplexer=client.lane(connection=wrong, pinned=True),
                        timeout=5,
                    )
                self.assertLess(time.monotonic() - started, 3)
                self.assertEqual([], peer.received)
            finally:
                client.shutdown()
                peer.stop()

    def test_a_typed_query_leaves_the_lane_where_the_answer_came_from(self):
        with Cluster(2, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:
            peer.on(REQUEST, answer, RESPONSE)
            client = TestClient(cluster, peers.WEBSITE)
            try:
                lane = client.lane()
                reply, connection = client.query(b"first", REQUEST, multiplexer=lane, with_connection=True)
                self.assertEqual(b"FIRST", reply.message)
                self.assertEqual(connection.endpoint, lane.connection.endpoint)
                for index in range(50):
                    client.send(b"then-%d" % index, REQUEST, multiplexer=lane)
                arrivals = peer.arrivals(REQUEST)
                self.assertEqual(1, len({via for _, via in arrivals}), "the sends followed the query")
                way = cluster.multiplexer_at(connection.endpoint)
                self.assertTrue(all(via is way for _, via in arrivals))
            finally:
                client.shutdown()

    def test_pinning_to_a_connection_a_reply_came_through(self):
        """A pinned lane seeded with the connection a reply came through
        sends and queries that way and fails once it is gone; the bare
        connection prefers that way and falls back, as a reply does."""
        with Cluster(2, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:
            peer.on(REQUEST, answer, RESPONSE)
            client = TestClient(cluster, peers.WEBSITE)
            try:
                reply, connection = client.query(b"first", REQUEST, with_connection=True)
                way = cluster.multiplexer_at(connection.endpoint)
                pinned = client.lane(pinned=True, connection=connection)
                client.send(b"pinned-send", REQUEST, multiplexer=pinned)
                reply = client.query(b"pinned-query", REQUEST, multiplexer=pinned)
                self.assertEqual(b"PINNED-QUERY", reply.message)
                client.send(b"pinned-queued", REQUEST, multiplexer=pinned, flush=False)
                client.send(b"preferred", REQUEST, multiplexer=connection)
                peer.wait_for(REQUEST, count=5)
                self.assertTrue(all(via is way for _, via in peer.arrivals(REQUEST)))
                way.kill()
                with self.assertRaises(NotConnected):
                    client.send(b"too-late", REQUEST, multiplexer=pinned)
                with self.assertRaises(NotConnected):
                    client.query(b"too-late", REQUEST, multiplexer=pinned, timeout=5)
                with self.assertRaises(NotConnected):
                    client.send(b"too-late", REQUEST, multiplexer=pinned, flush=False)
                self.assertEqual(5, len(peer.received), "nothing arrived after the kill")
                client.send(b"fallback", REQUEST, multiplexer=connection)
                reply = client.query(b"fallback-query", REQUEST, multiplexer=connection, timeout=5)
                self.assertEqual(b"FALLBACK-QUERY", reply.message)
                (fell_back, _) = peer.wait_for(REQUEST, count=2, matching=lambda m: m.message.startswith(b"fallback"))
                self.assertIsNot(way, peer.via(fell_back), "the bare connection form went the other way")
            finally:
                client.shutdown()


if __name__ == "__main__":
    unittest.main()
