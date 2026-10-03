"""A Python BaseMultiplexerServer on two multiplexers, one of which stops
reading with the backend's connection to it full while the backend owes
answers through it, echoes of PINGs and searches, a report of a handler's
exception and a pickle reply: the loop queues those answers and serves
on through the other multiplexer, periodic_task() keeps being called and
the program's other threads keep running; once the multiplexer reads
again, every answer reaches its requester. Each such answer waited for
its write, 10 s, with the GIL held, so that the loop answered nothing
else and every thread of the process stopped meanwhile. Counted, not
timed: requests answered, turns of the loop and round trips of another
thread, each wait bounded only as a failure detector.
"""

import pickle
import queue
import threading
import time
import unittest

from multiplexer.clients import SyncClient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import ConnectionWrapper, OperationTimedOut, parse_message
from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, runfile, wait_until
from multiplexer.testing.buffers import fill_frames

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

REQUEST = types.PYTHON_TEST_REQUEST
RESPONSE = types.PYTHON_TEST_RESPONSE
PINGS = 3  # PINGs, and as many searches, the backend owes an echo through the frozen multiplexer
OWED = 2 * PINGS + 2  # with the report of a handler's exception and a pickle reply
REQUESTS = 10  # requests through the other multiplexer, answered while the first is frozen
TURNS = 10  # turns of the loop, periodic_task() calls, while it is frozen
ROUND_TRIPS = 1000  # round trips of another thread while it is frozen


class OwingBackend(BaseMultiplexerServer):
    """The backend. b"hello" teaches it the connection it came through,
    the one to the multiplexer that freezes; b"gate" holds the loop until
    `release` is set and then fills that connection; b"raise" raises,
    b"pickle" is answered with a pickle and anything else with b"answer".
    Counts the answers it owes through the frozen multiplexer as each send
    returns, and the turns of its loop."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)
        self.toward_frozen: ConnectionWrapper | None = None  # the connection to the multiplexer that freezes
        self.gated = threading.Event()  # the gate is being handled
        self.release = threading.Event()  # the test's, once that multiplexer is frozen: go on
        self.taking_up = threading.Event()  # the send of the first answer owed through it began
        self.owed_sent = 0  # answers owed through it whose send returned
        self.turns = 0  # periodic_task() calls so far

    def handle_message(self, mxmsg: MultiplexerMessage) -> None:
        """Answer as the payload says."""
        if mxmsg.message == b"hello":
            self.toward_frozen = self.last_connwrap
            self.send_message(message=b"hello", type=RESPONSE)
        elif mxmsg.message == b"gate":
            self.gated.set()
            self.release.wait(60)  # a failure detector's bound
            # Twice what the two sockets hold: the rest waits in the
            # connection's queue, ahead of the answers.
            for payload in fill_frames():
                self.conn.send_message(
                    payload, type=types.TEST_UNROUTED, multiplexer=self.toward_frozen, report_delivery_error=False
                )
            self.no_response()
        elif mxmsg.message == b"raise":
            raise RuntimeError("raised on purpose")
        elif mxmsg.message == b"pickle":
            self.send_pickle({"answer": 42})
        else:
            self.send_message(message=b"answer", type=RESPONSE)

    def send_message(self, **kwargs):
        """The library's. An answer owed through the frozen multiplexer, to
        what came through it once the gate opened, is noted as its send
        begins and counted once the send returns."""
        owed = (
            self.release.is_set()
            and self.toward_frozen is not None
            and self.last_connwrap is not None
            and self.last_connwrap.is_same_connection(self.toward_frozen)
        )
        if owed:
            self.taking_up.set()
        sent = super().send_message(**kwargs)
        if owed:
            self.owed_sent += 1
        return sent

    def periodic_task(self) -> None:
        """Count the turn."""
        self.turns += 1


def next_message(client: SyncClient, timeout: float) -> MultiplexerMessage:
    """The next message `client` reads, a BACKEND_ERROR as it came, where
    SyncClient's reads raise it as BackendError; OperationTimedOut after
    `timeout` seconds."""
    serialized, _ = client.read_raw_message(timeout)
    return parse_message(MultiplexerMessage, serialized)


def written_out(client: SyncClient, timeout: float = 30.0) -> None:
    """Return once the multiplexer has written out what `client` sent
    before, to the peers it routed it to: a PING the client sends itself,
    routed after it, came back. The multiplexer handles a connection's
    frames one at a time, and writes a frame to a receiver's socket that
    has room before it handles the next one from the same connection, so
    what came before the PING reaches its receivers whatever becomes of
    the multiplexer afterwards, frozen or gone. OperationTimedOut after
    `timeout`, a failure detector's bound."""
    marker = client.send_message(b"written out?", type=types.PING, to=client.instance_id, flush=True)
    deadline = time.monotonic() + timeout
    while next_message(client, max(0.0, deadline - time.monotonic())).id != marker:
        pass


def replies(client: SyncClient, sent: list[int], timeout: float) -> dict[int, MultiplexerMessage]:
    """The replies `client` reads to the messages `sent`, by the id each
    references, once every one came or `timeout` seconds passed, a
    failure detector's bound."""
    got: dict[int, MultiplexerMessage] = {}
    deadline = time.monotonic() + timeout
    while len(got) < len(sent):
        try:
            mxmsg = next_message(client, max(0.0, deadline - time.monotonic()))
        except OperationTimedOut:
            break
        if mxmsg.references in sent:
            got[mxmsg.references] = mxmsg
    return got


def round_trips(count: int, timeout: float) -> int:
    """How many of `count` round trips with another thread, a number handed
    to it and back through two queues, are done within `timeout` seconds,
    a failure detector's bound: every one unless a thread holds the GIL
    meanwhile, since each needs both threads to run."""
    there: queue.Queue[int | None] = queue.Queue()
    back: queue.Queue[int] = queue.Queue()

    def echo() -> None:
        """The other thread: hand back every number, until None."""
        while (number := there.get()) is not None:
            back.put(number)

    thread = threading.Thread(target=echo, name="round-trips")
    thread.start()
    done = 0
    deadline = time.monotonic() + timeout
    try:
        for number in range(count):
            there.put(number)
            back.get(timeout=max(0.0, deadline - time.monotonic()))
            done += 1
    except queue.Empty:
        pass
    finally:
        there.put(None)
        thread.join()
    return done


class FrozenMultiplexerTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_multiplexer_that_stops_reading_holds_up_neither_the_loop_nor_other_threads(self) -> None:
        """What the backend owes through the first multiplexer is in its
        socket before that multiplexer freezes (written_out), the loop held
        meanwhile on a gate that came through the second; once released,
        the gate fills the connection to the frozen one, and the loop
        answers what it owes there behind that fill, then whatever comes
        through the second."""
        with Cluster(2, rules=RULES) as cluster, BackendThread(lambda: OwingBackend(cluster.endpoints)) as served:
            backend = served.backend
            assert backend is not None
            frozen, serving = cluster.mx
            with (
                SyncClient([frozen.endpoint], type=peers.WEBSITE) as through_frozen,
                SyncClient([serving.endpoint], type=peers.WEBSITE) as through_serving,
            ):
                self.assertEqual(b"hello", through_frozen.query(b"hello", REQUEST, timeout=30).message)
                backend_id = backend.conn.instance_id
                try:
                    through_serving.send_message(b"gate", type=REQUEST, flush=True)
                    self.assertTrue(backend.gated.wait(30), "the backend took up the gate")
                    echoed: dict[int, bytes] = {}
                    for index in range(PINGS):
                        for kind, payload in (
                            (types.PING, b"ping %d" % index),
                            (types.BACKEND_FOR_PACKET_SEARCH, b"search %d" % index),
                        ):
                            echoed[through_frozen.send_message(payload, type=kind, to=backend_id, flush=True)] = payload
                    raised = through_frozen.send_message(b"raise", type=REQUEST, flush=True)
                    pickled = through_frozen.send_message(b"pickle", type=REQUEST, flush=True)
                    written_out(through_frozen)
                    frozen.pause()
                finally:
                    backend.release.set()
                try:
                    self.assertTrue(
                        backend.taking_up.wait(30), "the backend took up what it owes through the frozen multiplexer"
                    )
                    self.assertEqual(ROUND_TRIPS, round_trips(ROUND_TRIPS, 60), "another thread's round trips")
                    requests = [
                        through_serving.send_message(b"request %d" % index, type=REQUEST, flush=True)
                        for index in range(REQUESTS)
                    ]
                    answered = {
                        request: reply.message for request, reply in replies(through_serving, requests, 60).items()
                    }
                    self.assertEqual(
                        dict.fromkeys(requests, b"answer"), answered, "requests through the other multiplexer"
                    )
                    turns = backend.turns
                    wait_until(lambda: backend.turns >= turns + TURNS, 60, "%d more turns of the loop" % TURNS)
                    self.assertEqual(OWED, backend.owed_sent, "answers owed through the frozen multiplexer, queued")
                finally:
                    frozen.resume()
                answers = replies(through_frozen, list(echoed) + [raised, pickled], 60)
                self.assertEqual(
                    echoed,
                    {request: answers[request].message for request in echoed if request in answers},
                    "the echoes once the multiplexer reads again",
                )
                self.assertEqual({types.PING}, {answers[request].type for request in echoed if request in answers})
                self.assertEqual(types.BACKEND_ERROR, answers[raised].type)
                self.assertEqual({"answer": 42}, pickle.loads(answers[pickled].message))


if __name__ == "__main__":
    unittest.main()
