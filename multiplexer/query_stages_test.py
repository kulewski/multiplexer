"""A query's stages when a connection is lost, in the Python SyncClient, whose
stages are its own code, against multiplexers of the test's own (StandIn),
as multiplexer/query_stages_test.cc does for the C++ clients. A request goes
out at most twice and a query never goes back a stage; a lost connection
never proves an attempt dead, since it may have been routed first and its
reply may come back another way. The search that finds nobody waits for the
request a backend may have, where it failed at once; a lost direct request
is waited for, where it went out again; an addressed query whose second
request draws a delivery error waits for the first, where it sent the
request again where its connection died; a searched connection that goes is
no answer, the search waiting on for the PING that comes back another way;
an addressed query whose request went with its connection takes the reply to
the request sent again, where it sent the request again at once; a request
whose send ran out of time is still an attempt, its reply, written later,
answering, where it threw that reply away; a request the client gave up on
went nowhere and is struck off the late wait, which then ends at once; a
pinned query waiting for a late reply ends NotConnected when the lane's
connection goes, where it timed out; the request sent again is a copy of the
request, its workflow kept, where it was built anew from the type and
payload; a search that waited for a connection and lost it at once waits on
for the next, where it was placed on nothing and the query waited out the
stage; and a delivery error for the request, which a backend leaving sends
back another way, strikes the request off and leaves the search, and the
direct request's stage, on, where it was taken for the multiplexer's
"nobody" or the direct request's own and the query timed out. And a request
too big to send again is refused at the call, where it went out and failed
at the third stage. And an addressed query whose request still waits for
room at its one deadline, a connection live, ends OperationTimedOut, not
NotConnected, which nothing pinned. Counted, not timed: the stand-ins record
every message the client sent, every turn is a script's but a first stage's
end in the cases only a stage that runs out of time reaches, a pinned
query's late wait, a request left unwritten and an addressed query's
deadline, and a bound on a wait only detects a failure.
"""

import queue
import socket
import threading
import time
import unittest
import zlib
from typing import Callable

from multiplexer.clients import SyncClient
from multiplexer.Multiplexer_pb2 import MultiplexerMessage, WelcomeMessage
from multiplexer.multiplexer_constants import peers, types
from multiplexer._native import MAX_MESSAGE_SIZE
from multiplexer.mxclient import NotConnected, OperationFailed, OperationTimedOut
from multiplexer.testing.raw_peer import HEADER, frame

BACKEND = 0xBAC  # the instance a search finds, or an addressed query asks
LEAVING = 0x1EA7  # the backend that had the request, refusing it as it leaves


class StandIn:
    """A multiplexer of the test's own on 127.0.0.1, for one client: a thread
    accepts it, exchanges welcomes and reads every frame it sends, keeping
    its messages, the protocol's own frames skipped, for the test's script
    to take in order (next) and to count afterwards (count); one made
    `stalled` reads nothing after the welcome until release(), so that
    what the client sends fills the socket's buffers and waits in the
    client. What the script writes goes to the client; hang_up() ends the
    connection."""

    def __init__(self, stalled: bool = False) -> None:
        self.stalled = stalled
        self.released = threading.Event()
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.port: int = self.listener.getsockname()[1]
        self.id = 0x5100 + self.port  # this multiplexer's instance id
        self.ids = self.port << 32  # ids of its own: the client drops a repeated id as a copy
        self.connection: socket.socket | None = None
        self.client_id = 0
        self.unread: "queue.Queue[MultiplexerMessage | None]" = queue.Queue()
        self.seen: list[MultiplexerMessage] = []
        self.lock = threading.Lock()
        self.welcomed = threading.Event()
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read_exactly(self, count: int) -> bytes:
        """`count` bytes from the client; ConnectionError when it ends first."""
        assert self.connection is not None
        data = b""
        while len(data) < count:
            chunk = self.connection.recv(count - len(data))
            if not chunk:
                raise ConnectionError("the connection ended")
            data += chunk
        return data

    def _frame(self) -> MultiplexerMessage:
        """The client's next frame, as a message."""
        length, crc = HEADER.unpack(self._read_exactly(HEADER.size))
        body = self._read_exactly(length)
        assert zlib.crc32(body) == crc
        return MultiplexerMessage.FromString(body)

    def _read(self) -> None:
        """The reader thread: the connection, the welcomes, then every frame until it ends."""
        try:
            self.connection, _ = self.listener.accept()
            self.connection.settimeout(60)  # a client that never ends fails the test rather than hangs it
            self.client_id = getattr(self._frame(), "from")
            welcome = WelcomeMessage(type=peers.MULTIPLEXER, id=self.id).SerializeToString()
            self.write(self._message(types.CONNECTION_WELCOME, self.id, 0, welcome), welcomed=True)
            self.welcomed.set()
            if self.stalled:
                self.released.wait()
            while True:
                mxmsg = self._frame()
                if mxmsg.type in (types.HEARTBIT, types.PEER_CONTROL):
                    continue
                with self.lock:
                    self.seen.append(mxmsg)
                self.unread.put(mxmsg)
        except (OSError, ConnectionError):
            pass  # the connection ended
        finally:
            # The client ended its side, or hang_up() this one: this side
            # closes too, as a multiplexer's does, so that the client's
            # shutdown, which waits for it, returns at once.
            if self.connection is not None:
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            self.welcomed.set()
            self.unread.put(None)

    def next(self) -> MultiplexerMessage | None:
        """The client's next message, within 30 s: None when none came."""
        try:
            return self.unread.get(timeout=30)
        except queue.Empty:
            return None

    def _message(self, type_: int, from_: int, references: int, payload: bytes = b"late") -> MultiplexerMessage:
        """A message of `type_` from `from_` to the client, answering `references`."""
        self.ids += 1
        mxmsg = MultiplexerMessage(id=self.ids, to=self.client_id, type=type_, references=references, message=payload)
        setattr(mxmsg, "from", from_)
        return mxmsg

    def answer(self, type_: int, from_: int, references: int) -> MultiplexerMessage:
        """A message of `type_` from `from_` to the client, answering `references`."""
        return self._message(type_, from_, references)

    def write(self, mxmsg: MultiplexerMessage, welcomed: bool = False) -> bool:
        """`mxmsg` to the client: False when it could not be written."""
        if not welcomed:
            self.welcomed.wait(30)
        try:
            assert self.connection is not None
            self.connection.sendall(frame(mxmsg.SerializeToString()))
            return True
        except (OSError, AssertionError):
            return False

    def release(self) -> None:
        """A stalled stand-in reads from now on, a multiplexer that caught up."""
        self.released.set()

    def hang_up(self) -> None:
        """Ends the client's connection."""
        self.released.set()  # a stalled reader goes on, to the end
        if self.connection is not None:
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def count(self, type_: int) -> int:
        """How many messages of `type_` the client sent so far."""
        with self.lock:
            return sum(1 for mxmsg in self.seen if mxmsg.type == type_)

    def close(self) -> None:
        """Every socket closed and the reader joined."""
        self.hang_up()
        self.listener.close()
        self.thread.join(60)
        if self.connection is not None:
            self.connection.close()


class ThreeConnections:
    """A multiplexer of the test's own that the client connects to three
    times, one connection after another, on one thread: the first takes
    the request and ends; the second, the client's reconnect, is welcomed
    and ended at once, the welcome and the end in one segment (TCP_CORK),
    so that the client counts it up and has its end at hand before it
    places anything on it; the third, the next reconnect, says nobody has
    a backend of the type to the search and then answers the request.
    `outcome` is "" once it went so."""

    def __init__(self) -> None:
        self.listener = socket.create_server(("127.0.0.1", 0), backlog=4)
        self.port: int = self.listener.getsockname()[1]
        self.id = 0x5100 + self.port  # this multiplexer's instance id
        self.ids = 1 << 40  # ids of its own
        self.client_id = 0
        self.outcome = "not over"
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        """The connections in turn, the outcome kept."""
        try:
            self.outcome = self._connections()
        except (OSError, ConnectionError) as error:
            self.outcome = "the connections broke: %r" % error

    def _connections(self) -> str:
        """The three connections: "" as planned, else what went wrong."""
        first = self._welcome()
        request = self._next(first, types.PYTHON_TEST_REQUEST)
        first.shutdown(socket.SHUT_RDWR)  # the request's way: it goes
        first.close()
        if request is None:
            return "the request did not come"
        self._welcome(then_end=True).close()
        third = self._welcome()
        search = self._next(third, types.BACKEND_FOR_PACKET_SEARCH)
        if search is None:
            third.close()
            return "no search came"
        self._write(third, types.DELIVERY_ERROR, self.id, search.id)  # nobody here
        self._write(third, types.PYTHON_TEST_RESPONSE, BACKEND, request.id)
        while self._next(third, -1) is not None:  # until the client ends
            pass
        third.close()
        return ""

    def _welcome(self, then_end: bool = False) -> socket.socket:
        """The next connection, its welcomes exchanged; with `then_end`
        this side's welcome and its end go out in one segment."""
        connection, _ = self.listener.accept()
        connection.settimeout(60)  # a client that never ends fails the test rather than hangs it
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1 if then_end else 0)
        self.client_id = getattr(self._frame(connection), "from")
        welcome = WelcomeMessage(type=peers.MULTIPLEXER, id=self.id).SerializeToString()
        connection.sendall(frame(self._message(types.CONNECTION_WELCOME, self.id, 0, welcome).SerializeToString()))
        if then_end:
            connection.shutdown(socket.SHUT_RDWR)  # the end behind the welcome, the corked segment sent with it
        return connection

    def _frame(self, connection: socket.socket) -> MultiplexerMessage:
        """The client's next frame on `connection`, as a message."""
        length, crc = HEADER.unpack(self._read_exactly(connection, HEADER.size))
        body = self._read_exactly(connection, length)
        assert zlib.crc32(body) == crc
        return MultiplexerMessage.FromString(body)

    @staticmethod
    def _read_exactly(connection: socket.socket, count: int) -> bytes:
        """`count` bytes from `connection`; ConnectionError when it ends first."""
        data = b""
        while len(data) < count:
            chunk = connection.recv(count - len(data))
            if not chunk:
                raise ConnectionError("the connection ended")
            data += chunk
        return data

    def _next(self, connection: socket.socket, type_: int) -> MultiplexerMessage | None:
        """The client's next message of `type_` on `connection`, any type
        for -1, the protocol's own skipped: None at the connection's end."""
        try:
            while True:
                mxmsg = self._frame(connection)
                if mxmsg.type not in (types.HEARTBIT, types.PEER_CONTROL) and type_ in (-1, mxmsg.type):
                    return mxmsg
        except (OSError, ConnectionError):
            return None

    def _message(self, type_: int, from_: int, references: int, payload: bytes = b"late") -> MultiplexerMessage:
        """A message of `type_` from `from_` to the client, answering `references`."""
        self.ids += 1
        mxmsg = MultiplexerMessage(id=self.ids, to=self.client_id, type=type_, references=references, message=payload)
        setattr(mxmsg, "from", from_)
        return mxmsg

    def _write(self, connection: socket.socket, type_: int, from_: int, references: int) -> None:
        """A message of `type_` from `from_` to the client on `connection`, answering `references`."""
        connection.sendall(frame(self._message(type_, from_, references).SerializeToString()))

    def close(self) -> None:
        """The listener closed and the thread joined."""
        self.listener.close()
        self.thread.join(60)


def frames_past_the_sockets(size: int) -> int:
    """How many frames of `size` bytes a multiplexer that reads nothing does
    not take: the most the two sockets may buffer, and some."""
    buffers = 0
    for path in ("/proc/sys/net/ipv4/tcp_wmem", "/proc/sys/net/ipv4/tcp_rmem"):
        try:
            with open(path) as limits:
                buffers += int(limits.read().split()[2])
        except (OSError, IndexError, ValueError):
            buffers += 8 << 20  # a guess where /proc does not say
    return 2 * buffers // size + 64


def in_thread(script: Callable[[], str], outcomes: list[str]) -> threading.Thread:
    """`script` on a thread of its own, its outcome appended to `outcomes`."""
    thread = threading.Thread(target=lambda: outcomes.append(script()), daemon=True)
    thread.start()
    return thread


class Board:
    """What two scripts tell each other: an attempt's id, once one knows it,
    that a multiplexer went, and which of them took the search's one role."""

    def __init__(self) -> None:
        self.ids: dict[str, int] = {}
        self.told = {"request": threading.Event(), "direct": threading.Event(), "gone": threading.Event()}
        self.lock = threading.Lock()
        self.claimed = False

    def publish(self, name: str, mxmsg_id: int) -> None:
        """Tells the other script `mxmsg_id` as `name`, once: the first word stands."""
        with self.lock:
            self.ids.setdefault(name, mxmsg_id)
        self.told[name].set()

    def awaited(self, name: str) -> int:
        """The id told as `name`, 30 s at most: 0 when none was told."""
        self.told[name].wait(30)
        with self.lock:
            return self.ids.get(name, 0)

    def claim(self) -> bool:
        """Whether this caller is the first to claim the search's role."""
        with self.lock:
            first, self.claimed = not self.claimed, True
            return first


class QueryStagesTest(unittest.TestCase):
    """See the module docstring."""

    def run_scripts(self, script: Callable[[StandIn], str]) -> tuple[StandIn, StandIn, list[str]]:
        """Two stand-ins, each running `script` on a thread of its own; the
        scripts' outcomes are filled in once they end, "" for a script that
        went as planned."""
        first, second = StandIn(), StandIn()
        return first, second, self.run_on([first, second], script)

    def run_on(self, stand_ins: list[StandIn], script: Callable[[StandIn], str]) -> list[str]:
        """`script` on a thread of its own for each of `stand_ins`: the
        scripts' outcomes, filled in once they end, "" for a script that
        went as planned."""
        for stand_in in stand_ins:
            self.addCleanup(stand_in.close)
        outcomes = ["not over"] * len(stand_ins)

        def run(index: int, stand_in: StandIn) -> None:
            outcomes[index] = script(stand_in)

        threads = [
            threading.Thread(target=run, args=(index, stand_in), daemon=True)
            for index, stand_in in enumerate(stand_ins)
        ]
        for thread in threads:
            thread.start()
        self.addCleanup(lambda: [thread.join(60) for thread in threads])
        self.threads = threads
        return outcomes

    def end_scripts(self, *stand_ins: StandIn) -> None:
        """The connections ended and the scripts joined."""
        for stand_in in stand_ins:
            stand_in.hang_up()
        for thread in self.threads:
            thread.join(60)

    def test_a_search_that_finds_nobody_waits_for_the_request_a_backend_may_have(self) -> None:
        """The connection that carried the request goes: the query searches
        at once through the other multiplexer, which says nobody has a
        backend of the type; the request may have been routed before the
        connection went, so the query waits for its reply, which comes."""
        board = Board()

        def script(stand_in: StandIn) -> str:
            mxmsg = stand_in.next()
            if mxmsg is None:
                return "nothing came"
            if mxmsg.type == types.PYTHON_TEST_REQUEST:  # the request's way: it goes
                board.publish("request", mxmsg.id)
                stand_in.hang_up()
                return ""
            if mxmsg.type != types.BACKEND_FOR_PACKET_SEARCH:
                return "type %d came, not the search" % mxmsg.type
            stand_in.write(stand_in.answer(types.DELIVERY_ERROR, stand_in.id, mxmsg.id))
            stand_in.write(stand_in.answer(types.PYTHON_TEST_RESPONSE, BACKEND, board.awaited("request")))
            return ""

        first, second, outcomes = self.run_scripts(script)
        with SyncClient([("127.0.0.1", first.port), ("127.0.0.1", second.port)], type=peers.WEBSITE) as client:
            reply = client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=30)
        self.end_scripts(first, second)
        self.assertEqual(["", ""], outcomes)
        self.assertEqual(b"late", reply.message)
        self.assertEqual(1, first.count(types.PYTHON_TEST_REQUEST) + second.count(types.PYTHON_TEST_REQUEST))

    def test_a_lost_direct_request_is_waited_for_not_sent_again(self) -> None:
        """Nobody takes the request; the search finds a backend behind one
        multiplexer, which takes the direct request and then goes, while the
        other says it has none. The query waits for the direct request's
        reply, which comes through the other, and nothing goes out again."""
        board = Board()

        def script(stand_in: StandIn) -> str:
            mxmsg = stand_in.next()
            if mxmsg is None:
                return "nothing came"
            if mxmsg.type == types.PYTHON_TEST_REQUEST:  # nobody takes the request
                stand_in.write(stand_in.answer(types.DELIVERY_ERROR, stand_in.id, mxmsg.id))
                mxmsg = stand_in.next()
                if mxmsg is None:
                    return "no search came"
            if mxmsg.type != types.BACKEND_FOR_PACKET_SEARCH:
                return "type %d came, not the search" % mxmsg.type
            if board.claim():  # the backend is behind this one: it takes the direct request, then goes
                stand_in.write(stand_in.answer(types.PING, BACKEND, mxmsg.id))
                direct = stand_in.next()
                if direct is None or direct.type != types.PYTHON_TEST_REQUEST or direct.to != BACKEND:
                    return "no direct request came"
                board.publish("direct", direct.id)
                stand_in.hang_up()
                return ""
            stand_in.write(stand_in.answer(types.DELIVERY_ERROR, stand_in.id, mxmsg.id))
            stand_in.write(stand_in.answer(types.PYTHON_TEST_RESPONSE, BACKEND, board.awaited("direct")))
            return ""

        first, second, outcomes = self.run_scripts(script)
        with SyncClient([("127.0.0.1", first.port), ("127.0.0.1", second.port)], type=peers.WEBSITE) as client:
            reply = client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=30)
        self.end_scripts(first, second)
        self.assertEqual(["", ""], outcomes)
        self.assertEqual(b"late", reply.message)
        self.assertEqual(2, first.count(types.PYTHON_TEST_REQUEST) + second.count(types.PYTHON_TEST_REQUEST))

    def test_an_addressed_query_waits_for_the_request_the_addressee_may_have(self) -> None:
        """An addressed query's request goes with its connection; the PING
        finds the addressee behind the other multiplexer, which then answers
        the request again with a delivery error. The first request may still
        be answered: the query waits, and its reply comes."""
        board = Board()

        def script(stand_in: StandIn) -> str:
            mxmsg = stand_in.next()
            if mxmsg is None:
                return "nothing came"
            if mxmsg.type == types.PYTHON_TEST_REQUEST and mxmsg.to == BACKEND:  # the request's way: it goes
                board.publish("request", mxmsg.id)
                stand_in.hang_up()
                return ""
            if mxmsg.type != types.PING or mxmsg.to != BACKEND:
                return "type %d came, not the PING that locates" % mxmsg.type
            stand_in.write(stand_in.answer(types.PING, BACKEND, mxmsg.id))
            again = stand_in.next()
            if again is None or again.type != types.PYTHON_TEST_REQUEST:
                return "the request did not come again"
            stand_in.write(stand_in.answer(types.DELIVERY_ERROR, stand_in.id, again.id))
            stand_in.write(stand_in.answer(types.PYTHON_TEST_RESPONSE, BACKEND, board.awaited("request")))
            return ""

        first, second, outcomes = self.run_scripts(script)
        with SyncClient([("127.0.0.1", first.port), ("127.0.0.1", second.port)], type=peers.WEBSITE) as client:
            reply = client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=30, to=BACKEND)
        self.end_scripts(first, second)
        self.assertEqual(["", ""], outcomes)
        self.assertEqual(b"late", reply.message)

    def test_an_addressed_query_takes_the_reply_to_its_second_request_after_a_loss(self) -> None:
        """An addressed query's request goes with its connection; the PING
        finds the addressee behind the other multiplexer, which answers the
        request sent again: that reply is the query's answer, whatever
        became of the first. It sent the request again at once through the
        other multiplexer, never locating the addressee."""

        def script(stand_in: StandIn) -> str:
            mxmsg = stand_in.next()
            if mxmsg is None:
                return "nothing came"
            if mxmsg.type == types.PYTHON_TEST_REQUEST and mxmsg.to == BACKEND:  # the request's way: it goes
                stand_in.hang_up()
                return ""
            if mxmsg.type != types.PING or mxmsg.to != BACKEND:
                return "type %d came, not the PING that locates" % mxmsg.type
            stand_in.write(stand_in.answer(types.PING, BACKEND, mxmsg.id))
            again = stand_in.next()
            if again is None or again.type != types.PYTHON_TEST_REQUEST:
                return "the request did not come again"
            stand_in.write(stand_in.answer(types.PYTHON_TEST_RESPONSE, BACKEND, again.id))
            return ""

        first, second, outcomes = self.run_scripts(script)
        with SyncClient([("127.0.0.1", first.port), ("127.0.0.1", second.port)], type=peers.WEBSITE) as client:
            reply = client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=30, to=BACKEND)
        self.end_scripts(first, second)
        self.assertEqual(["", ""], outcomes)
        self.assertEqual(b"late", reply.message)

    def test_a_searched_connection_that_goes_is_no_answer(self) -> None:
        """Nobody takes the request; one multiplexer goes with the search
        unanswered, and the other says nobody has a backend of the type.
        The search that went may have been routed first: the query waits
        on, and the backend's PING to it comes back another way, through
        the other, so the direct request goes there and is answered; the
        request went out twice, no more."""
        board = Board()

        def script(stand_in: StandIn) -> str:
            mxmsg = stand_in.next()
            if mxmsg is None:
                return "nothing came"
            if mxmsg.type == types.PYTHON_TEST_REQUEST:  # nobody takes the request
                stand_in.write(stand_in.answer(types.DELIVERY_ERROR, stand_in.id, mxmsg.id))
                mxmsg = stand_in.next()
                if mxmsg is None:
                    return "no search came"
            if mxmsg.type != types.BACKEND_FOR_PACKET_SEARCH:
                return "type %d came, not the search" % mxmsg.type
            if board.claim():
                stand_in.hang_up()  # goes with the search unanswered
                board.publish("gone", 1)
                return ""
            search = mxmsg.id
            stand_in.write(stand_in.answer(types.DELIVERY_ERROR, stand_in.id, search))
            if not board.awaited("gone"):
                return "the other multiplexer never went"
            stand_in.write(stand_in.answer(types.PING, BACKEND, search))  # the gone one's, come back this way
            direct = stand_in.next()
            if direct is None or direct.type != types.PYTHON_TEST_REQUEST or direct.to != BACKEND:
                return "no direct request came"
            stand_in.write(stand_in.answer(types.PYTHON_TEST_RESPONSE, BACKEND, direct.id))
            return ""

        first, second, outcomes = self.run_scripts(script)
        with SyncClient([("127.0.0.1", first.port), ("127.0.0.1", second.port)], type=peers.WEBSITE) as client:
            reply = client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=30)
        self.end_scripts(first, second)
        self.assertEqual(["", ""], outcomes)
        self.assertEqual(b"late", reply.message)
        self.assertEqual(2, first.count(types.PYTHON_TEST_REQUEST) + second.count(types.PYTHON_TEST_REQUEST))

    def test_a_refusal_of_the_request_during_the_search_leaves_the_search_on(self) -> None:
        """The connection that carried the request goes, and the search goes
        through the other multiplexer; the backend that had the request
        refuses it as it leaves, its delivery error coming back that way,
        and then a backend there answers the search. The refusal strikes
        the request off and leaves the search on: the PING finds the
        backend, and the direct request's reply is the answer."""
        board = Board()

        def script(stand_in: StandIn) -> str:
            mxmsg = stand_in.next()
            if mxmsg is None:
                return "nothing came"
            if mxmsg.type == types.PYTHON_TEST_REQUEST:  # the request's way: it goes
                board.publish("request", mxmsg.id)
                stand_in.hang_up()
                return ""
            if mxmsg.type != types.BACKEND_FOR_PACKET_SEARCH:
                return "type %d came, not the search" % mxmsg.type
            request = board.awaited("request")
            if not request:
                return "the request's id never came"
            stand_in.write(stand_in.answer(types.DELIVERY_ERROR, LEAVING, request))
            stand_in.write(stand_in.answer(types.PING, BACKEND, mxmsg.id))
            direct = stand_in.next()
            if direct is None or direct.type != types.PYTHON_TEST_REQUEST or direct.to != BACKEND:
                return "no direct request came"
            stand_in.write(stand_in.answer(types.PYTHON_TEST_RESPONSE, BACKEND, direct.id))
            return ""

        first, second, outcomes = self.run_scripts(script)
        with SyncClient([("127.0.0.1", first.port), ("127.0.0.1", second.port)], type=peers.WEBSITE) as client:
            reply = client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=30)
        self.end_scripts(first, second)
        self.assertEqual(["", ""], outcomes)
        self.assertEqual(b"late", reply.message)
        self.assertEqual(2, first.count(types.PYTHON_TEST_REQUEST) + second.count(types.PYTHON_TEST_REQUEST))

    def test_a_refusal_of_the_request_during_the_direct_request_leaves_its_stage_on(self) -> None:
        """The connection that carried the request goes; the search finds a
        backend behind the other multiplexer, which takes the direct
        request; then the backend that had the first request refuses it as
        it leaves, its delivery error coming back the same way, before the
        direct request's reply. The refusal strikes the request off and
        leaves the direct request's stage on: its reply is the answer."""
        board = Board()

        def script(stand_in: StandIn) -> str:
            mxmsg = stand_in.next()
            if mxmsg is None:
                return "nothing came"
            if mxmsg.type == types.PYTHON_TEST_REQUEST:  # the request's way: it goes
                board.publish("request", mxmsg.id)
                stand_in.hang_up()
                return ""
            if mxmsg.type != types.BACKEND_FOR_PACKET_SEARCH:
                return "type %d came, not the search" % mxmsg.type
            stand_in.write(stand_in.answer(types.PING, BACKEND, mxmsg.id))
            direct = stand_in.next()
            if direct is None or direct.type != types.PYTHON_TEST_REQUEST or direct.to != BACKEND:
                return "no direct request came"
            request = board.awaited("request")
            if not request:
                return "the request's id never came"
            stand_in.write(stand_in.answer(types.DELIVERY_ERROR, LEAVING, request))
            stand_in.write(stand_in.answer(types.PYTHON_TEST_RESPONSE, BACKEND, direct.id))
            return ""

        first, second, outcomes = self.run_scripts(script)
        with SyncClient([("127.0.0.1", first.port), ("127.0.0.1", second.port)], type=peers.WEBSITE) as client:
            reply = client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=30)
        self.end_scripts(first, second)
        self.assertEqual(["", ""], outcomes)
        self.assertEqual(b"late", reply.message)
        self.assertEqual(2, first.count(types.PYTHON_TEST_REQUEST) + second.count(types.PYTHON_TEST_REQUEST))

    def test_a_request_refused_during_the_search_leaves_nothing_to_wait_for(self) -> None:
        """The connection that carried the request goes, and the search goes
        through the other two multiplexers; the backend that had the
        request refuses it as it leaves, its delivery error coming back
        through one of them, and both say nobody has a backend of the type.
        The refusal strikes the request off, and the search ends once both
        have said so: with nothing left that can answer, OperationFailed at
        once."""
        board = Board()

        def script(stand_in: StandIn) -> str:
            mxmsg = stand_in.next()
            if mxmsg is None:
                return "nothing came"
            if mxmsg.type == types.PYTHON_TEST_REQUEST:  # the request's way: it goes
                board.publish("request", mxmsg.id)
                stand_in.hang_up()
                return ""
            if mxmsg.type != types.BACKEND_FOR_PACKET_SEARCH:
                return "type %d came, not the search" % mxmsg.type
            if board.claim():  # the refusal comes back this way
                request = board.awaited("request")
                if not request:
                    return "the request's id never came"
                stand_in.write(stand_in.answer(types.DELIVERY_ERROR, LEAVING, request))
            stand_in.write(stand_in.answer(types.DELIVERY_ERROR, stand_in.id, mxmsg.id))  # nobody here
            return ""

        stand_ins = [StandIn(), StandIn(), StandIn()]
        outcomes = self.run_on(stand_ins, script)
        endpoints = [("127.0.0.1", stand_in.port) for stand_in in stand_ins]
        with SyncClient(endpoints, type=peers.WEBSITE) as client:
            with self.assertRaises(OperationFailed):
                client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=30)
        self.end_scripts(*stand_ins)
        self.assertEqual(["", "", ""], outcomes)
        self.assertEqual(1, sum(stand_in.count(types.PYTHON_TEST_REQUEST) for stand_in in stand_ins))

    def test_a_pinned_query_waiting_late_ends_when_its_connection_goes(self) -> None:
        """Through a pinned lane a late reply can come only the lane's way.
        The request draws no answer within its stage, so a backend may have
        it; the search through the lane finds nobody, and the query waits
        for that late reply; then the lane's connection goes: NotConnected,
        at once, where the wait ran on to the end of its stage,
        OperationTimedOut. The one case a timer drives: a pinned query
        reaches the late wait only once its request's stage has run out, a
        loss through a pinned lane ending the query at once. The stand-in
        acts once the search comes, which says the timer fired, so what is
        left to time is two frames on one socket, read within the search's
        2 s."""
        only = StandIn()
        self.addCleanup(only.close)
        outcome = ["not over"]

        def script() -> str:
            request = only.next()
            if request is None or request.type != types.PYTHON_TEST_REQUEST:
                return "the request did not come"
            search = only.next()
            if search is None or search.type != types.BACKEND_FOR_PACKET_SEARCH:
                return "no search came"
            only.write(only.answer(types.DELIVERY_ERROR, only.id, search.id))  # nobody
            only.hang_up()
            return ""

        thread = threading.Thread(target=lambda: outcome.__setitem__(0, script()), daemon=True)
        thread.start()
        with SyncClient([("127.0.0.1", only.port)], type=peers.WEBSITE) as client:
            with self.assertRaises(NotConnected):
                client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=2, multiplexer=client.lane(pinned=True))
        only.hang_up()
        thread.join(60)
        self.assertEqual([""], outcome)
        self.assertEqual(1, only.count(types.PYTHON_TEST_REQUEST))

    def test_a_request_whose_send_ran_out_of_time_is_still_an_attempt(self) -> None:
        """The request waits unwritten, behind what a lane sent before it,
        on a connection whose multiplexer reads nothing, past its stage's
        time; the search goes out, and the other multiplexer says nobody
        has a backend of the type; then the first catches up, reads the
        request, written at last, and answers it: that reply is the query's
        answer, the request having gone out once. It recorded the request
        only once its send was done and threw that reply away,
        OperationTimedOut. A timer drives the first
        stage, as only a stage that runs out of time leaves a request
        unwritten; the rest are the scripts' turns, within the search's 3 s."""
        first, second = StandIn(stalled=True), StandIn()
        self.addCleanup(first.close)
        self.addCleanup(second.close)

        def nobody() -> str:
            search = second.next()
            if search is None or search.type != types.BACKEND_FOR_PACKET_SEARCH:
                return "no search came"
            second.write(second.answer(types.DELIVERY_ERROR, second.id, search.id))
            first.release()  # the first multiplexer catches up
            return ""

        def answers() -> str:
            while (mxmsg := first.next()) is not None:
                if mxmsg.type == types.PYTHON_TEST_REQUEST:
                    first.write(first.answer(types.PYTHON_TEST_RESPONSE, BACKEND, mxmsg.id))
                    return ""
            return "the request never came"

        outcomes: list[str] = []
        with SyncClient([("127.0.0.1", first.port)], type=peers.WEBSITE) as client:
            lane = client.lane()
            chunk = b"x" * 65536
            for _ in range(frames_past_the_sockets(len(chunk))):
                client.send_message(chunk, type=types.TEST_EVENT, multiplexer=lane)
            client.connect(("127.0.0.1", second.port))
            threads = [in_thread(nobody, outcomes), in_thread(answers, outcomes)]
            reply = client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=3, multiplexer=lane)
        first.hang_up()
        second.hang_up()
        for thread in threads:
            thread.join(60)
        self.assertEqual(["", ""], outcomes)
        self.assertEqual(b"late", reply.message)
        self.assertEqual(1, first.count(types.PYTHON_TEST_REQUEST) + second.count(types.PYTHON_TEST_REQUEST))

    def test_a_request_its_send_gave_up_is_struck_off_the_late_wait(self) -> None:
        """The request waits for room behind a full queue, on a connection
        whose multiplexer reads nothing, past its stage's time, and the
        client gives it up; the search goes out behind it, and once the
        first multiplexer catches up, both say nobody has a backend of the
        type. The request went nowhere, so nothing can answer:
        OperationFailed at once, not a wait for a late reply that cannot
        come. The first stage runs out on its timer, as in the case above;
        the first multiplexer catches up once the client counts the request
        given up."""
        first, second = StandIn(stalled=True), StandIn()
        self.addCleanup(first.close)
        self.addCleanup(second.close)
        with SyncClient([("127.0.0.1", first.port)], type=peers.WEBSITE) as client:

            def nobody() -> str:
                search = second.next()
                if search is None or search.type != types.BACKEND_FOR_PACKET_SEARCH:
                    return "no search came"
                second.write(second.answer(types.DELIVERY_ERROR, second.id, search.id))
                for _ in range(3000):  # a bound on the wait only detects a failure
                    if client.dropped:
                        break
                    time.sleep(0.01)
                first.release()  # the first multiplexer catches up
                return "" if client.dropped else "the request was never given up"

            def answers() -> str:
                while (mxmsg := first.next()) is not None:
                    if mxmsg.type == types.PYTHON_TEST_REQUEST:
                        return "the request was written after all"
                    if mxmsg.type == types.BACKEND_FOR_PACKET_SEARCH:
                        first.write(first.answer(types.DELIVERY_ERROR, first.id, mxmsg.id))
                        return ""
                return "the search never came"

            outcomes: list[str] = []
            lane = client.lane()
            chunk = b"x" * 65536
            for _ in range(frames_past_the_sockets(len(chunk))):
                client.send_message(chunk, type=types.TEST_EVENT, multiplexer=lane)
            for _ in range(1024 + 64):  # the connection's queue full, at 1024: what follows waits for room
                client.send_message(b"x" * 16, type=types.TEST_EVENT, multiplexer=lane)
            client.connect(("127.0.0.1", second.port))
            threads = [in_thread(nobody, outcomes), in_thread(answers, outcomes)]
            with self.assertRaises(OperationFailed):
                client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=3, multiplexer=lane)
        first.hang_up()
        second.hang_up()
        for thread in threads:
            thread.join(60)
        self.assertEqual(["", ""], sorted(outcomes))

    def test_the_request_sent_again_keeps_its_workflow(self) -> None:
        """The request a query sends again, to the backend its search found,
        is a copy of the request, its workflow and every other field kept,
        with a fresh id, where it was built anew from the type and payload
        alone and the backend lost the caller's workflow. The request is a
        whole message, which the query takes as it is."""
        only = StandIn()
        self.addCleanup(only.close)

        def script() -> str:
            request = only.next()
            if request is None or request.type != types.PYTHON_TEST_REQUEST:
                return "the request did not come"
            only.write(only.answer(types.DELIVERY_ERROR, only.id, request.id))  # nobody takes it
            search = only.next()
            if search is None or search.type != types.BACKEND_FOR_PACKET_SEARCH:
                return "no search came"
            only.write(only.answer(types.PING, BACKEND, search.id))
            direct = only.next()
            if direct is None or direct.type != types.PYTHON_TEST_REQUEST or direct.to != BACKEND:
                return "no direct request came"
            if direct.workflow != b"trace":
                return "the request sent again had the workflow %r" % direct.workflow
            only.write(only.answer(types.PYTHON_TEST_RESPONSE, BACKEND, direct.id))
            return ""

        outcomes: list[str] = []
        thread = in_thread(script, outcomes)
        with SyncClient([("127.0.0.1", only.port)], type=peers.WEBSITE) as client:
            request = client.new_message(type=types.PYTHON_TEST_REQUEST, message=b"question", workflow=b"trace")
            reply = client.query(request, timeout=30)
        only.hang_up()
        thread.join(60)
        self.assertEqual([""], outcomes)
        self.assertEqual(b"late", reply.message)

    def test_an_addressed_query_waiting_for_room_at_its_deadline_times_out(self) -> None:
        """An addressed query whose request waits for room behind a full
        queue, on a connection whose multiplexer reads nothing, past the
        query's one deadline, a connection live all along:
        OperationTimedOut, the query's time up, not NotConnected, which
        would say no connection was there. Nothing pinned it. The deadline
        is what is tested, so a timer ends the query."""
        only = StandIn(stalled=True)
        self.addCleanup(only.close)
        with SyncClient([("127.0.0.1", only.port)], type=peers.WEBSITE) as client:
            lane = client.lane()
            chunk = b"x" * 65536
            for _ in range(frames_past_the_sockets(len(chunk))):
                client.send_message(chunk, type=types.TEST_EVENT, multiplexer=lane)
            for _ in range(1024 + 64):  # the connection's queue full, at 1024: what follows waits for room
                client.send_message(b"x" * 16, type=types.TEST_EVENT, multiplexer=lane)
            with self.assertRaises(OperationTimedOut):
                client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=1, to=BACKEND, multiplexer=lane)
            only.hang_up()

    def test_a_search_waits_on_when_the_connection_it_waited_for_goes_at_once(self) -> None:
        """The request's connection goes, and the search waits for a
        connection; the reconnect is counted up and lost before the search
        is placed on it. The search waits on and goes out on the next one,
        which says nobody has a backend of the type, and the request, which
        a backend may have, is answered there, where the search was placed
        on nothing after that one wait and the query waited out the stage,
        OperationTimedOut. The two waits for a reconnect are the client's
        own."""
        mx = ThreeConnections()
        self.addCleanup(mx.close)
        with SyncClient([("127.0.0.1", mx.port)], type=peers.WEBSITE) as client:
            reply = client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=30)
        mx.thread.join(60)
        self.assertEqual("", mx.outcome)
        self.assertEqual(b"late", reply.message)

    def test_a_request_too_big_to_send_again_is_refused_at_the_call(self) -> None:
        """A request whose copy sent again, with an id and a `to` of the most
        bytes and a delivery error asked for, would be one byte over the
        limit is refused at the call with ValueError, before anything goes
        out, where it went out and failed at the third stage, or, with no
        connection, waited for one and raised NotConnected."""
        with SyncClient([], type=peers.WEBSITE) as client:
            request = client.new_message(type=types.PYTHON_TEST_REQUEST, message=b"")
            request.id = request.to = 2**64 - 1
            request.report_delivery_error = True
            request.message = b"p" * (MAX_MESSAGE_SIZE - request.ByteSize() - 16)
            request.message = b"p" * (len(request.message) + MAX_MESSAGE_SIZE + 1 - request.ByteSize())
            request.ClearField("to")
            request.ClearField("report_delivery_error")
            with self.assertRaises(ValueError):
                client.query(request, timeout=0.5)


if __name__ == "__main__":
    unittest.main()
