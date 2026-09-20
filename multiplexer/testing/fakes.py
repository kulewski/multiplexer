"""In-process peers for unit tests: a scripted backend, a backend of your own
served on a thread, and a client, all against a Cluster of real multiplexers.

FakePeer stands in for a backend the code under test talks to: tell it what
to answer, run the code, then look at what it received. BackendThread serves
a BaseMultiplexerServer of yours on its own thread, the thread the client
library requires, and keeps what it raised. TestClient sends and queries
from the test itself, on the synchronous client; ThreadedTestClient does
the same on a ThreadedClient, so that a test sees what a production peer
built on one sees, and keeps what arrives on its own. They complement the
roles that run as processes (spawn): those exercise a whole program, these
keep a test in one process where it can inspect everything.
"""

import threading
import time
from typing import Any, Callable, Generic, Protocol, TypeVar

from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.clients import Client
from multiplexer.mxclient import ConnectionWrapper, Lane
from multiplexer.multiplexer_constants import types
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import Cluster, Mx, wait_until
from multiplexer.threaded_client import ThreadedClient

# What a FakePeer handler returns: the reply payload (bytes, str or a
# protocol buffer message), or None for no reply.
Handler = Callable[[MultiplexerMessage], Any]
# What a wait may select on: true for the messages meant.
Matcher = Callable[[MultiplexerMessage], bool]


class Served(Protocol):
    """What BackendThread drives: serve_forever(poll=...) until stop()."""

    def serve_forever(self, *, poll: float) -> None: ...
    def stop(self) -> None: ...


ServedT = TypeVar("ServedT", bound=Served)


class BackendThread(Generic[ServedT]):
    """A backend served on its own thread.

    `factory` builds the backend, a BaseMultiplexerServer, on that thread,
    where it is then served with serve_forever(poll); start() returns once
    it is built and connected, raising what the factory raised. stop() asks
    it to leave, joins the thread and re-raises what serving raised, so a
    failing handler fails the test. `backend` is the instance, `error` the
    exception if there was one.
    """

    def __init__(self, factory: Callable[[], ServedT], poll: float = 0.05, name: str | None = None):
        self.factory = factory
        self.poll = poll
        self.backend: ServedT | None = None
        self.error: BaseException | None = None
        self._built = threading.Event()
        self._thread = threading.Thread(target=self._run, name=name or "mx-backend", daemon=True)

    def start(self, timeout: float = 15) -> "BackendThread":
        """Start the thread and wait until the backend is built and connected."""
        self._thread.start()
        if not self._built.wait(timeout):
            raise TimeoutError("the backend was not built within %ss" % timeout)
        if self.backend is None:
            assert self.error is not None
            raise self.error
        return self

    def _run(self) -> None:
        """The thread: build, announce, serve, keep what went wrong."""
        try:
            self.backend = self.factory()
        except BaseException as error:  # reported by start()
            self.error = error
            self._built.set()
            return
        self._built.set()
        try:
            self.backend.serve_forever(poll=self.poll)
        except BaseException as error:  # reported by stop()
            self.error = error

    @property
    def running(self) -> bool:
        """Whether the thread is still serving."""
        return self._thread.is_alive()

    def stop(self, timeout: float = 10) -> None:
        """Ask the backend to leave, join the thread, re-raise what serving raised."""
        if self.backend is not None:
            self.backend.stop()
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError("the backend thread did not stop within %ss" % timeout)
        if self.error is not None:
            raise self.error

    def __enter__(self) -> "BackendThread[ServedT]":
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()


class _ScriptedBackend(BaseMultiplexerServer):
    """The backend behind a FakePeer: records every message, answers the
    types it has a handler for, drops the rest without a warning."""

    def __init__(self, peer: "FakePeer", addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peer.peer_type)
        self.peer = peer

    def handle_message(self, mxmsg: MultiplexerMessage) -> None:
        """Record `mxmsg` and the multiplexer it came through, then reply as
        scripted or declare no response."""
        peer = self.peer
        assert self.last_connwrap is not None
        via = peer.cluster.multiplexer_at(self.last_connwrap.endpoint)
        with peer._cond:
            peer.received.append(mxmsg)
            peer._via[id(mxmsg)] = via
            peer._cond.notify_all()
        handler = peer._handlers.get(mxmsg.type)
        if handler is None:
            self.no_response()
            return
        handler, reply_type = handler
        payload = handler(mxmsg)
        if payload is None:
            self.no_response()
        else:
            self.send_message(message=payload, type=reply_type if reply_type is not None else mxmsg.type, flush=True)

    def on_handler_exception(self, exc: Exception) -> bool:
        """Keep the exception for FakePeer.stop() to raise; keep serving."""
        self.peer.errors.append(exc)
        return True

    def should_respond_to_backend_for_packet_search(self) -> bool:
        """Decline while the peer plays a draining backend."""
        return not self.peer.declining_searches and super().should_respond_to_backend_for_packet_search()


class FakePeer:
    """A scripted backend of `peer_type`, connected to every multiplexer of
    `cluster`, served on its own thread.

    Script it before start(): reply_with() answers every request of a type
    with a fixed payload, on() with what a handler returns; a type with no
    handler is received and dropped. `received` is every message in
    arrival order, messages(type) those of one type, wait_for(type, count)
    blocks until enough have arrived, via(mxmsg) is the multiplexer a
    message came through and arrivals(type) pairs each message with it. A
    handler that raises fails the test at stop(), which also joins the
    thread; the requester meanwhile got a BACKEND_ERROR. Use as a context
    manager or call start() and stop().
    """

    def __init__(
        self,
        cluster: Cluster,
        peer_type: int,
        name: str | None = None,
        endpoints: list[tuple[str, int]] | None = None,
    ):
        """`endpoints` restricts the peer to some of the cluster's
        multiplexers, for a test of a peer that is behind one only."""
        self.cluster = cluster
        self.peer_type = peer_type
        self.name = name or "fake-peer-%d" % peer_type
        self.endpoints = list(endpoints) if endpoints is not None else list(cluster.endpoints)
        self.received: list[MultiplexerMessage] = []
        self.errors: list[Exception] = []
        self._handlers: dict[int, tuple[Handler, int | None]] = {}
        self._via: dict[int, Mx] = {}  # id(mxmsg) -> the Mx it came through
        # True: decline every search for a backend, as a draining backend
        # does, while still serving what arrives.
        self.declining_searches = False
        self._cond = threading.Condition()
        self._thread = BackendThread(self._build, name=self.name)

    def _build(self) -> BaseMultiplexerServer:
        """The backend, built on the serving thread."""
        return _ScriptedBackend(self, self.endpoints)

    @property
    def backend(self) -> BaseMultiplexerServer | None:
        """The BaseMultiplexerServer behind the peer, once started; for what
        the script does not cover, such as sending from a handler."""
        return self._thread.backend

    @property
    def instance_id(self) -> int:
        """The peer's instance id, once started: what a client addresses with `to`."""
        backend = self.backend
        assert backend is not None, "not started"
        return backend.conn.instance_id

    def via(self, mxmsg: MultiplexerMessage) -> Mx:
        """The multiplexer a received message came through, so that a test
        can say "all of these came the same way" without reading a recording."""
        with self._cond:
            return self._via[id(mxmsg)]

    def arrivals(self, type: int, matching: Matcher | None = None) -> list[tuple[MultiplexerMessage, Mx]]:
        """Every message of `type` received so far paired with the
        multiplexer it came through, in arrival order; with `matching`,
        those for which `matching(mxmsg)` is true."""
        with self._cond:
            return [
                (mxmsg, self._via[id(mxmsg)])
                for mxmsg in self.received
                if mxmsg.type == type and (matching is None or matching(mxmsg))
            ]

    def on(self, request_type: int, handler: Handler, reply_type: int | None = None) -> "FakePeer":
        """Answer every message of `request_type` with what `handler(mxmsg)`
        returns: a payload, sent as a reply of `reply_type` (the request's
        type by default), or None for no reply."""
        self._handlers[request_type] = (handler, reply_type)
        return self

    def reply_with(self, request_type: int, payload: Any, reply_type: int | None = None) -> "FakePeer":
        """Answer every message of `request_type` with `payload` (bytes, str
        or a protocol buffer message) as a reply of `reply_type`, the
        request's type by default."""
        return self.on(request_type, lambda _: payload, reply_type)

    def start(self, timeout: float = 15) -> "FakePeer":
        """Connect and serve; returns once every multiplexer the peer connects to has it registered."""
        self._thread.start(timeout)
        if len(self.endpoints) == len(self.cluster.endpoints):
            self.cluster.wait_for_peer(self.peer_type, timeout=timeout)
        else:
            for endpoint in self.endpoints:
                multiplexer = self.cluster.multiplexer_at(endpoint)
                wait_until(
                    lambda: any(number == self.peer_type for _, _, number in multiplexer.connected_peers()),
                    timeout,
                    "%s registered on multiplexer %d" % (self.name, multiplexer.index),
                )
        return self

    def stop(self, timeout: float = 10) -> None:
        """Leave and join the thread; raises the first exception a handler raised."""
        self._thread.stop(timeout)
        if self.errors:
            raise self.errors[0]

    def messages(self, type: int, matching: Matcher | None = None) -> list[MultiplexerMessage]:
        """Every message of `type` received so far; with `matching`, those
        for which `matching(mxmsg)` is true."""
        with self._cond:
            return [mxmsg for mxmsg in self.received if mxmsg.type == type and (matching is None or matching(mxmsg))]

    def wait_for(
        self, type: int, count: int = 1, timeout: float = 10, matching: Matcher | None = None
    ) -> list[MultiplexerMessage]:
        """Block until at least `count` messages of `type` have arrived and
        return them; with `matching`, only the ones for which
        `matching(mxmsg)` is true count and are returned, so a wait names
        the message it means rather than a total, which a request the
        client retried can reach early. TimeoutError names the type and how
        many came."""
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                found = [
                    mxmsg for mxmsg in self.received if mxmsg.type == type and (matching is None or matching(mxmsg))
                ]
                if len(found) >= count:
                    return found
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "%s: %d of %d message(s) of type %d%s within %ss"
                        % (self.name, len(found), count, type, " matching" if matching else "", timeout)
                    )
                self._cond.wait(min(remaining, 0.5))

    def __enter__(self) -> "FakePeer":
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()


class TestClient:
    """A client of `peer_type` connected to every multiplexer of `cluster`,
    for the test's own sending and querying; `client` is the underlying
    multiplexer.clients.Client for the rest of its API. Like every
    synchronous client it runs the loop only inside calls, so its peer type
    should be is_passive in the rules file."""

    def __init__(self, cluster: Cluster, peer_type: int):
        self.cluster = cluster
        self.peer_type = peer_type
        self.client = Client(cluster.endpoints, type=peer_type)

    @property
    def instance_id(self) -> int:
        """The client's instance id, what a peer addresses with `to`."""
        return self.client.instance_id

    def lane(self, pinned: bool = False, connection: ConnectionWrapper | None = None) -> Lane:
        """A Lane for `multiplexer=`: one connection for a stream of messages; Client.lane() says the rest."""
        return self.client.lane(pinned, connection)

    def send(
        self,
        payload: Any,
        type: int,
        to: int = 0,
        flush: bool = True,
        multiplexer: int | Lane | ConnectionWrapper = Client.ONE,
        **kwargs: Any,
    ) -> int:
        """Send `payload` (bytes, str or a protocol buffer message) as a
        message of `type`, routed by the rules or to instance `to`, through
        one connection, a Lane's or a ConnectionWrapper's; flushed to the
        socket by default. Returns the message id."""
        if to:
            kwargs["to"] = to
        return self.client.send_message(payload, type=type, flush=flush, multiplexer=multiplexer, **kwargs)

    def query(
        self,
        payload: Any,
        type: int,
        timeout: float = 10,
        to: int = 0,
        probe: int = types.BACKEND_FOR_PACKET_SEARCH,
        multiplexer: int | Lane | ConnectionWrapper = Client.ONE,
        with_connection: bool = False,
    ) -> Any:
        """Send `payload` as a request of `type` and return the reply; with
        `to`, addressed to that instance, located with `probe` when it
        moved; through a Lane or a ConnectionWrapper as `multiplexer`;
        (reply, connection) with `with_connection`. Client.query() says the rest."""
        return self.client.query(
            payload,
            type=type,
            timeout=timeout,
            to=to,
            probe=probe,
            multiplexer=multiplexer,
            with_connection=with_connection,
        )

    def receive(self, timeout: float = 10) -> MultiplexerMessage:
        """The next message addressed to this client."""
        return self.client.read_message(timeout=timeout)

    def shutdown(self) -> None:
        """Close the connections."""
        self.client.shutdown()

    def __enter__(self) -> "TestClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.shutdown()


class ThreadedTestClient:
    """A client of `peer_type` shaped like a production peer built on
    ThreadedClient: an io thread of its own, an active peer type, replies
    matched to queries by id, a late reply to a query it has seen answered
    dropped, a search addressed to it answered. What arrives on its own,
    events and requests addressed to it, is kept: `received` in arrival
    order, messages(type, matching=None), wait_for(type, count=1,
    timeout=10, matching=None), via(mxmsg) and arrivals(type), as on
    FakePeer. TestClient, on the synchronous client, never drops a late
    reply and never answers a search, so a test of a threaded peer that
    passes on it may not pass in production; this one shows what
    production shows. `client` is the ThreadedClient underneath.
    """

    def __init__(self, cluster: Cluster, peer_type: int, name: str | None = None):
        self.cluster = cluster
        self.peer_type = peer_type
        self.name = name or "threaded-test-client-%d" % peer_type
        self.received: list[MultiplexerMessage] = []
        self._via: dict[int, Mx] = {}
        self._cond = threading.Condition()
        self.client = ThreadedClient(
            cluster.endpoints, type=peer_type, on_message=self._on_message, with_connection=True
        )

    def _on_message(self, mxmsg: MultiplexerMessage, connection: ConnectionWrapper) -> None:
        """The io thread: keep the message and the multiplexer it came through."""
        via = self.cluster.multiplexer_at(connection.endpoint)
        with self._cond:
            self.received.append(mxmsg)
            self._via[id(mxmsg)] = via
            self._cond.notify_all()

    @property
    def instance_id(self) -> int:
        """The client's instance id, what a peer addresses with `to`."""
        return self.client.instance_id

    def lane(self, pinned: bool = False, connection: ConnectionWrapper | None = None) -> Lane:
        """A Lane for `multiplexer=`; ThreadedClient.lane() says the rest."""
        return self.client.lane(pinned, connection)

    def send(
        self,
        payload: Any,
        type: int,
        to: int = 0,
        flush: bool = True,
        multiplexer: int | Lane | ConnectionWrapper = ThreadedClient.ONE,
        **kwargs: Any,
    ) -> int:
        """Send `payload` as a message of `type`, routed by the rules or to
        instance `to`, through one connection, a Lane's or a
        ConnectionWrapper's; waited for by default. Returns the message id."""
        if to:
            kwargs["to"] = to
        return self.client.send_message(payload, type=type, flush=flush, multiplexer=multiplexer, **kwargs)

    def query(
        self,
        payload: Any,
        type: int,
        timeout: float = 10,
        to: int = 0,
        probe: int = types.BACKEND_FOR_PACKET_SEARCH,
        multiplexer: int | Lane | ConnectionWrapper = ThreadedClient.ONE,
        with_connection: bool = False,
    ) -> Any:
        """Send `payload` as a request of `type` and return the reply, or
        raise as ThreadedClient.query() does; its `to`, `probe`,
        `multiplexer` and `with_connection`."""
        return self.client.query(
            payload,
            type,
            timeout,
            to=to,
            probe=probe,
            multiplexer=multiplexer,
            with_connection=with_connection,
        )

    def messages(self, type: int, matching: Matcher | None = None) -> list[MultiplexerMessage]:
        """Every message of `type` that arrived on its own so far; with `matching`, those it selects."""
        with self._cond:
            return [mxmsg for mxmsg in self.received if mxmsg.type == type and (matching is None or matching(mxmsg))]

    def wait_for(
        self, type: int, count: int = 1, timeout: float = 10, matching: Matcher | None = None
    ) -> list[MultiplexerMessage]:
        """Block until at least `count` messages of `type` (that `matching`
        selects) have arrived and return them; TimeoutError names the type
        and how many came."""
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                found = [
                    mxmsg for mxmsg in self.received if mxmsg.type == type and (matching is None or matching(mxmsg))
                ]
                if len(found) >= count:
                    return found
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "%s: %d of %d message(s) of type %d%s within %ss"
                        % (self.name, len(found), count, type, " matching" if matching else "", timeout)
                    )
                self._cond.wait(min(remaining, 0.5))

    def via(self, mxmsg: MultiplexerMessage) -> Mx:
        """The multiplexer a received message came through."""
        with self._cond:
            return self._via[id(mxmsg)]

    def arrivals(self, type: int, matching: Matcher | None = None) -> list[tuple[MultiplexerMessage, Mx]]:
        """Every message of `type` received paired with the multiplexer it came through, in arrival order."""
        with self._cond:
            return [
                (mxmsg, self._via[id(mxmsg)])
                for mxmsg in self.received
                if mxmsg.type == type and (matching is None or matching(mxmsg))
            ]

    def shutdown(self) -> None:
        """Fail what is in flight, close the connections, stop the io thread."""
        self.client.shutdown()

    def __enter__(self) -> "ThreadedTestClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.shutdown()
