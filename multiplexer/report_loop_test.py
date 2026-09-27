"""A handler that raises on a message answering another, a reply or a
report, has its exception logged and sends no report: two backends whose
handlers raise on what they do not expect answered each other's
BACKEND_ERRORs for good, and two pickle servers each other's replies. And a
Python plain backend that reads a BACKEND_ERROR serves on, where
serve_forever() raised BackendError and the backend was gone.

Each case is ordered, not timed: X sends Y a marker only after it handled
what it would have answered, through the same connection, so Y reaches
the marker after any answer X sent.
"""

import pickle
import threading
import unittest
from typing import Any

from multiplexer.multiplexer_constants import peers, types
from multiplexer.servers import BaseMultiplexerServer, MultiplexerServer
from multiplexer.testing import BackendThread, Cluster, runfile
from multiplexer.threaded_server import BaseThreadedMultiplexerServer

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST  # what X sends Y, and Y raises on
MARKER = types.PYTHON_TEST_RESPONSE  # the last thing X sends Y


class Seen:
    """What one backend saw, in order, and an event per type; from any thread."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.types: list[int] = []
        self.arrived: dict[int, threading.Event] = {}

    def saw(self, message_type: int) -> None:
        with self.lock:
            self.types.append(message_type)
            self.arrived.setdefault(message_type, threading.Event()).set()

    def wait_for(self, message_type: int, timeout: float = 10) -> bool:
        with self.lock:
            event = self.arrived.setdefault(message_type, threading.Event())
        return event.wait(timeout)

    def count(self, message_type: int) -> int:
        with self.lock:
            return self.types.count(message_type)


class Plain(BaseMultiplexerServer):
    """A plain backend whose handler raises on every type but the marker.
    `peer` is the instance it sends the event to at its first poll, and
    the marker to once its handler raised on a BACKEND_ERROR."""

    def __init__(self, addresses: list[tuple[str, int]], seen: Seen):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)
        self.seen = seen
        self.peer = 0
        self.sent_event = False
        self.handled_report = False
        self.sent_marker = False

    def handle_message(self, mxmsg: Any) -> None:
        self.seen.saw(mxmsg.type)
        if mxmsg.type == MARKER:
            self.no_response()
            return
        raise ValueError("unexpected type %d" % mxmsg.type)

    def on_handler_exception(self, exc: Exception) -> bool:
        """After the report, if any: the marker goes behind it."""
        if self.last_mxmsg is not None and self.last_mxmsg.type == types.BACKEND_ERROR:
            self.handled_report = True
        return True

    def periodic_task(self) -> None:
        if self.peer and not self.sent_event:
            self.sent_event = True
            self.send_message(message=b"unexpected", type=EVENT, to=self.peer)
        if self.handled_report and not self.sent_marker:
            self.sent_marker = True
            self.send_message(message=b"marker", type=MARKER, to=self.peer)


class Threaded(BaseThreadedMultiplexerServer):
    """The same backend on one worker thread."""

    def __init__(self, addresses: list[tuple[str, int]], seen: Seen):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)
        self.seen = seen
        self.peer = 0
        self.sent_event = False
        self.raised_on_report = False  # the one worker's
        self.handled_report = threading.Event()
        self.sent_marker = False

    def handle_message(self, request: Any) -> None:
        self.seen.saw(request.mxmsg.type)
        if request.mxmsg.type == MARKER:
            request.no_response()
            return
        self.raised_on_report = request.mxmsg.type == types.BACKEND_ERROR
        raise ValueError("unexpected type %d" % request.mxmsg.type)

    def on_handler_exception(self, exc: Exception) -> bool:
        """After the report, if any, went to the client: the marker goes behind it."""
        if self.raised_on_report:
            self.handled_report.set()
        return True

    def periodic_task(self) -> None:
        if self.peer and not self.sent_event:
            self.sent_event = True
            self.client.send_message(b"unexpected", type=EVENT, to=self.peer)
        if self.handled_report.is_set() and not self.sent_marker:
            self.sent_marker = True
            self.client.send_message(b"marker", type=MARKER, to=self.peer)


class Pickles(MultiplexerServer):
    """A pickle server that records what process_pickle() is given; `peer`
    gets a request at the first poll, and the marker once X processed the
    answer to it."""

    def __init__(self, addresses: list[tuple[str, int]], seen: list[Any]):
        super().__init__(addresses, peers.PYTHON_TEST_SERVER)
        self.given = seen
        self.marker = threading.Event()
        self.peer = 0
        self.sent_request = False
        self.sent_marker = False

    def process_pickle(self, data: Any) -> Any:
        self.given.append(data)
        if data == "marker":
            self.marker.set()
        return {"answering": data}

    def periodic_task(self) -> None:
        if self.peer and not self.sent_request:
            self.sent_request = True
            self.send_message(message=pickle.dumps("request"), type=EVENT, to=self.peer)
        if not self.sent_marker and any(isinstance(data, dict) for data in self.given):
            self.sent_marker = True
            self.send_message(message=pickle.dumps("marker"), type=EVENT, to=self.peer)


def instance_of(backend: Any) -> int:
    """A backend's instance id, the plain class's through its client."""
    return backend.instance_id if isinstance(backend, BaseThreadedMultiplexerServer) else backend.conn.instance_id


class ReportLoopTest(unittest.TestCase):
    def check_classes(self, factory: Any) -> None:
        """X sends Y an event; Y raises on it and reports to X, X raises on
        the report and must not report back; then X's marker reaches Y,
        behind anything X sent."""
        with Cluster(1, rules=RULES) as cluster:
            x_seen, y_seen = Seen(), Seen()
            y_thread = BackendThread(lambda: factory(cluster.endpoints, y_seen), poll=0.05)
            x_thread = BackendThread(lambda: factory(cluster.endpoints, x_seen), poll=0.05)
            with y_thread, x_thread:
                assert y_thread.backend is not None and x_thread.backend is not None
                x_thread.backend.peer = instance_of(y_thread.backend)
                self.assertTrue(y_seen.wait_for(MARKER), "the marker never came")
                self.assertTrue(x_thread.running, "X still serves")
            self.assertEqual(1, x_seen.count(types.BACKEND_ERROR), "X got Y's report, once")
            self.assertEqual(0, y_seen.count(types.BACKEND_ERROR), "X did not report on Y's report")

    def test_plain_backends_answer_no_report_with_a_report(self) -> None:
        """Also: a plain Python backend that reads a BACKEND_ERROR serves on."""
        self.check_classes(Plain)

    def test_threaded_backends_answer_no_report_with_a_report(self) -> None:
        self.check_classes(Threaded)

    def test_pickle_servers_answer_no_reply_with_a_reply(self) -> None:
        """X's request gets Y's reply, which process_pickle() sees on X, and
        X sends nothing back: Y is given the request and the marker only,
        each answered once by Y, X answering neither answer."""
        with Cluster(1, rules=RULES) as cluster:
            x_given: list[Any] = []
            y_given: list[Any] = []
            y_thread = BackendThread(lambda: Pickles(cluster.endpoints, y_given), poll=0.05)
            x_thread = BackendThread(lambda: Pickles(cluster.endpoints, x_given), poll=0.05)
            with y_thread, x_thread:
                assert y_thread.backend is not None and x_thread.backend is not None
                x_thread.backend.peer = instance_of(y_thread.backend)
                self.assertTrue(y_thread.backend.marker.wait(10), "the marker never came")
            self.assertEqual(["request", "marker"], y_given, "X answered none of Y's replies")
            self.assertEqual({"answering": "request"}, x_given[0], "process_pickle() saw Y's reply on X")


if __name__ == "__main__":
    unittest.main()
