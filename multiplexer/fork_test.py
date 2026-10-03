"""Clients inherited across fork() are orphans in the child.

Every call on them raises UsedAfterFork, dropping them neither hangs nor
touches the parent's connections, a fresh client in the child works, and
the parent's clients are untouched. See lib/fork.h. The calls raise before
they write anything on the connection the parent still uses, and before
they take a lock a parent thread may have held at the instant of the fork:
a parent thread holds the lock while the main thread forks. And the count
of io threads waiting for the interpreter's lock starts at zero in a child.
"""

import asyncio
import gc
import os
import signal
import subprocess
import sys
import threading
import time
import unittest
import warnings
import weakref
from collections.abc import Callable

from multiplexer import _native
from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationFailed, UsedAfterFork
from multiplexer.threaded_client import ThreadedClient
from multiplexer.threaded_server import BaseThreadedMultiplexerServer


def runfile(path: str) -> str:
    """A file of this repository inside the test's runfiles tree."""
    return os.path.join(os.environ["TEST_SRCDIR"], os.environ.get("TEST_WORKSPACE", "mx"), path)


def in_child(checks: list[tuple[str, Callable[[], object]]]) -> tuple[int, str]:
    """Forks; the child runs each check, one `name: outcome` line each (the
    exception's type, or "returned"), under an alarm that ends it if a
    check waits for good. Returns the child's exit code, minus the signal
    that ended it, and its report."""
    read_end, write_end = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(read_end)
        signal.alarm(10)
        lines = []
        for name, check in checks:
            try:
                check()
                lines.append("%s: returned" % name)
            except BaseException as error:  # the outcome is the point
                lines.append("%s: %s" % (name, type(error).__name__))
            os.write(write_end, (lines[-1] + "\n").encode())
        os._exit(0)
    os.close(write_end)
    _, status = os.waitpid(pid, 0)
    report = b""
    while chunk := os.read(read_end, 65536):
        report += chunk
    os.close(read_end)
    return os.waitstatus_to_exitcode(status), report.decode()


def sockets() -> int:
    """How many of this process's descriptors are sockets, from /proc."""
    count = 0
    for name in os.listdir("/proc/self/fd"):
        try:
            if os.readlink(os.path.join("/proc/self/fd", name)).startswith("socket:"):
                count += 1
        except OSError:
            pass  # the directory's own descriptor, closed by now
    return count


def zero(count: int) -> None:
    """Raises AssertionError(count) unless `count` is 0: a check for in_child()."""
    if count:
        raise AssertionError(count)


class Held:
    """A lock held by a thread of this process until close(): what a fork
    finds when one of the parent's threads is inside the lock."""

    def __init__(self, lock) -> None:
        self._held = threading.Event()
        self._release = threading.Event()
        self._thread = threading.Thread(target=self._hold, args=(lock,))
        self._thread.start()
        self._held.wait()

    def _hold(self, lock) -> None:
        with lock:
            self._held.set()
            self._release.wait()

    def close(self) -> None:
        self._release.set()
        self._thread.join()


class ForkTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self):
        """Start a multiplexer on a free port."""
        self.port_file = os.path.join(os.environ["TEST_TMPDIR"], "mx.port")
        if os.path.exists(self.port_file):
            os.unlink(self.port_file)
        self.mx = subprocess.Popen(
            [
                runfile("mxcontrol/mxcontrol"),
                "run_multiplexer",
                "--address",
                "127.0.0.1:0",
                "--rules",
                runfile("tests/testing.rules"),
                "--port-file",
                self.port_file,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 15
        while not os.path.exists(self.port_file):
            self.assertLess(time.time(), deadline, "multiplexer did not start")
            time.sleep(0.02)
        with open(self.port_file) as port_file:
            host, port = port_file.read().strip().rsplit(":", 1)
        self.endpoint = (host, int(port))

    def tearDown(self):
        """Stop the multiplexer."""
        self.mx.terminate()
        self.mx.wait(10)

    def test_inherited_clients_are_orphans_and_the_parent_keeps_its_connections(self) -> None:
        """Fork with a live Client and ThreadedClient; check both sides. They
        are held by a list only, which the child empties, so that dropping
        them there frees them: held by this frame too, the drop freed
        nothing, and its "ok" checked no teardown."""
        clients = [Client([self.endpoint], type=peers.WEBSITE), ThreadedClient([self.endpoint], type=peers.WEBSITE)]
        os.environ["MX_FORK_TEST_ENDPOINT"] = "%s:%d" % self.endpoint
        read_end, write_end = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(read_end)
            self._child(clients, write_end)
        os.close(write_end)
        sync, threaded = clients
        _, status = os.waitpid(pid, 0)
        report = os.read(read_end, 65536).decode()
        self.assertEqual(0, os.waitstatus_to_exitcode(status), report)
        self.assertEqual(
            {
                "sync.query": "UsedAfterFork",
                "sync.send_message": "UsedAfterFork",
                "sync.connect": "UsedAfterFork",
                "sync.disconnect": "UsedAfterFork",
                "threaded.query": "UsedAfterFork",
                "threaded.send_message": "UsedAfterFork",
                "threaded.connect": "UsedAfterFork",
                "threaded.disconnect": "UsedAfterFork",
                "dropped": "ok",
                "fresh": "OperationFailed",
            },
            dict(line.split(": ", 1) for line in report.splitlines()),
        )
        # The parent's clients still work and are still registered: the
        # child closed only its own descriptor copies.
        time.sleep(0.5)
        self.assertEqual(1, threaded.connections_count())
        self.assertEqual(1, sync.connections_count())
        with self.assertRaises(OperationFailed):
            threaded.query(b"nobody serves this", type=types.PYTHON_TEST_REQUEST, timeout=5)
        with self.assertRaises(OperationFailed):
            sync.query(b"nobody serves this", type=types.PYTHON_TEST_REQUEST, timeout=5)
        threaded.shutdown()
        sync.shutdown()

    @staticmethod
    def _child(clients: list, write_end: int) -> None:
        """The child's checks on the SyncClient and the ThreadedClient in
        `clients`, which it empties; reports one `name: outcome` per line
        and exits."""
        report = []

        def attempt(name, call) -> None:
            """Record what `call` raised, or that it returned."""
            try:
                call()
                report.append("%s: returned" % name)
            except UsedAfterFork:
                report.append("%s: UsedAfterFork" % name)
            except Exception as error:  # the outcome is the point
                report.append("%s: %s" % (name, type(error).__name__))

        try:
            sync, threaded = clients
            host, port = os.environ["MX_FORK_TEST_ENDPOINT"].rsplit(":", 1)
            # The parent's multiplexer: dropped here, the connection the
            # parent checks after would be closed.
            parents = (host, int(port))
            attempt("sync.query", lambda sync=sync: sync.query(b"x", type=types.PYTHON_TEST_REQUEST, timeout=1))
            attempt(
                "sync.send_message", lambda sync=sync: sync.send_message(message=b"x", type=types.PYTHON_TEST_REQUEST)
            )
            attempt("sync.connect", lambda sync=sync: sync.connect(("127.0.0.1", 1)))
            attempt("sync.disconnect", lambda sync=sync: sync.disconnect(parents))
            attempt(
                "threaded.query",
                lambda threaded=threaded: threaded.query(b"x", type=types.PYTHON_TEST_REQUEST, timeout=1),
            )
            attempt(
                "threaded.send_message",
                lambda threaded=threaded: threaded.send_message(b"x", type=types.PYTHON_TEST_REQUEST),
            )
            attempt("threaded.connect", lambda threaded=threaded: threaded.connect(("127.0.0.1", 1), 0.1))
            attempt("threaded.disconnect", lambda threaded=threaded: threaded.disconnect(parents))
            # The orphan teardown, which must neither hang nor hurt the
            # parent: the clients' last references go, and they are freed.
            gone = [weakref.ref(sync), weakref.ref(threaded)]
            del sync, threaded
            clients.clear()
            gc.collect()
            report.append("dropped: %s" % ("ok" if all(client() is None for client in gone) else "still referenced"))
            fresh = ThreadedClient([parents], type=peers.WEBSITE)
            try:
                fresh.query(b"x", type=types.PYTHON_TEST_REQUEST, timeout=5)
                report.append("fresh: returned")
            except Exception as error:
                report.append("fresh: %s" % type(error).__name__)
            fresh.shutdown()
            os.write(write_end, "\n".join(report).encode())
            os._exit(0)
        except BaseException as error:  # the parent must see why the child died
            os.write(write_end, ("\n".join(report) + "\nchild failed: %r" % (error,)).encode())
            os._exit(1)

    def test_an_inherited_synchronous_client_writes_nothing_from_the_child(self) -> None:
        """The child's calls on an inherited SyncClient, from the thread
        that made it, raise UsedAfterFork and leave nothing on the
        connection the parent still uses: a backend of the type counts what
        arrives, up to a message the parent sends after the child exited.
        The getters of the connections' state raise too, rather than answer
        with the parent's."""
        received: list[bytes] = []
        parent_after_child = threading.Event()

        def on_message(mxmsg: MultiplexerMessage) -> None:
            received.append(mxmsg.message)
            if mxmsg.message == b"after the child":
                parent_after_child.set()

        backend = ThreadedClient([self.endpoint], type=peers.PYTHON_TEST_SERVER, on_message=on_message)
        sync = Client([self.endpoint], type=peers.WEBSITE)
        lane = sync.lane()
        sync.send_message(b"seeding the lane", type=types.PYTHON_TEST_REQUEST, multiplexer=lane, flush=True)
        try:
            code, report = in_child(
                [
                    ("query", lambda: sync.query(b"from the child", type=types.PYTHON_TEST_REQUEST, timeout=1)),
                    ("send", lambda: sync.send_message(b"from the child", type=types.PYTHON_TEST_REQUEST, flush=True)),
                    (
                        "lane",
                        lambda: sync.send_message(b"from the child", type=types.PYTHON_TEST_REQUEST, multiplexer=lane),
                    ),
                    ("connections_count", sync.connections_count),
                    ("has_incoming_messages", sync.has_incoming_messages),
                    ("routing_acknowledged", sync.routing_acknowledged),
                ]
            )
            sync.send_message(b"after the child", type=types.PYTHON_TEST_REQUEST, flush=True)
            parent_after_child.wait(10)
            self.assertEqual(
                (
                    0,
                    "query: UsedAfterFork\nsend: UsedAfterFork\nlane: UsedAfterFork\nconnections_count: UsedAfterFork\n"
                    "has_incoming_messages: UsedAfterFork\nrouting_acknowledged: UsedAfterFork\n",
                    0,
                    1,
                    1,
                ),
                (
                    code,
                    report,
                    received.count(b"from the child"),
                    received.count(b"after the child"),
                    sync.connections_count(),
                ),
                "the child's exit code and outcomes, what reached the backend from it and from the parent after it, "
                "and the parent's connections",
            )
        finally:
            sync.shutdown()
            backend.shutdown()

    def test_inherited_calls_raise_while_a_parent_thread_holds_their_lock(self) -> None:
        """An AsyncClient's subscriptions and a threaded server's condition
        are locks the io thread takes for every message: with a parent
        thread holding one at the fork, the child's calls raise
        UsedAfterFork rather than wait for it for good, and the two that
        clean up, an unsubscribe and stop(), return without it."""
        loop = asyncio.new_event_loop()  # never run: the calls checked need no loop
        aclient = AsyncClient([self.endpoint], peers.PYTHON_TEST_CLIENT, loop=loop)
        unsubscribe = aclient.subscribe(None, lambda mxmsg: None)
        server = BaseThreadedMultiplexerServer([self.endpoint], type=peers.PYTHON_TEST_SERVER)
        try:
            held = Held(aclient._lock)
            try:
                code, report = in_child(
                    [
                        ("subscribe", lambda: aclient.subscribe(None, lambda mxmsg: None)),
                        ("unsubscribe", unsubscribe),
                        ("messages", lambda: aclient.messages()),
                    ]
                )
            finally:
                held.close()
            self.assertEqual(
                (0, "subscribe: UsedAfterFork\nunsubscribe: returned\nmessages: UsedAfterFork\n"), (code, report)
            )
            held = Held(server._cond)
            try:
                code, report = in_child(
                    [
                        ("close", server.close),
                        ("pending", lambda: server.pending),
                        ("connect", server.connect),
                        ("serve_forever", lambda: server.serve_forever(poll=0.1)),
                        ("stop", server.stop),
                    ]
                )
            finally:
                held.close()
            self.assertEqual(
                (
                    0,
                    "close: UsedAfterFork\npending: UsedAfterFork\nconnect: UsedAfterFork\n"
                    "serve_forever: UsedAfterFork\nstop: returned\n",
                ),
                (code, report),
            )
            # The lock of the event serve_forever() waits on, which stop() sets:
            # CPython's own, so not in the event's type.
            held = Held(getattr(server._wake, "_cond"))
            try:
                code, report = in_child([("stop", server.stop)])
            finally:
                held.close()
            self.assertEqual((0, "stop: returned\n"), (code, report))
        finally:
            server.close()
            aclient.close()
            loop.close()

    def test_an_inherited_client_closes_the_childs_copies_once(self) -> None:
        """Shut down in the child and then dropped, an inherited client
        closes the child's copies of the parent's sockets once: a second
        close would close what the child opened since under the same
        numbers, a new client's sockets and timers among them. The child
        opens files until it holds every low number the copies had."""
        clients = [Client([self.endpoint], type=peers.WEBSITE), ThreadedClient([self.endpoint], type=peers.WEBSITE)]

        def shut_down_open_and_drop() -> None:
            for index in range(len(clients)):
                clients[index].shutdown()  # by index: no name keeps a client past clear()
            opened = [os.open(os.devnull, os.O_RDONLY) for _ in range(64)]
            clients.clear()  # the destructors run now
            closed = 0
            for descriptor in opened:
                try:
                    os.fstat(descriptor)
                except OSError:
                    closed += 1
            zero(closed)

        try:
            code, report = in_child([("shut down, open, drop", shut_down_open_and_drop)])
            self.assertEqual((0, "shut down, open, drop: returned\n"), (code, report))
            self.assertEqual([1, 1], [client.connections_count() for client in clients])
        finally:
            for client in clients:
                client.shutdown()

    def test_an_inherited_client_with_a_callback_is_freed_once_shut_down(self) -> None:
        """Shut down in the child, an inherited client lets go of its
        callbacks there too, and is freed when dropped, as in the parent: it
        refers to itself through on_message, a cycle the collector cannot see
        while the binding holds the callback."""
        clients = [ThreadedClient([self.endpoint], type=peers.WEBSITE, on_message=lambda mxmsg: None)]

        def shut_down_and_drop() -> None:
            clients[0].shutdown()
            weak = weakref.ref(clients[0])
            clients.clear()
            gc.collect()
            if weak() is not None:
                raise AssertionError("still referenced")

        try:
            code, report = in_child([("shut down and dropped", shut_down_and_drop)])
            self.assertEqual((0, "shut down and dropped: returned\n"), (code, report))
        finally:
            for client in clients:
                client.shutdown()

    def test_a_holders_inherited_client_is_closed_in_the_child(self) -> None:
        """AsyncClient.holder() closes the client a child inherited, which
        closes the child's copy of its one connection: dropping the client
        would close nothing, since its callback keeps it alive."""
        loop = asyncio.new_event_loop()
        holder = AsyncClient.holder(peers.PYTHON_TEST_CLIENT, [self.endpoint])
        try:
            loop.run_until_complete(holder.aget())
            before = sockets()
            code, report = in_child([("one socket fewer", lambda: zero(before - 1 - sockets()))])
            self.assertEqual((0, "one socket fewer: returned\n"), (code, report))
        finally:
            holder.close()
            loop.close()

    def test_no_io_thread_waits_for_the_interpreter_in_a_child(self) -> None:
        """A child of a busy parent starts with no io thread counted as
        waiting for the interpreter's lock: the threads waiting in the
        parent do not exist in the child, and a count left over from them
        made every child's interpreter exit wait two seconds. The parent
        holds the lock, with a switch interval long enough that it keeps
        it, until a message to itself has an io thread waiting, then forks."""
        client = ThreadedClient([self.endpoint], type=peers.PYTHON_TEST_SERVER, on_message=lambda m: None)
        interval = sys.getswitchinterval()
        sys.setswitchinterval(1000)
        try:
            waiting = 0
            for _ in range(20):
                client.send_message(b"wake", type=types.PYTHON_TEST_REQUEST, to=client.instance_id)
                deadline = time.monotonic() + 1
                while (waiting := _native._gil_takers()) == 0 and time.monotonic() < deadline:
                    pass
                if waiting:
                    break
            self.assertGreater(waiting, 0, "an io thread waiting for the interpreter's lock")
            # os.fork() warns, in the parent, that the process has threads:
            # forking beside a waiting io thread is what this test does.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                pid = os.fork()
            if pid == 0:
                os._exit(min(_native._gil_takers(), 100))
        finally:
            sys.setswitchinterval(interval)
        _, status = os.waitpid(pid, 0)
        client.shutdown()
        self.assertEqual(0, os.waitstatus_to_exitcode(status), "threads counted as waiting, in the child")


if __name__ == "__main__":
    unittest.main()
