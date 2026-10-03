"""AsyncClient against a real multiplexer and a scripted backend: every
verb, the exceptions, concurrency, cancellation, subscriptions, what
arrived before a reply handled before its await resumes, the queue and its
end at close(), a multiplexer restart, the loop rule, a create() given up
on, a holder's client whose loop has closed, a fork, and the interpreter's
exit;
and sends as on every client, against a frozen multiplexer: the await
returns once the io thread has the message, where it waited for the
write, flush=True waits for the write, a callback hears how the message
ended, and flush_all() is awaited. Every wait is on an event or a count:
a handler that ends it, a sentinel message behind the one that must not
arrive, the warnings read back from the client's log.
"""

import asyncio
import contextlib
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from typing import Iterator
from unittest import mock

from multiplexer import aio
from multiplexer.aio import AsyncClient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import NotConnected, OperationFailed, OperationTimedOut
from multiplexer.mxlog import WARNING
from multiplexer.testing import Cluster, FakePeer, TestClient
from multiplexer.testing import runfile
from multiplexer.testing.buffers import past_the_queue
from multiplexer.threaded_client import BackendError

RULES = runfile("tests/testing.rules")  # the file the constants were generated from


@contextlib.contextmanager
def stderr_to(path: str) -> Iterator[None]:
    """Descriptor 2, which the C++ side of an in-process client logs to,
    goes to `path` meanwhile."""
    sys.stderr.flush()
    saved = os.dup(2)
    with open(path, "wb") as target:
        os.dup2(target.fileno(), 2)
    try:
        yield
    finally:
        sys.stderr.flush()
        os.dup2(saved, 2)
        os.close(saved)


class AsyncClientTest(unittest.IsolatedAsyncioTestCase):
    """One multiplexer for the class; a fake backend and a client per test."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(1, rules=RULES).__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.__exit__(None, None, None)

    async def asyncSetUp(self):
        self.peer = FakePeer(self.cluster, peers.PYTHON_TEST_SERVER).start()
        self.peer.on(types.PYTHON_TEST_REQUEST, self.answer, types.PYTHON_TEST_RESPONSE)
        self.client = AsyncClient(self.cluster.endpoints, peers.PYTHON_TEST_CLIENT)

    async def asyncTearDown(self):
        await self.client.aclose()
        self.peer.stop()

    @staticmethod
    def answer(mxmsg):
        """The fake's script: upper-case, echo, nothing, or an error, by payload."""
        if mxmsg.message == b"":
            return None
        if mxmsg.message == b"raise":
            raise ValueError("as asked")
        return mxmsg.message.upper()

    async def test_query_awaits_the_reply(self):
        reply = await self.client.query(b"pears", types.PYTHON_TEST_REQUEST)
        self.assertEqual((types.PYTHON_TEST_RESPONSE, b"PEARS"), (reply.type, reply.message))
        self.assertEqual(b"PEARS", (await self.client.query("pears", types.PYTHON_TEST_REQUEST)).message)

    async def test_query_pickle_round_trips(self):
        self.peer.on(
            types.PYTHON_TEST_REQUEST,
            lambda m: __import__("pickle").dumps({"echo": __import__("pickle").loads(m.message)}),
            types.PYTHON_TEST_RESPONSE,
        )
        self.assertEqual({"echo": [1, 2]}, await self.client.query_pickle([1, 2], types.PYTHON_TEST_REQUEST))

    async def test_every_failure_is_an_exception(self):
        with self.assertRaises(BackendError):
            await self.client.query(b"raise", types.PYTHON_TEST_REQUEST)
        self.peer.errors.clear()  # the fake would fail the test at stop() for that raise, as designed
        with self.assertRaises((OperationTimedOut, OperationFailed)):
            await self.client.query(b"", types.PYTHON_TEST_REQUEST, timeout=0.5)
        with self.assertRaises(OperationFailed):
            await self.client.query(b"nobody serves this", types.PYTHON_TEST_RESPONSE, timeout=5)
        self.assertEqual(b"STILL", (await self.client.query(b"still", types.PYTHON_TEST_REQUEST)).message)

    async def test_a_hundred_queries_at_once_each_get_their_own_reply(self):
        replies = await asyncio.gather(
            *(self.client.query(b"n%d" % index, types.PYTHON_TEST_REQUEST) for index in range(100))
        )
        self.assertEqual([b"N%d" % index for index in range(100)], [reply.message for reply in replies])

    async def test_cancelling_an_await_leaves_the_client_usable(self):
        task = asyncio.ensure_future(self.client.query(b"", types.PYTHON_TEST_REQUEST, timeout=5))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(b"AFTER", (await self.client.query(b"after", types.PYTHON_TEST_REQUEST)).message)

    async def test_send_message_returns_the_id_and_the_event_arrives(self):
        mxmsg_id = await self.client.send_message(b"event", type=types.PYTHON_TEST_REQUEST)
        (received,) = self.peer.wait_for(types.PYTHON_TEST_REQUEST, matching=lambda m: m.message == b"event")
        self.assertEqual(mxmsg_id, received.id)
        ids = await asyncio.gather(
            *(self.client.send_message(b"e%d" % index, type=types.PYTHON_TEST_REQUEST) for index in range(100))
        )
        self.assertEqual(100, len(set(ids)))
        self.peer.wait_for(types.PYTHON_TEST_REQUEST, count=100, matching=lambda m: m.message.startswith(b"e"))
        await self.client.send_message(
            b"everyone", type=types.PYTHON_TEST_REQUEST, multiplexer=AsyncClient.ALL, flush=True
        )
        self.peer.wait_for(types.PYTHON_TEST_REQUEST, matching=lambda m: m.message == b"everyone")

    def push(self, payload: bytes, to: int) -> None:
        """Send `payload` straight to a peer, as a backend pushing an event would."""
        self.push_in_order(to, payload)

    def push_in_order(self, to: int, *payloads: bytes) -> None:
        """Send `payloads` straight to a peer from one sender, through one
        multiplexer, so that they arrive in the order given."""
        with TestClient(self.cluster, peers.WEBSITE) as sender:
            for payload in payloads:
                sender.send(payload, types.PYTHON_TEST_RESPONSE, to=to)

    async def test_subscriptions_run_on_the_loop(self):
        seen: list[tuple[str, bytes]] = []
        loop = asyncio.get_running_loop()
        done = asyncio.Event()

        async def coroutine_handler(mxmsg):
            self.assertIs(loop, asyncio.get_running_loop())
            seen.append(("coroutine", mxmsg.message))
            if len(seen) == 3:
                done.set()

        def plain_handler(mxmsg):
            self.assertIs(loop, asyncio.get_running_loop())
            seen.append(("plain", mxmsg.message))
            if len(seen) == 3:
                done.set()

        unsubscribe = self.client.subscribe(types.PYTHON_TEST_RESPONSE, coroutine_handler)
        self.client.subscribe(None, plain_handler, matching=lambda m: m.message == b"only this")
        await loop.run_in_executor(None, self.push, b"only this", self.client.instance_id)
        await loop.run_in_executor(None, self.push, b"not that", self.client.instance_id)
        await asyncio.wait_for(done.wait(), 10)
        self.assertEqual(
            sorted([("coroutine", b"only this"), ("plain", b"only this"), ("coroutine", b"not that")]), sorted(seen)
        )
        unsubscribe()
        # A sentinel behind the message the ended subscription must miss:
        # once its handler ran, a task the other would have made has had its
        # first step, which comes first on the loop.
        ended = asyncio.Event()
        self.client.subscribe(
            types.PYTHON_TEST_RESPONSE, lambda mxmsg: ended.set(), matching=lambda m: m.message == b"end"
        )
        await loop.run_in_executor(None, self.push_in_order, self.client.instance_id, b"after", b"end")
        await asyncio.wait_for(ended.wait(), 10)
        self.assertNotIn(("coroutine", b"after"), seen)

    async def test_a_raising_handler_is_logged_and_the_others_still_run(self):
        """A plain handler that raises, and a coroutine handler that raises:
        each is logged for each message, a warning in the client's log that
        names the handler, what it raised and the message's type; neither
        silences the other handlers, and asyncio has nothing unretrieved to
        complain about. The warnings are counted as they are logged, which
        for the coroutine is once its task is over, and read back from the
        log the C++ side writes."""
        loop = asyncio.get_running_loop()
        complaints: list[dict] = []
        loop.set_exception_handler(lambda _loop, context: complaints.append(context))
        seen: list[bytes] = []
        done = asyncio.Event()
        warned = asyncio.Event()
        warnings: list[int] = []
        logging = aio.log

        def counted(level: int, verbosity: int, **fields: object) -> None:
            """The client's log call, counted: every warning here is a handler's, on the loop."""
            logging(level, verbosity, **fields)
            if level == WARNING:
                warnings.append(level)
                if len(warnings) == 4:
                    warned.set()

        def raising(mxmsg):
            raise ValueError("plain handler")

        async def raising_later(mxmsg):
            await asyncio.sleep(0)
            raise ValueError("coroutine handler")

        def keeps_going(mxmsg):
            seen.append(mxmsg.message)
            if len(seen) == 2:
                done.set()

        self.client.subscribe(types.PYTHON_TEST_RESPONSE, raising)
        self.client.subscribe(types.PYTHON_TEST_RESPONSE, raising_later)
        self.client.subscribe(types.PYTHON_TEST_RESPONSE, keeps_going)
        with tempfile.TemporaryDirectory(dir=os.environ.get("TEST_TMPDIR")) as directory:
            path = os.path.join(directory, "stderr")
            with stderr_to(path), mock.patch.object(aio, "log", counted):
                for payload in (b"one", b"two"):
                    await loop.run_in_executor(None, self.push, payload, self.client.instance_id)
                await asyncio.wait_for(done.wait(), 10)
                await asyncio.wait_for(warned.wait(), 10)
            with open(path, "rb") as written:
                log = written.read().decode(errors="replace")
        self.assertEqual([b"one", b"two"], seen, "the handler after the raising ones ran for both messages")
        on_the_type = "on a message of type %d" % types.PYTHON_TEST_RESPONSE
        for name, error in (("raising", "plain handler"), ("raising_later", "coroutine handler")):
            said = [line for line in log.splitlines() if "<locals>.%s raised" % name in line and error in line]
            self.assertEqual(2, len(said), "a warning for each message about %s:\n%s" % (name, log[-3000:]))
            for line in said:
                self.assertIn("[WARNING]", line)
                self.assertIn(on_the_type, line)
        import gc

        gc.collect()
        self.assertEqual([], complaints, "no 'Exception in callback', no unretrieved task exception")

    async def test_a_message_costs_a_look_at_its_type_s_subscriptions_only(self):
        """The io thread looks at the subscriptions of a message's type and
        of every type, and no others: a hundred subscriptions of another
        type cost a message nothing, where every subscription's type was
        compared with every message's. Counted: the other type compares
        itself only when asked. The handlers of a type's subscriptions and
        of every type's still run in the order the subscriptions were made."""

        class CountedType(int):
            """A message type that counts how often it is compared."""

            comparisons = 0

            def __eq__(self, other: object) -> bool:
                CountedType.comparisons += 1
                return int.__eq__(self, other)

            def __ne__(self, other: object) -> bool:
                CountedType.comparisons += 1
                return int.__ne__(self, other)

            __hash__ = int.__hash__

        other = CountedType(types.PYTHON_TEST_REQUEST)
        unsubscribes = [self.client.subscribe(other, lambda mxmsg: None) for _ in range(100)]
        order: list[str] = []
        done = asyncio.Event()

        def noted(name: str):
            """A handler that notes `name`, and the end after the last."""

            def handler(mxmsg) -> None:
                order.append(name)
                if name == "typed two":
                    done.set()

            return handler

        self.client.subscribe(None, noted("every one"))
        self.client.subscribe(types.PYTHON_TEST_RESPONSE, noted("typed one"))
        self.client.subscribe(None, noted("every two"))
        self.client.subscribe(types.PYTHON_TEST_RESPONSE, noted("typed two"))
        CountedType.comparisons = 0
        await asyncio.get_running_loop().run_in_executor(None, self.push, b"ordered", self.client.instance_id)
        await asyncio.wait_for(done.wait(), 10)
        self.assertEqual(["every one", "typed one", "every two", "typed two"], order)
        self.assertEqual(0, CountedType.comparisons, "the other type's subscriptions were looked at")
        for unsubscribe in unsubscribes:
            unsubscribe()

    async def test_ending_one_of_two_equal_subscriptions_leaves_the_other(self):
        """The same handler subscribed twice is two subscriptions: ending the
        first, twice even, leaves the second, which gets each message once,
        where removal by equality ended one of the two on each call.
        Ordered, not timed: both messages come from one sender, in order."""
        loop = asyncio.get_running_loop()
        heard: asyncio.Queue[bytes] = asyncio.Queue()

        def handler(mxmsg) -> None:
            heard.put_nowait(mxmsg.message)

        first = self.client.subscribe(types.PYTHON_TEST_RESPONSE, handler)
        self.client.subscribe(types.PYTHON_TEST_RESPONSE, handler)
        first()
        first()

        def push_both() -> None:
            """Both messages from one sender, so that they arrive in order."""
            with TestClient(self.cluster, peers.WEBSITE) as sender:
                sender.send(b"still", types.PYTHON_TEST_RESPONSE, to=self.client.instance_id)
                sender.send(b"marker", types.PYTHON_TEST_RESPONSE, to=self.client.instance_id)

        await loop.run_in_executor(None, push_both)
        self.assertEqual(
            [b"still", b"marker"], [await asyncio.wait_for(heard.get(), 30), await asyncio.wait_for(heard.get(), 30)]
        )

    async def test_an_unsubscribed_handler_misses_what_was_already_handed_over(self):
        """Once unsubscribe() has returned, on the loop, its handler is not
        called again, for a message the io thread had handed to the loop
        already either, where the handlers were fixed at the hand-over.
        Ordered, not timed: the loop, in the first message's delivery,
        waits until a predicate on the io thread has seen the second one,
        which by then took its subscriptions, and only then unsubscribes."""
        loop = asyncio.get_running_loop()
        second_taken = threading.Event()  # the io thread has fixed the second message's subscriptions
        seen: list[bytes] = []
        after: list[bytes] = []
        done = asyncio.Event()

        def watches(mxmsg) -> bool:
            if mxmsg.message == b"two":
                second_taken.set()
            return False

        def unsubscribes(mxmsg):
            after.append(mxmsg.message)
            if mxmsg.message == b"one":
                self.assertTrue(second_taken.wait(10), "the second message never reached the io thread")
                unsubscribe()
            else:
                done.set()

        unsubscribe = self.client.subscribe(types.PYTHON_TEST_RESPONSE, lambda mxmsg: seen.append(mxmsg.message))
        self.client.subscribe(types.PYTHON_TEST_RESPONSE, unsubscribes)
        self.client.subscribe(types.PYTHON_TEST_RESPONSE, lambda mxmsg: None, matching=watches)

        def push_both() -> None:
            """Both messages from one sender, so that they arrive in order."""
            with TestClient(self.cluster, peers.WEBSITE) as sender:
                sender.send(b"one", types.PYTHON_TEST_RESPONSE, to=self.client.instance_id)
                sender.send(b"two", types.PYTHON_TEST_RESPONSE, to=self.client.instance_id)

        await loop.run_in_executor(None, push_both)
        await asyncio.wait_for(done.wait(), 10)
        self.assertEqual([b"one", b"two"], after)
        self.assertEqual([b"one"], seen, "the handler ran after unsubscribe() had returned")

    async def test_a_raising_predicate_takes_nothing_and_the_others_still_get_it(self):
        """A `matching` that raises counts as no match: the other
        subscriptions and messages() still get the message, where the
        exception lost it for every one of them."""
        loop = asyncio.get_running_loop()
        seen: list[bytes] = []
        never: list[bytes] = []
        done = asyncio.Event()

        def raising(mxmsg):
            raise ValueError("predicate")

        def keeps_going(mxmsg):
            seen.append(mxmsg.message)
            done.set()

        self.client.subscribe(types.PYTHON_TEST_RESPONSE, lambda mxmsg: never.append(mxmsg.message), matching=raising)
        self.client.subscribe(types.PYTHON_TEST_RESPONSE, keeps_going)
        stream = self.client.messages()
        await loop.run_in_executor(None, self.push, b"one", self.client.instance_id)
        await asyncio.wait_for(done.wait(), 10)
        self.assertEqual([b"one"], seen)
        self.assertEqual(b"one", (await asyncio.wait_for(stream.__anext__(), 10)).message)
        self.assertEqual([], never)

    async def test_messages_iterates_and_a_full_queue_drops_the_oldest(self):
        """Six messages reach the loop before anybody reads, counted by a
        subscription of every type, whose handler runs in the delivery that
        then feeds the queue: the queue of three keeps the last three."""
        small = AsyncClient(self.cluster.endpoints, peers.PYTHON_TEST_CLIENT, queue_size=3)
        try:
            stream = small.messages()
            delivered: list[bytes] = []
            all_six = asyncio.Event()

            def count(mxmsg) -> None:
                delivered.append(mxmsg.message)
                if len(delivered) == 6:
                    all_six.set()

            small.subscribe(None, count)
            loop = asyncio.get_running_loop()
            for index in range(6):
                await loop.run_in_executor(None, self.push, b"%d" % index, small.instance_id)
            await asyncio.wait_for(all_six.wait(), 10)  # the sixth's delivery, the queue's feed included, is over
            kept = [await asyncio.wait_for(stream.__anext__(), 10) for _ in range(3)]
            self.assertEqual([b"3", b"4", b"5"], [m.message for m in kept])
        finally:
            await small.aclose()

    async def test_messages_ends_once_the_client_is_closed(self):
        """After close(), every reader of messages() gets what arrived
        before it and then ends, its `async for` over, where each waited for
        good; a stream asked for after close() ends at once. Two readers
        share the queue: the one that meets the end puts it back for the
        other."""
        loop = asyncio.get_running_loop()
        first, second = self.client.messages(), self.client.messages()
        delivered = asyncio.Event()
        self.client.subscribe(None, lambda mxmsg: delivered.set(), matching=lambda m: m.message == b"1")

        async def read(stream) -> list[bytes]:
            return [mxmsg.message async for mxmsg in stream]

        readers = [asyncio.ensure_future(read(stream)) for stream in (first, second)]
        await loop.run_in_executor(None, self.push_in_order, self.client.instance_id, b"0", b"1")
        await asyncio.wait_for(delivered.wait(), 10)
        await self.client.aclose()
        read_by = await asyncio.wait_for(asyncio.gather(*readers), 10)
        self.assertEqual([b"0", b"1"], sorted(read_by[0] + read_by[1]))
        self.assertEqual([], await asyncio.wait_for(read(self.client.messages()), 10))

    async def test_no_handler_is_called_once_close_was(self):
        """A message handed to the loop before close() was called, and not
        delivered yet, reaches no handler: the loop is held, on purpose,
        until a predicate on the io thread has seen three messages, and
        close() is called from the loop with their deliveries waiting.
        Those ran after close() had returned, on the closed client."""
        loop = asyncio.get_running_loop()
        seen_all = threading.Event()  # the io thread took the third; its hand-over follows before its end
        taken: list[bytes] = []
        handled: list[bytes] = []

        def takes(mxmsg) -> bool:
            taken.append(mxmsg.message)
            if len(taken) == 3:
                seen_all.set()
            return True

        self.client.subscribe(types.PYTHON_TEST_RESPONSE, lambda mxmsg: handled.append(mxmsg.message), takes)
        pushing = loop.run_in_executor(None, self.push_in_order, self.client.instance_id, b"0", b"1", b"2")
        self.assertTrue(seen_all.wait(10), "the three messages never reached the io thread")  # the loop held
        self.client.close()  # joins the io thread: the three deliveries wait on the loop
        await pushing
        ran = loop.create_future()
        loop.call_soon(ran.set_result, None)  # behind the deliveries queued before
        await ran
        self.assertEqual([], handled, "a handler ran after close()")

    async def test_close_cancels_the_handlers_still_running(self):
        """A coroutine handler still running once close() has returned is
        cancelled on the loop, where it ran on, on a closed client, and was
        destroyed pending if the loop ended first."""
        loop = asyncio.get_running_loop()
        started, ended = asyncio.Event(), asyncio.Event()
        outcome: list[str] = []

        async def never_ends(mxmsg) -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                outcome.append("cancelled")
                raise
            finally:
                ended.set()

        self.client.subscribe(types.PYTHON_TEST_RESPONSE, never_ends)
        await loop.run_in_executor(None, self.push, b"work", self.client.instance_id)
        await asyncio.wait_for(started.wait(), 10)
        self.client.close()
        await asyncio.wait_for(ended.wait(), 10)
        self.assertEqual(["cancelled"], outcome)

    async def test_aclose_lets_running_handlers_end_and_sends_what_they_send(self):
        """aclose() waits, within its timeout, for the coroutine handlers
        still running, and what they send then is written before the
        client closes: the handler sends once released, after aclose()
        began, and the backend gets it. It was closed under them, their
        sends failing with NotConnected."""
        loop = asyncio.get_running_loop()
        started, release = asyncio.Event(), asyncio.Event()
        outcome: list[str] = []

        async def sends_when_released(mxmsg) -> None:
            started.set()
            await release.wait()
            await self.client.send_message(b"from the handler", type=types.PYTHON_TEST_REQUEST)
            outcome.append("sent")

        self.client.subscribe(types.PYTHON_TEST_RESPONSE, sends_when_released)
        await loop.run_in_executor(None, self.push, b"work", self.client.instance_id)
        await asyncio.wait_for(started.wait(), 10)
        closing = asyncio.ensure_future(self.client.aclose(timeout=30))
        for _ in range(3):
            await asyncio.sleep(0)
        self.assertFalse(closing.done(), "aclose() did not wait for the handler")
        release.set()
        await asyncio.wait_for(closing, 60)
        self.assertEqual(["sent"], outcome)
        self.peer.wait_for(types.PYTHON_TEST_REQUEST, matching=lambda m: m.message == b"from the handler")

    async def test_aclose_cancels_a_handler_still_running_at_its_timeout(self):
        """A coroutine handler that does not end within aclose()'s timeout
        is cancelled; one that awaits aclose() itself goes on."""
        loop = asyncio.get_running_loop()
        started, closer_done = asyncio.Event(), asyncio.Event()
        outcome: list[str] = []

        async def never_ends(mxmsg) -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                outcome.append("cancelled")
                raise

        async def closes(mxmsg) -> None:
            await started.wait()
            await self.client.aclose(timeout=0.2)  # the other never ends: cancelled at the timeout
            outcome.append("the closer went on")
            closer_done.set()

        self.client.subscribe(types.PYTHON_TEST_RESPONSE, never_ends, matching=lambda m: m.message == b"stuck")
        self.client.subscribe(types.PYTHON_TEST_RESPONSE, closes, matching=lambda m: m.message == b"close")
        await loop.run_in_executor(None, self.push_in_order, self.client.instance_id, b"stuck", b"close")
        await asyncio.wait_for(closer_done.wait(), 10)
        self.assertEqual(["cancelled", "the closer went on"], outcome)

    async def test_a_queue_of_no_messages_is_refused(self):
        """queue_size is 1 at least, before anything connects: 0, which is
        no bound to asyncio.Queue, and less raise ValueError, from create()
        too, where messages() then held every message for good."""
        for size in (0, -1):
            with self.assertRaises(ValueError):
                AsyncClient(self.cluster.endpoints, peers.PYTHON_TEST_CLIENT, queue_size=size)
            with self.assertRaises(ValueError):
                await AsyncClient.create(self.cluster.endpoints, peers.PYTHON_TEST_CLIENT, queue_size=size)

    async def test_what_arrived_before_the_reply_is_handled_before_the_await_resumes(self):
        """A query awaited on the client's loop resumes after the handlers of
        everything that arrived before its reply: the backend sends the
        client an event and then the reply, through its one connection, so
        they arrive in that order, and the event's handler has run by the
        time the query returns."""
        backend = self.peer.backend
        assert backend is not None

        def event_then_reply(mxmsg) -> bytes:
            backend.conn.send_message(b"before the reply", type=types.PYTHON_TEST_RESPONSE, to=mxmsg.sender)
            return b"the reply"

        self.peer.on(types.PYTHON_TEST_REQUEST, event_then_reply, types.PYTHON_TEST_RESPONSE)
        handled: list[bytes] = []
        self.client.subscribe(types.PYTHON_TEST_RESPONSE, lambda mxmsg: handled.append(mxmsg.message))
        reply = await self.client.query(b"request", types.PYTHON_TEST_REQUEST)
        self.assertEqual(b"the reply", reply.message)
        self.assertEqual([b"before the reply"], handled)

    async def test_messages_is_the_inbox_with_no_type(self):
        """messages() takes no type: a typed stream threw away what another
        stream wanted. Two readers share the inbox: every message reaches
        one of them, none is thrown away."""
        with self.assertRaises(TypeError):
            self.client.messages(types.PYTHON_TEST_RESPONSE)  # pyright: ignore[reportCallIssue]
        first, second = self.client.messages(), self.client.messages()
        loop = asyncio.get_running_loop()
        for index in range(4):
            await loop.run_in_executor(None, self.push, b"%d" % index, self.client.instance_id)
        read = [await asyncio.wait_for(stream.__anext__(), 10) for stream in (first, second, first, second)]
        self.assertEqual([b"0", b"1", b"2", b"3"], sorted(m.message for m in read))

    async def test_a_multiplexer_restart_under_a_live_client(self):
        self.assertEqual(b"BEFORE", (await self.client.query(b"before", types.PYTHON_TEST_REQUEST)).message)
        self.cluster.mx[0].restart()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
        self.assertEqual(b"AFTER", (await self.client.query(b"after", types.PYTHON_TEST_REQUEST, timeout=15)).message)

    async def test_queries_and_sends_are_awaited_from_any_loop(self):
        """What asgiref's async_to_sync does away from the server's loop: a
        new loop per call, on another thread. Queries and sends work there;
        messages(), whose queue lives on the client's loop, does not."""
        client = self.client

        async def through_another_loop():
            reply = await client.query(b"elsewhere", types.PYTHON_TEST_REQUEST)
            await client.send_message(b"sent", type=types.PYTHON_TEST_REQUEST)
            return reply.message

        def elsewhere():
            return asyncio.run(through_another_loop())

        for _ in range(3):  # a new loop each time, as async_to_sync makes
            self.assertEqual(b"ELSEWHERE", await asyncio.get_running_loop().run_in_executor(None, elsewhere))
        with self.assertRaises(RuntimeError):
            await asyncio.get_running_loop().run_in_executor(None, lambda: asyncio.run(self._messages_elsewhere()))

    async def _messages_elsewhere(self):
        """messages() called from a loop that is not the client's."""
        self.client.messages()


class SendTest(unittest.IsolatedAsyncioTestCase):
    """Sends as on every client; a multiplexer per test, frozen when a test
    needs the client's connection full."""

    CHUNK = b"x" * (16 * 1024)

    async def asyncSetUp(self):
        self.cluster = Cluster(1, rules=RULES).__enter__()
        self.client = AsyncClient(self.cluster.endpoints, peers.PYTHON_TEST_CLIENT)

    async def asyncTearDown(self):
        await self.client.aclose(timeout=0)
        self.cluster.mx[0].resume()
        self.cluster.__exit__(None, None, None)

    async def test_a_send_returns_with_the_connection_full(self):
        """The multiplexer frozen: sends past what its connection takes
        return at once, the messages waiting for room in the client, where
        each awaited its write and the first that had no room waited out its
        timeout; once the multiplexer reads again, flush_all() sees every
        one written and none is dropped."""
        self.cluster.mx[0].pause()

        async def fill():
            for payload in past_the_queue(self.CHUNK):
                await self.client.send_message(payload, type=types.PYTHON_TEST_REQUEST, timeout=60)

        await asyncio.wait_for(fill(), 30)
        self.cluster.mx[0].resume()
        self.assertTrue(await self.client.flush_all(60))
        self.assertEqual(0, self.client.dropped)

    async def test_a_flushing_send_awaits_the_write(self):
        """The connection full: a flushing send that could not be written
        within its timeout raises OperationTimedOut, and flush_all() says
        False, for that send's message and those before it."""
        self.cluster.mx[0].pause()
        for payload in past_the_queue(self.CHUNK):
            await self.client.send_message(payload, type=types.PYTHON_TEST_REQUEST, timeout=60)
        with self.assertRaises(OperationTimedOut):
            await self.client.send_message(b"waits", type=types.PYTHON_TEST_REQUEST, flush=True, timeout=0.3)
        self.assertFalse(await self.client.flush_all(0.3))

    async def test_a_callback_hears_how_the_message_ended_on_the_loop(self):
        """A send with a callback: 1 once written, called once, on the
        client's loop; 0 for one given up on, the multiplexer frozen with
        the connection full."""
        loop_thread = threading.get_ident()
        heard: list[tuple[int, int]] = []

        def callback(written: int) -> None:
            heard.append((written, threading.get_ident()))

        await self.client.send_message(b"written", type=types.PYTHON_TEST_REQUEST, callback=callback)
        self.assertTrue(await self.client.flush_all(10))
        self.assertEqual([(1, loop_thread)], heard)
        self.cluster.mx[0].pause()
        for payload in past_the_queue(self.CHUNK):
            await self.client.send_message(payload, type=types.PYTHON_TEST_REQUEST, timeout=60)
        await self.client.send_message(b"dropped", type=types.PYTHON_TEST_REQUEST, timeout=0.3, callback=callback)
        await self.client.flush_all(0.6)  # past the message's timeout, whose callback the loop got first
        self.assertEqual([(1, loop_thread), (0, loop_thread)], heard)
        self.assertEqual(1, self.client.dropped)


class NoConnectionTest(unittest.IsolatedAsyncioTestCase):
    """A client connected nowhere."""

    async def test_out_of_time_is_told_at_the_deadline(self):
        """A flushing send that runs out of time with no connection live
        raises NotConnected, as the io thread saw at the deadline: the loop
        asks the io thread nothing afterwards, where it asked for the
        connections, waiting for the io thread, and one up by then made it
        a timeout."""
        client = AsyncClient([], peers.PYTHON_TEST_CLIENT)
        try:
            with mock.patch.object(client._threaded, "connections_count", return_value=1) as asked:
                with self.assertRaises(NotConnected):
                    await client.send_message(b"nowhere", type=types.PYTHON_TEST_REQUEST, flush=True, timeout=0.2)
            asked.assert_not_called()
        finally:
            await client.aclose(timeout=0)


class CreateTest(unittest.TestCase):
    """AsyncClient.create() given up on, by a timeout around the await."""

    def test_a_cancelled_create_closes_the_client_it_made(self):
        """The caller gives up while the constructor connects: the client
        made anyway is closed once it is there, where it stayed connected
        and registered, with nobody to close it."""
        made: list[AsyncClient] = []
        closed = threading.Event()

        class Slow(AsyncClient):
            def __init__(self, *args, **kwargs):
                time.sleep(0.2)  # a slow handshake: time for the caller to give up
                super().__init__(*args, **kwargs)
                made.append(self)

            def close(self, *args, **kwargs):
                super().close(*args, **kwargs)
                closed.set()

        with Cluster(1, rules=RULES) as cluster:

            async def give_up() -> None:
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(Slow.create(cluster.endpoints, peers.PYTHON_TEST_CLIENT), 0.05)

            asyncio.run(give_up())
            self.assertTrue(closed.wait(10), "the client a cancelled create() made was not closed")
            self.assertEqual(1, len(made))
            cluster.wait_for_peer_gone(peers.PYTHON_TEST_CLIENT, 5)


class HolderTest(unittest.TestCase):
    """One client per process, on the loop that first asks; a forked child
    gets its own, the way an ASGI server's workers do after forking."""

    def test_aget_makes_one_client_off_the_loop(self):
        """Three concurrent first uses await one creation, made on a thread
        that is not the loop's while the loop keeps turning: the constructor
        waits for the loop to run a coroutine, which on the loop's own
        thread it would wait for in vain. get() afterwards hands out the
        same client."""
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:
            peer.reply_with(types.PYTHON_TEST_REQUEST, b"pong", types.PYTHON_TEST_RESPONSE)
            made_on: list[int] = []

            class Watched(AsyncClient):
                def __init__(self, *args, **kwargs):
                    made_on.append(threading.get_ident())
                    # The loop runs a coroutine while the client is made.
                    asyncio.run_coroutine_threadsafe(asyncio.sleep(0), kwargs["loop"]).result(10)
                    super().__init__(*args, **kwargs)

            holder = Watched.holder(peers.PYTHON_TEST_CLIENT, lambda: cluster.endpoints)

            async def scenario() -> None:
                clients = await asyncio.gather(holder.aget(), holder.aget(), holder.aget())
                self.assertEqual(1, len({id(client) for client in clients}), "one client, whoever asked first")
                self.assertEqual(1, len(made_on), "made once")
                self.assertNotEqual(threading.get_ident(), made_on[0], "made on the loop's thread")
                self.assertIs(clients[0], holder.get())
                self.assertIs(clients[0], await holder.aget())
                self.assertEqual(b"pong", (await clients[0].query(b"ping", types.PYTHON_TEST_REQUEST)).message)

            try:
                asyncio.run(scenario())
            finally:
                holder.close()

    def test_aget_makes_the_client_on_the_loop_the_holder_was_given(self):
        """holder(..., loop=L) makes the client on L with aget(), as with
        get(), whichever loop awaits: aget() passed the loop twice and every
        call raised TypeError."""
        other = asyncio.new_event_loop()
        turning = threading.Thread(target=other.run_forever, name="other-loop", daemon=True)
        turning.start()
        try:
            with Cluster(1, rules=RULES) as cluster:
                holder = AsyncClient.holder(peers.PYTHON_TEST_CLIENT, lambda: cluster.endpoints, loop=other)

                async def scenario() -> None:
                    try:
                        client = await holder.aget()
                        self.assertIs(other, client.loop)
                        self.assertIs(client, holder.get())
                    finally:
                        await holder.aclose()

                asyncio.run(scenario())
        finally:
            other.call_soon_threadsafe(other.stop)
            turning.join(10)
            other.close()

    def test_a_cancelled_first_aget_costs_nobody_else(self):
        """The first caller gives up while the client is being made: the
        caller behind it still gets it; a caller alone giving up leaves it
        made and kept for the next one; a close meanwhile closes it."""
        made: list[AsyncClient] = []

        class Slow(AsyncClient):
            def __init__(self, *args, **kwargs):
                made.append(self)
                time.sleep(0.2)  # a slow handshake: time for the first caller to give up
                super().__init__(*args, **kwargs)

        with Cluster(1, rules=RULES) as cluster:
            holder = Slow.holder(peers.PYTHON_TEST_CLIENT, lambda: cluster.endpoints)

            async def give_up_first() -> AsyncClient:
                first, second = asyncio.ensure_future(holder.aget()), asyncio.ensure_future(holder.aget())
                await asyncio.sleep(0.05)
                first.cancel()
                client = await second
                self.assertTrue(first.cancelled(), "the first caller's own wait ended")
                return client

            async def give_up_alone() -> None:
                alone = asyncio.ensure_future(holder.aget())
                await asyncio.sleep(0.05)
                alone.cancel()
                await asyncio.sleep(0.3)  # the making goes on without anybody waiting
                self.assertIs(made[-1], await holder.aget(), "kept, not made again")

            async def close_meanwhile() -> None:
                waiting = asyncio.ensure_future(holder.aget())
                await asyncio.sleep(0.05)
                holder.close()
                with self.assertRaises(RuntimeError):
                    await waiting

            try:
                client = asyncio.run(give_up_first())
                self.assertEqual([client], made, "one client, made once")
                holder.close()
                asyncio.run(give_up_alone())
                self.assertEqual(2, len(made))
                holder.close()
                asyncio.run(close_meanwhile())
                self.assertEqual(3, len(made))
                cluster.wait_for_peer_gone(peers.PYTHON_TEST_CLIENT, 5)  # the disowned one closed itself
            finally:
                holder.close()

    def test_first_uses_on_several_threads_at_once_make_one_client(self):
        """Threads that use the holder for the first time at once, each on a
        loop of its own, as async_to_sync calls from a threaded server's
        requests do: one client for all, where each get() made its own and
        all but one stayed registered, closed by nobody."""
        made: list[AsyncClient] = []

        class Slow(AsyncClient):
            def __init__(self, *args, **kwargs):
                made.append(self)
                time.sleep(0.2)  # a slow handshake: time for every thread to ask meanwhile
                super().__init__(*args, **kwargs)

        with Cluster(1, rules=RULES) as cluster:
            holder = Slow.holder(peers.PYTHON_TEST_CLIENT, lambda: cluster.endpoints)
            together = threading.Barrier(4, timeout=10)
            got: list[AsyncClient] = []

            def first_use(asynchronous: bool) -> None:
                async def use() -> None:
                    together.wait()
                    got.append(await holder.aget() if asynchronous else holder.get())

                asyncio.run(use())

            threads = [threading.Thread(target=first_use, args=(index == 3,)) for index in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(30)
            try:
                self.assertEqual(1, len(made), "clients made")
                self.assertEqual(4, len(got))
                self.assertEqual(1, len({id(client) for client in got}), "one client, whoever asked first")
            finally:
                holder.close()

    def test_a_first_use_that_failed_is_tried_again(self):
        """A client that could not be made, its constructor raising at once
        as with no descriptor left: that call raises, and the next one makes
        it, where the failure stayed and every later call raised it again
        until close()."""
        attempts: list[int] = []

        class FailsOnce(AsyncClient):
            def __init__(self, *args, **kwargs):
                attempts.append(len(attempts))
                if len(attempts) == 1:
                    raise OSError("too many open files")
                super().__init__(*args, **kwargs)

        with Cluster(1, rules=RULES) as cluster:
            holder = FailsOnce.holder(peers.PYTHON_TEST_CLIENT, lambda: cluster.endpoints)

            async def scenario() -> None:
                with self.assertRaises(OSError):
                    await holder.aget()
                client = await holder.aget()
                self.assertIs(client, holder.get())

            try:
                asyncio.run(scenario())
            finally:
                holder.close()
            self.assertEqual(2, len(attempts))

    def test_a_client_whose_loop_has_closed_says_so(self):
        """The holder's client is the first caller's loop's, as in a suite
        whose every test runs on a loop of its own. Once the first test's
        loop has closed, what arrives for its subscription is dropped with a
        warning, once, and the next test's subscribe() and messages() raise,
        where the subscription took the handler and nothing ever came;
        closing the holder makes the next use start a client on that test's
        loop, whose subscription delivers. Counted, not timed: the first
        subscription's predicate runs on the io thread for every message,
        before the hand-over that fails, and three messages through means
        the first two handed over."""
        passes: list[bytes] = []
        third = threading.Event()

        def counted(mxmsg) -> bool:
            passes.append(mxmsg.message)
            if len(passes) == 3:
                third.set()
            return True

        def push(cluster: Cluster, to: int, *payloads: bytes) -> None:
            """`payloads` straight to the peer `to`, in this order, as a backend pushing events."""
            with TestClient(cluster, peers.WEBSITE) as sender:
                for payload in payloads:
                    sender.send(payload, types.PYTHON_TEST_RESPONSE, to=to)

        with Cluster(1, rules=RULES) as cluster:
            holder = AsyncClient.holder(peers.PYTHON_TEST_CLIENT, lambda: cluster.endpoints)

            async def first_test() -> int:
                client = await holder.aget()
                client.subscribe(types.PYTHON_TEST_RESPONSE, lambda mxmsg: None, matching=counted)
                return client.instance_id

            async def second_test() -> bytes:
                client = await holder.aget()
                with self.assertRaisesRegex(RuntimeError, "has closed"):
                    client.subscribe(types.PYTHON_TEST_RESPONSE, lambda mxmsg: None)
                with self.assertRaisesRegex(RuntimeError, "has closed"):
                    client.messages()
                await holder.aclose()
                client = await holder.aget()
                loop = asyncio.get_running_loop()
                arrived: asyncio.Future[bytes] = loop.create_future()

                def take(mxmsg) -> None:
                    if not arrived.done():
                        arrived.set_result(mxmsg.message)

                client.subscribe(types.PYTHON_TEST_RESPONSE, take)
                await loop.run_in_executor(None, push, cluster, client.instance_id, b"here")
                return await asyncio.wait_for(arrived, 10)

            try:
                instance_id = asyncio.run(first_test())
                with tempfile.TemporaryDirectory(dir=os.environ.get("TEST_TMPDIR")) as directory:
                    path = os.path.join(directory, "stderr")
                    with stderr_to(path):
                        push(cluster, instance_id, b"one", b"two", b"three")
                        self.assertTrue(third.wait(10), "the io thread saw the three messages")
                    with open(path, "rb") as written:
                        log = written.read().decode(errors="replace")
                said = [line for line in log.splitlines() if "has closed" in line]
                self.assertEqual(1, len(said), "one warning for the messages dropped:\n%s" % log[-3000:])
                self.assertIn("[WARNING]", said[0])
                self.assertEqual(b"here", asyncio.run(second_test()))
            finally:
                holder.close()

    def test_the_holder_makes_one_client_per_process(self):
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:
            peer.on(types.PYTHON_TEST_REQUEST, lambda m: m.message.upper(), types.PYTHON_TEST_RESPONSE)
            holder = AsyncClient.holder(peers.PYTHON_TEST_CLIENT, lambda: cluster.endpoints)

            async def use(payload: bytes) -> tuple[AsyncClient, bytes]:
                client = holder.get()
                assert client is holder.get()
                return client, (await client.query(payload, types.PYTHON_TEST_REQUEST)).message

            parent_client, reply = asyncio.run(use(b"parent"))
            self.assertEqual(b"PARENT", reply)
            pid = os.fork()
            if pid == 0:  # the child: its own loop, its own client from the same holder
                try:
                    child_client, child_reply = asyncio.run(use(b"child"))
                    assert child_client is not parent_client, "the parent's client leaked into the child"
                    assert child_reply == b"CHILD", child_reply
                    child_client.close()
                    os._exit(0)
                except BaseException as error:  # reported through the exit code
                    print("child failed:", repr(error), file=sys.stderr)
                    os._exit(1)
            _, status = os.waitpid(pid, 0)
            self.assertEqual(0, os.waitstatus_to_exitcode(status))
            holder.close()


EXIT_PROBE = """
import asyncio, sys
from multiplexer.aio import AsyncClient
host, port = sys.argv[1].rsplit(":", 1)

async def main():
    client = AsyncClient([(host, int(port))], type=%(peer)d)
    try:
        await client.query(b"nobody serves this", type=%(request)d, timeout=0.05)
    except Exception:
        pass
    # leave without closing: the io thread is still alive when the interpreter exits

asyncio.run(main())
print("leaving with the io thread alive")
"""


class InterpreterExitTest(unittest.TestCase):
    """A program that exits with an AsyncClient alive exits cleanly."""

    def test_exit_with_a_live_client(self):
        with Cluster(1, rules=RULES) as cluster:
            env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
            program = EXIT_PROBE % {"peer": peers.PYTHON_TEST_CLIENT, "request": types.PYTHON_TEST_RESPONSE}
            for _ in range(5):
                result = subprocess.run(
                    [sys.executable, "-c", program, cluster.addresses[0]],
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=30,
                )
                self.assertEqual(0, result.returncode, result.stderr.decode(errors="replace")[-2000:])


if __name__ == "__main__":
    unittest.main()
