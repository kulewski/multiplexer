"""The example's test on the library's harness (docs/api_python.md,
"Testing"): real multiplexers from `Cluster`, generators on
`BackendThread`s and as processes, an answer in order and complete, two
at once told apart, a gap filled by asking for what is missing, a
multiplexer killed under an answer with two generators behind it and
with the answer pinned, a consumer that stops early, an answer queued
behind a busy generator and a gap filled while it is busy, the web side
driven through its ASGI app, a generator killed under an answer, one
leaving, and a prompt with no generator failing at once; and the
committed constants checked against the rules file. The multiplexers are
the mxcontrol the package installed unless MXCONTROL names another;
`python -m unittest -v test`."""

import asyncio
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "web"))

from multiplexer.aio import AsyncClient  # noqa: E402
from multiplexer.mxclient import NotConnected, OperationFailed  # noqa: E402
from multiplexer.testing import BackendThread, Cluster, mxcontrol_path, wait_until  # noqa: E402

from generator import Generator, answer_words  # noqa: E402
from multiplexer_constants import peers  # noqa: E402
from mxstream import Stream, Streams  # noqa: E402

RULES = os.path.join(HERE, "stream.rules")  # what the constants were generated from; Cluster's multiplexers read it
RATE = 200.0  # tokens per second in the tests: a 400-token answer takes two seconds
# A generator process dies with the test process however that ends, where util-linux's setpriv is there.
WITH_THE_TEST = ["setpriv", "--pdeathsig", "KILL", "--"] if shutil.which("setpriv") else []


async def collect(stream: Stream) -> list[str]:
    """The whole answer, as a list."""
    return [text async for text in stream]


class StreamTest(unittest.IsolatedAsyncioTestCase):
    """Two multiplexers and two generators for the class; a client per
    test, since the case makes a loop per test and what arrives is
    delivered on the loop the client was made on."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(2, rules=RULES).__enter__()
        cls.addClassCleanup(cls.cluster.__exit__, None, None, None)
        cls.generators = []
        for name in ("g1", "g2"):
            thread = BackendThread(lambda name=name: Generator(cls.cluster.endpoints, name, tokens_per_second=RATE))
            cls.generators.append(thread.start())
            cls.addClassCleanup(thread.stop)
        cls.cluster.wait_for_peer(peers.GENERATOR, 2)

    async def asyncSetUp(self):
        self.client = AsyncClient(self.cluster.endpoints, type=peers.STREAM_CLIENT)
        self.streams = Streams(self.client)

    async def asyncTearDown(self):
        self.streams.close()
        await self.client.aclose()

    def started(self, stream: Stream) -> int:
        """How many generators started this answer."""
        return sum(stream.stream_id in thread.backend.answers for thread in self.generators)

    async def test_the_answer_arrives_in_order_and_ends_with_the_reply(self):
        stream = self.streams.open("what is a multiplexer", 30)
        tokens = await collect(stream)
        self.assertEqual(answer_words("what is a multiplexer", 30), tokens)
        assert stream.answer is not None
        self.assertEqual(30, stream.answer.tokens)
        self.assertIn(stream.answer.worker, ("g1", "g2"))
        self.assertEqual((0, 0, 0), (stream.gaps, stream.resent, stream.reattached))
        self.assertGreater(stream.tokens_per_second, RATE / 2)

    async def test_two_answers_at_once_are_told_apart(self):
        first, second = self.streams.open("one", 40), self.streams.open("two", 40)
        tokens = await asyncio.gather(collect(first), collect(second))
        self.assertEqual([answer_words("one", 40), answer_words("two", 40)], tokens)

    async def test_a_gap_is_filled_by_asking_for_what_is_missing(self):
        stream = self.streams.open("gap", 400)
        tokens = [await stream.__anext__()]  # the generator is known from the first token
        stream.drop = 5  # the next five to arrive never do, as with a dead connection
        asked = time.monotonic()
        tokens.append(await stream.__anext__())
        self.assertLess(time.monotonic() - asked, 0.5, "the gap filled at once, not at the end of the answer")
        tokens += await collect(stream)
        self.assertEqual(answer_words("gap", 400), tokens, "complete and in order")
        self.assertEqual((1, 5), (stream.gaps, stream.resent), "the five, and only them")

    async def test_a_multiplexer_dies_under_an_answer(self):
        started = sum(thread.backend.started for thread in self.generators)
        stream = self.streams.open("killed", 400)
        tokens = [await stream.__anext__()]
        victim = self.cluster.multiplexer_at(stream.lane.connection.endpoint)  # the lane's, once the request went
        victim.kill()
        tokens += await collect(stream)
        self.assertEqual(answer_words("killed", 400), tokens, "complete and in order")
        self.assertEqual(1, stream.reattached, "the request again, through the other multiplexer")
        self.assertEqual(1, self.started(stream), "joined by the generator of the answer, not started by another")
        self.assertEqual(
            started + 1, sum(thread.backend.started for thread in self.generators), "joined, not started over"
        )
        assert stream.answer is not None
        self.assertLess(stream.answer.seconds, 3, "the answer went on, not over again")
        victim.start()
        self.cluster.wait_for_peer(peers.GENERATOR, 2)  # the generators reconnected on their own, within 3 s
        self.cluster.wait_for_peer(peers.STREAM_CLIENT)  # and so did this client

    async def test_a_pinned_answer_ends_instead(self):
        stream = self.streams.open("pinned", 400, pinned=True)
        await stream.__anext__()
        victim = self.cluster.multiplexer_at(stream.lane.connection.endpoint)
        victim.kill()
        with self.assertRaises(NotConnected):
            await collect(stream)
        victim.start()
        self.cluster.wait_for_peer(peers.GENERATOR, 2)
        self.cluster.wait_for_peer(peers.STREAM_CLIENT)

    async def test_a_consumer_that_stops_early_stops_the_generator(self):
        async with self.streams.open("early", 400) as stream:
            async for _ in stream:
                if stream.received == 3:
                    break
        self.assertNotIn(stream.stream_id, self.streams._open, "forgotten")
        answer = next(
            t.backend.answers[stream.stream_id] for t in self.generators if stream.stream_id in t.backend.answers
        )
        await asyncio.to_thread(wait_until, lambda: answer.done, 2, "the generator stopped")
        self.assertTrue(answer.cancelled)
        self.assertLess(len(answer.tokens), 100, "stopped within a few tokens, not at the end")

    async def test_the_web_side_streams_events(self):
        import httpx

        os.environ["MX_ADDRESSES"] = ",".join(f"{host}:{port}" for host, port in self.cluster.endpoints)
        import app

        transport = httpx.ASGITransport(app=app.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as web:
            async with web.stream("GET", "/generate", params={"prompt": "web", "tokens": 5}) as response:
                self.assertEqual("text/event-stream", response.headers["content-type"].split(";")[0])
                lines = [line async for line in response.aiter_lines() if line]
            page = await web.get("/")
            refused = await web.get("/generate", params={"prompt": "web", "tokens": -1})
        await app.MX.aclose()
        self.assertEqual([f"data: {json.dumps(word)}" for word in answer_words("web", 5)], lines[:5])
        self.assertEqual("event: done", lines[5])
        self.assertIn('"tokens": 5', lines[6])
        self.assertEqual(200, page.status_code)
        self.assertEqual(422, refused.status_code, "a count of tokens out of range, refused before anything streams")


class BusyGeneratorTest(unittest.IsolatedAsyncioTestCase):
    """One generator that takes one answer at a time: a second answer waits
    in its queue, and a gap in the one under way is filled regardless."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(1, rules=RULES).__enter__()
        cls.addClassCleanup(cls.cluster.__exit__, None, None, None)
        thread = BackendThread(lambda: Generator(cls.cluster.endpoints, "busy", tokens_per_second=RATE, answers=1))
        cls.generator = thread.start()
        cls.addClassCleanup(thread.stop)
        cls.cluster.wait_for_peer(peers.GENERATOR)

    async def asyncSetUp(self):
        self.client = AsyncClient(self.cluster.endpoints, type=peers.STREAM_CLIENT)
        self.streams = Streams(self.client, stall=0.5)  # a short stall, which a queued answer must not trip

    async def asyncTearDown(self):
        self.streams.close()
        await self.client.aclose()

    async def test_an_answer_queued_behind_another_waits_its_turn(self):
        first, second = self.streams.open("first", 200), self.streams.open("second", 20)  # a second, then its turn
        tokens = await asyncio.gather(collect(first), collect(second))
        self.assertEqual([answer_words("first", 200), answer_words("second", 20)], tokens)

    async def test_a_gap_is_filled_while_the_only_answer_thread_is_busy(self):
        stream = self.streams.open("busy gap", 400)
        tokens = [await stream.__anext__()]
        stream.drop = 5
        tokens += await collect(stream)
        self.assertEqual(answer_words("busy gap", 400), tokens)
        self.assertEqual((1, 5), (stream.gaps, stream.resent))


class QueuedAnswerTest(unittest.IsolatedAsyncioTestCase):
    """Two multiplexers and a generator that takes one answer at a time:
    the multiplexer of an answer waiting in its pool dies before any token
    has named the generator."""

    async def test_a_queued_answer_survives_its_multiplexer_dying(self):
        with Cluster(2, rules=RULES) as cluster:
            generator = BackendThread(lambda: Generator(cluster.endpoints, "busy", tokens_per_second=RATE, answers=1))
            generator.start()
            self.addCleanup(generator.stop)
            cluster.wait_for_peer(peers.GENERATOR)
            client = AsyncClient(cluster.endpoints, type=peers.STREAM_CLIENT)
            streams = Streams(client)
            try:
                first = streams.open("first", 200)  # a second of the pool's one thread
                tokens = [await first.__anext__()]
                queued = streams.open("queued", 20)
                await asyncio.sleep(0.1)  # the request went, and waits in the pool
                self.assertEqual(0, queued.received)
                cluster.multiplexer_at(queued.lane.connection.endpoint).kill()
                tokens += await collect(first)
                self.assertEqual(answer_words("first", 200), tokens)
                self.assertEqual(answer_words("queued", 20), await collect(queued), "whole, once its turn came")
                self.assertEqual(1, queued.reattached)
                assert generator.backend is not None
                self.assertEqual(2, generator.backend.started, "the request sent again joined the answer waiting")
            finally:
                streams.close()
                await client.aclose()


class GeneratorProcess:
    """generator.py as a process of its own, so that a test can kill it the
    way a machine would, or ask it to leave the way a deployment does."""

    def __init__(self, cluster: Cluster, name: str, tokens_per_second: float):
        addresses = ",".join(f"{host}:{port}" for host, port in cluster.endpoints)
        command = WITH_THE_TEST + [sys.executable, os.path.join(HERE, "generator.py"), addresses, "--name", name]
        command += ["--tokens-per-second", str(tokens_per_second)]
        self.process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        assert self.process.stdout is not None
        ready = self.process.stdout.readline()
        if not ready.startswith("ready: generator"):
            self.kill()
            raise AssertionError(f"the generator did not start: {ready!r}")
        try:
            cluster.wait_for_peer(peers.GENERATOR)
        except BaseException:
            self.kill()
            raise

    def kill(self) -> None:
        """As a machine dies; nothing if it is gone already."""
        if self.process.poll() is None:
            self.process.kill()
        self.reap()

    def reap(self) -> str:
        """Wait for the exit; what it printed after the ready line."""
        self.process.wait(10)
        assert self.process.stdout is not None
        if self.process.stdout.closed:
            return ""
        rest = self.process.stdout.read()
        self.process.stdout.close()
        return rest


class GeneratorProcessTest(unittest.IsolatedAsyncioTestCase):
    """A generator as a process: killed under an answer, nothing tells the
    requester, the answer stalls, and the stalled answer's question to its
    generator fails at once for a peer nobody has; asked to leave, it
    finishes the answer it has and takes no new one."""

    async def asyncSetUp(self):
        self.cluster = Cluster(2, rules=RULES).__enter__()
        self.addCleanup(self.cluster.__exit__, None, None, None)
        self.client = AsyncClient(self.cluster.endpoints, type=peers.STREAM_CLIENT)
        self.streams = Streams(self.client, stall=1.0)

    async def asyncTearDown(self):
        self.streams.close()
        await self.client.aclose()

    async def test_a_dead_generator_fails_the_answer_after_the_stall(self):
        generator = GeneratorProcess(self.cluster, "g-killed", tokens_per_second=RATE)
        self.addCleanup(generator.kill)
        stream = self.streams.open("doomed", 1000)
        await stream.__anext__()
        generator.kill()
        started = time.monotonic()
        with self.assertRaises(OperationFailed):
            await collect(stream)
        self.assertLess(time.monotonic() - started, self.streams.stall + 3, "the stall, then the question failing")
        self.assertNotIn(stream.stream_id, self.streams._open)

    async def test_a_leaving_generator_finishes_its_answer_and_takes_no_new_one(self):
        generator = GeneratorProcess(self.cluster, "g-leaving", tokens_per_second=RATE)
        self.addCleanup(generator.kill)
        stream = self.streams.open("leaving", 400)
        tokens = [await stream.__anext__()]
        generator.process.send_signal(signal.SIGTERM)
        await asyncio.sleep(0.5)  # the leave under way: the multiplexers route the generator nothing new
        with self.assertRaises(OperationFailed):
            await collect(self.streams.open("too late", 5))
        tokens += await collect(stream)
        self.assertEqual(answer_words("leaving", 400), tokens, "the answer under way, whole")
        output = await asyncio.to_thread(generator.reap)
        self.assertEqual(0, generator.process.returncode)
        self.assertEqual("leaving, answers under way: 1\nleft, answers finished: 1\n", output)


class NoGeneratorTest(unittest.IsolatedAsyncioTestCase):
    """A multiplexer with nobody behind it."""

    async def test_a_prompt_with_no_generator_fails_at_once(self):
        with Cluster(1, rules=RULES) as cluster:
            client = AsyncClient(cluster.endpoints, type=peers.STREAM_CLIENT)
            streams = Streams(client)
            try:
                started = time.monotonic()
                with self.assertRaises(OperationFailed):
                    await collect(streams.open("nobody", 5))
                self.assertLess(time.monotonic() - started, 3, "OperationFailed, not a timeout")
            finally:
                streams.close()
                await client.aclose()


class ConstantsTest(unittest.TestCase):
    """The committed constants are what mxcontrol writes from the rules file
    as it is now: a change to the file that left them behind fails here."""

    def test_the_constants_are_generated_from_the_rules_file(self):
        out = tempfile.mkdtemp()
        written = [os.path.join(out, name) for name in ("multiplexer_constants.py", "multiplexer_constants.pyi")]
        command = [mxcontrol_path(), "generate_constants", "stream.rules", "--python", written[0], "--pyi", written[1]]
        subprocess.run(command, cwd=HERE, check=True, capture_output=True)  # the file's name as the header records it
        for path in written:
            with open(path) as generated, open(os.path.join(HERE, os.path.basename(path))) as committed:
                self.assertEqual(committed.read(), generated.read(), os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
