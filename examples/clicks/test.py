"""The example's test on the library's harness (docs/api_python.md,
"Testing"): real multiplexers from `Cluster`, collectors served on
threads by `BackendThread`, sources on threads of their own. Every click
is stored once, in the file of its shard, and the counts add up; through
one multiplexer the clicks of a source keep their order within a shard,
and a raw peer that takes one shard sees them come in batches, each one
message of the shard's key, holding the shard's clicks and no other.
Then the programs as the steps run them, a collector leaving on SIGTERM
with its files written, and the committed constants checked against the
rules file. The multiplexers are the mxcontrol the package installed
unless MXCONTROL names another; `python -m unittest -v test`.

The walkthrough's "Testing it with the harness" walks through this file."""

import collections
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
# The example's own modules import by name, as they do when its scripts run
# from its directory; this test may run from anywhere, so the example's
# directory goes first on the path.
sys.path.insert(0, HERE)

from multiplexer.Multiplexer_pb2 import MessageBatch, MultiplexerMessage
from multiplexer.mxclient import DEFAULT_BATCH_MAX_RECORDS
from multiplexer.testing import BackendThread, Cluster, RawPeer, mxcontrol_path, wait_until
from multiplexer.threaded_client import ThreadedClient

from clicks_pb2 import Click, Counts
from collector import Collector
from multiplexer_constants import peers, types
from source import SHARDS, ClickSource, shard_key

RULES = os.path.join(HERE, "clicks.rules")  # what the constants were generated from; Cluster's multiplexers read it
SOURCES = 4
# Each source's clicks. Ten thousand in all are far fewer than the
# multiplexers' queues and the sockets on the way hold, so that the
# collectors, on threads of this process and sharing its interpreter with
# the sources, may fall behind for a while and lose nothing.
CLICKS = 2500
PAGES = ["/", "/pricing", "/docs"]
PER_BATCH = DEFAULT_BATCH_MAX_RECORDS  # a thousand: a batch goes once it holds this many
HOUR = 3600.0  # a delay that never runs out here: the batches go full or at the flush
# A collector process dies with the test process however that ends, where util-linux's setpriv is there.
WITH_THE_TEST = ["setpriv", "--pdeathsig", "KILL", "--"] if shutil.which("setpriv") else []


def user_of(source: int, number: int) -> str:
    """Every click its own user, which says whose click it was and which:
    the lines of the files, and the batches, can then be checked click by
    click, and the users still spread over the shards as names do."""
    return f"source{source}.click{number}"


def lines_of(directory: str, shard: int) -> list[tuple[str, str]]:
    """The (user, page) of every line in a shard's file, in the file's order."""
    with open(os.path.join(directory, f"clicks-shard.{shard}.log")) as stored:
        return [(user, page) for user, page, _ in (line.split() for line in stored)]


def counts_of(client: ThreadedClient, shard: int) -> Counts:
    """What the collector of `shard` answers."""
    return Counts.FromString(client.query(b"", type=types.CLICK_COUNTS, key=f"shard.{shard}").message)


class ClicksTest(unittest.TestCase):
    """Two multiplexers, two collectors taking two shards each, and sources
    on threads, each its own ClickSource, as each web process has its own."""

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.directory, True)
        self.cluster = Cluster(2, rules=RULES).__enter__()
        self.addCleanup(self.cluster.__exit__, None, None, None)
        for shards in ([0, 1], [2, 3]):
            served = BackendThread(lambda shards=shards: Collector(self.cluster.endpoints, shards, self.directory))
            self.addCleanup(served.start().stop)
        self.client = ThreadedClient(self.cluster.endpoints, type=peers.CLICK_SOURCE)
        self.addCleanup(self.client.shutdown)

    def send(self) -> list[tuple[str, str]]:
        """SOURCES threads at once, each sending CLICKS clicks as fast as it
        goes, then flushing; the (user, page) of every click, in no order."""
        sent: list[tuple[str, str]] = []
        failed: list[BaseException] = []

        def one_source(index: int) -> None:
            """One source's clicks, kept once they are flushed."""
            try:
                clicks = [(user_of(index, number), PAGES[number % len(PAGES)]) for number in range(CLICKS)]
                with ClickSource(self.cluster.endpoints) as source:
                    for user, page in clicks:
                        source.click(user, page)
                    self.assertTrue(source.flush(), "every click written")
                    self.assertEqual(0, source.dropped)
                sent.extend(clicks)
            except BaseException as error:  # what the test fails on
                failed.append(error)

        threads = [threading.Thread(target=one_source, args=(index,)) for index in range(SOURCES)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)
        self.assertEqual([], failed)
        return sent

    def stored(self) -> int:
        """The clicks both collectors have stored, as their counts say."""
        return sum(counts_of(self.client, shard).clicks for shard in range(SHARDS))

    def test_every_click_is_stored_once_and_the_counts_add_up(self):
        sent = self.send()
        self.assertEqual(SOURCES * CLICKS, len(sent))
        # Written is not stored: the clicks are on their way through the
        # multiplexers until the counts say so. A bound that only detects a failure.
        wait_until(lambda: self.stored() >= len(sent), 60, "every click stored")
        self.assertEqual(len(sent), self.stored(), "and none twice")
        pages: collections.Counter[str] = collections.Counter()
        for shard in range(SHARDS):
            answer = counts_of(self.client, shard)  # the file is written out before the answer
            pages.update(answer.pages)
            stored = lines_of(self.directory, shard)
            expected = [click for click in sent if shard_key(click[0]) == f"shard.{shard}"]
            self.assertEqual(sorted(expected), sorted(stored), f"shard.{shard}: every click once, in its shard's file")
            self.assertEqual(len(stored), answer.clicks)
        self.assertEqual(collections.Counter(page for _, page in sent), pages, "the counts add up")


class BatchesTest(unittest.TestCase):
    """One multiplexer, so that what a source sends reaches each receiver in
    the order it was sent: one collector takes shards 0 to 2, and a raw
    peer of the collectors' type, the library's harness speaking the wire
    format by hand, takes shard 3. Its source batches with a delay that
    never runs out, so that the batches go full, at a thousand clicks, or
    at the flush, and the test can count them; with no delay, or a short
    one, a batch goes when the connection is free, or on time, which a
    test cannot count on."""

    def test_a_batch_holds_one_shards_clicks_and_goes_whole_to_its_collector(self):
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, True)
        cluster = Cluster(1, rules=RULES).__enter__()
        self.addCleanup(cluster.__exit__, None, None, None)
        served = BackendThread(lambda: Collector(cluster.endpoints, [0, 1, 2], directory)).start()
        self.addCleanup(served.stop)
        wire = RawPeer(cluster.endpoints[0], peers.CLICK_COLLECTOR)
        self.addCleanup(wire.close)
        wire.handshake({types.CLICK: [re.escape("shard.3")], types.CLICK_COUNTS: []})  # and no question

        clicks = [(user_of(0, number), PAGES[number % len(PAGES)]) for number in range(6000)]
        by_shard = {shard: [click for click in clicks if shard_key(click[0]) == f"shard.{shard}"] for shard in range(4)}
        self.assertNotEqual(1, len(by_shard[3]) % PER_BATCH, "the flush's batch holds more than one click")
        with ClickSource(cluster.endpoints, max_delay=HOUR) as source:
            for user, page in clicks:
                source.click(user, page)
            self.assertTrue(source.flush())

        # The wire: shard 3's clicks, in batches of a thousand and the
        # flush's, each one message keyed shard.3 from the source, its
        # clicks in the order they were sent.
        batches: list[MultiplexerMessage] = []
        received: list[tuple[str, str]] = []
        while len(received) < len(by_shard[3]):  # each message brings one click at least
            mxmsg = wire.receive_type(types.CLICK)
            batches.append(mxmsg)
            if mxmsg.HasField("batch"):
                inside = [MultiplexerMessage.FromString(body) for body in MessageBatch.FromString(mxmsg.batch).message]
            else:  # a click sent alone
                inside = [mxmsg]
            for message in inside:
                click = Click.FromString(message.message)
                received.append((click.user, click.page))
        self.assertEqual(math.ceil(len(by_shard[3]) / PER_BATCH), len(batches))
        self.assertEqual({(True, "shard.3")}, {(mxmsg.HasField("batch"), mxmsg.key) for mxmsg in batches})
        self.assertEqual(by_shard[3], received)

        # The collector: the other shards' clicks, one at a time, each file
        # in the order sent, which one multiplexer keeps.
        client = ThreadedClient(cluster.endpoints, type=peers.CLICK_SOURCE)
        self.addCleanup(client.shutdown)
        for shard in range(3):
            wait_until(lambda shard=shard: counts_of(client, shard).clicks == len(by_shard[shard]), 60, "stored")
            self.assertEqual(by_shard[shard], lines_of(directory, shard))


class ProgramTest(unittest.TestCase):
    """collector.py and clicks_cli.py as the steps run them: counts before any
    collector, two collectors, clicks sent and counted, a tap's view of them,
    and the collectors leaving on SIGTERM with every click written."""

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.directory, True)
        self.cluster = Cluster(2, rules=RULES, remote_recording=True).__enter__()  # taps allowed
        self.addCleanup(self.cluster.__exit__, None, None, None)
        self.addresses = ",".join(f"{host}:{port}" for host, port in self.cluster.endpoints)

    def start_collector(self, shards: str) -> subprocess.Popen:
        """collector.py taking `shards`, once it says it is ready."""
        command = WITH_THE_TEST + [sys.executable, "collector.py", self.addresses, "--shards", shards]
        command += ["--dir", self.directory]
        collector = subprocess.Popen(command, cwd=HERE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self.addCleanup(end, collector)
        assert collector.stdout is not None
        self.assertEqual(f"ready: shards {shards.replace(',', ', ')}\n", collector.stdout.readline())
        return collector

    def cli(self, *arguments: str) -> str:
        """What clicks_cli.py prints. send counts the rest lost after a minute with no click stored,
        not its five seconds, which collectors stalled by a busy machine can outlast: a bound that
        only detects a failure."""
        command = [sys.executable, "clicks_cli.py", self.addresses, *arguments]
        environment = {**os.environ, "NOTHING_MORE": "60"}
        return subprocess.run(
            command, cwd=HERE, env=environment, capture_output=True, text=True, check=True, timeout=120
        ).stdout

    def test_the_steps(self):
        self.assertEqual(
            "".join(f"shard.{shard}: no answer, OperationFailed\n" for shard in range(SHARDS)) + "0 clicks\n",
            self.cli("counts"),
            "no collector: every shard's query fails at once",
        )
        collectors = [self.start_collector("0,1"), self.start_collector("2,3")]
        sent = self.cli("send", "--clicks", "3000", "--sources", "2", "--pages", "3")
        self.assertRegex(
            sent,
            r"^3,000 clicks from 2 sources, batched with a delay of 0\.05 s: 3,000 stored in \d+\.\d\d s, [\d,]+ a"
            r" second; none lost\nthe multiplexers' CPU: [\d.e+-]+ s and [\d.e+-]+ s, [\d.e+-]+ s a million clicks\n$",
        )
        self.assertEqual("/page/0: 1,000\n/page/1: 1,000\n/page/2: 1,000\n3,000 clicks\n", self.cli("counts"))
        self.assertRegex(
            self.cli("send", "--clicks", "1000", "--sources", "1", "--no-batching"),
            r"^1,000 clicks from 1 source, not batched: 1,000 stored in [^;]*; none lost\n",
        )
        # The tap, while a source sends: what it saw, every click told.
        watch = subprocess.Popen(
            [sys.executable, "clicks_cli.py", self.addresses, "watch", "--clicks", "2000"],
            cwd=HERE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        self.addCleanup(end, watch)
        assert watch.stdout is not None
        self.assertEqual("tapping\n", watch.stdout.readline())
        self.cli("send", "--clicks", "2000", "--sources", "1")
        self.assertRegex(watch.stdout.read(), r"^[\d,]+ CLICK messages carried 2,000 clicks: ")
        self.assertEqual(0, watch.wait(60))
        # SIGTERM: each collector leaves, its files written out.
        for collector in collectors:
            collector.send_signal(signal.SIGTERM)
        left = []
        for collector in collectors:
            assert collector.stdout is not None
            said = re.fullmatch(r"left after storing (\d+) clicks\n", collector.stdout.read())
            assert said is not None
            left.append(int(said.group(1)))
            self.assertEqual(0, collector.wait(30))
        self.assertEqual(6000, sum(left))
        self.assertEqual(6000, sum(len(lines_of(self.directory, shard)) for shard in range(SHARDS)))


def end(process: subprocess.Popen) -> None:
    """Kill a process the test started, unless it is gone already, and reap it."""
    if process.poll() is None:
        process.kill()
    process.wait(10)
    if process.stdout is not None:
        process.stdout.close()


class ConstantsTest(unittest.TestCase):
    """The committed constants are what mxcontrol writes from the rules file
    as it is now: a change to the file that left them behind fails here."""

    def test_the_constants_are_generated_from_the_rules_file(self):
        out = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, out, True)
        written = [os.path.join(out, name) for name in ("multiplexer_constants.py", "multiplexer_constants.pyi")]
        command = [mxcontrol_path(), "generate_constants", "clicks.rules", "--python", written[0], "--pyi", written[1]]
        subprocess.run(command, cwd=HERE, check=True, capture_output=True)  # the file's name as the header records it
        for path in written:
            with open(path) as generated, open(os.path.join(HERE, os.path.basename(path))) as committed:
                self.assertEqual(committed.read(), generated.read(), os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
