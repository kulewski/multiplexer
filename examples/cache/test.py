"""The example's test, and the way to test a backend of your own with the
library's harness, `multiplexer.testing` (docs/api_python.md, "Testing"):
real multiplexers on ports of their own from `Cluster`, the replicas
served on `BackendThread`s, waits on what the multiplexers' peers files
say, a multiplexer frozen with reads inside it and killed under
traffic, then brought back, and Django configured against the cluster;
and the committed constants checked against the rules file. Needs Django,
which test.sh arranges; the multiplexers are the mxcontrol the package
installed unless MXCONTROL names another; `python -m unittest -v test`.

The walkthrough's "Testing it with the harness" walks through this file."""

import io
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "web"))

from multiplexer.testing import BackendThread, Cluster, mxcontrol_path, wait_until  # noqa: E402

from cache_pb2 import CacheValue  # noqa: E402
from journal import Journal  # noqa: E402
from multiplexer_constants import peers  # noqa: E402
from mxcache import Cache  # noqa: E402
from replica import Replica  # noqa: E402

RULES = os.path.join(HERE, "cache.rules")  # what the constants were generated from; Cluster's multiplexers read it


def endpoints_text(cluster: Cluster) -> str:
    """The cluster's multiplexers as the addresses a settings file names."""
    return ",".join(f"{host}:{port}" for host, port in cluster.endpoints)


def without_journal(rules: str) -> str:
    """The rules file as it was before the journal, its peer and its
    destinations taken out: what the multiplexers run before the test
    puts the committed file under them."""
    rules = re.sub(r'peer \{\n    type: 203\n    name: "CACHE_JOURNAL"\n.*?\n\}\n\n', "", rules, flags=re.DOTALL)
    rules = re.sub(
        r'    to \{\n        peer: "CACHE_JOURNAL"\n        whom: ALL\n        report_delivery_error: false\n    \}\n',
        "",
        rules,
    )
    assert "CACHE_JOURNAL" not in rules
    return rules


def without_header(rules: str) -> tuple[str, str]:
    """A rules file of this example without the comment that opens the
    example's part and says what the file is: the system rules before it,
    and the example's peers and messages after it."""
    system, own = rules.split("\n# The cache example's ", 1)
    return system, own[own.index("# The example's peers and messages.") :]


class CacheTest(unittest.TestCase):
    """Two multiplexers and three replicas for the whole class; a client per test."""

    @classmethod
    def setUpClass(cls):
        # Cluster(2) starts two multiplexers, each on a port the system
        # picked, reading the example's rules file; endpoints is what a
        # client connects to. BackendThread builds a replica on a thread of
        # its own and serves it there; start() returns once it is
        # connected. wait_for_peer reads the multiplexers' peers files, so
        # it returns only when every multiplexer has all three registered.
        cls.cluster = Cluster(2, rules=RULES).__enter__()
        cls.replicas = [
            BackendThread(lambda name=f"r{index}": Replica(cls.cluster.endpoints, name=name)).start()
            for index in range(3)
        ]
        cls.cluster.wait_for_peer(peers.CACHE, count=3)

    @classmethod
    def tearDownClass(cls):
        for replica in cls.replicas:
            if replica.running:
                replica.stop()  # asks it to leave and joins the thread; re-raises what serving raised
        cls.cluster.__exit__(None, None, None)

    def setUp(self):
        self.cache = Cache(self.cluster.endpoints)
        self.cache.clear()  # an event through every multiplexer; a read from this client follows it in order

    def tearDown(self):
        self.cache.close()

    def lookup(self, key: str) -> CacheValue:
        """A read that some replica must answer; the test fails otherwise."""
        value = self.cache.lookup(key)
        self.assertIsNotNone(value, f"no replica answered a read of {key}")
        assert value is not None
        return value

    def test_a_write_reaches_every_replica_and_a_read_any_one(self):
        self.cache.set("k", b"v")
        self.assertEqual(b"v", self.cache.get("k"))
        stats = self.cache.stats()
        self.assertEqual(["r0", "r1", "r2"], [each.replica for each in stats], "every replica answered")
        self.assertEqual([1, 1, 1], [each.entries for each in stats], "every replica holds the key")
        answered = {self.lookup("k").replica for _ in range(12)}
        self.assertEqual({"r0", "r1", "r2"}, answered, "reads go round robin over the replicas")

    def test_delete_and_clear_reach_every_replica(self):
        self.cache.set("a", b"1")
        self.cache.set("b", b"2")
        self.cache.delete("a")
        self.assertIsNone(self.cache.get("a"))
        self.assertEqual(b"2", self.cache.get("b"))
        self.cache.clear()
        self.assertIsNone(self.cache.get("b"))
        self.assertEqual([0, 0, 0], [each.entries for each in self.cache.stats()])

    def test_an_entry_expires(self):
        self.cache.set("soon", b"gone", expires=time.time() + 0.3)
        self.assertEqual(b"gone", self.cache.get("soon"))
        time.sleep(0.4)
        self.assertIsNone(self.cache.get("soon"))

    def test_add_is_decided_by_one_replica_for_all(self):
        self.assertTrue(self.cache.add("once", b"first"))
        self.assertFalse(self.cache.add("once", b"second"))
        answers = self.answers_from_every_replica("once")
        self.assertEqual({"r0": b"first", "r1": b"first", "r2": b"first"}, answers, "the same value everywhere")

    def test_a_read_right_after_an_add_finds_it(self):
        """The client sends an added entry to every replica before add()
        returns, through the connections its next read goes through, so
        the read cannot overtake it, whichever replica answers."""
        for index in range(300):
            key = f"added-{index}"
            self.assertTrue(self.cache.add(key, b"v"))
            self.assertEqual(b"v", self.cache.get(key), key)
            self.assertFalse(self.cache.add(key, b"w"), key)

    def test_a_late_copy_of_an_older_write_changes_nothing(self):
        """What a stalled multiplexer delivers after the newer writes went
        through the other one: older stamps, which every replica ignores,
        however late they come, even past the library's window of ids."""
        from multiplexer.threaded_client import ThreadedClient

        from cache_pb2 import CacheEntry
        from multiplexer_constants import types

        late = ThreadedClient(self.cluster.endpoints, type=peers.CACHE_CLIENT)
        try:
            self.cache.set("kept", b"new")
            self.cache.delete("gone")
            for key in ("kept", "gone"):
                old = CacheEntry(key=key, value=b"old", version=1, writer=1)
                late.send_message(old, type=types.CACHE_SET, multiplexer=ThreadedClient.ALL, flush=True)
            self.assertEqual({"r0": b"new", "r1": b"new", "r2": b"new"}, self.answers_from_every_replica("kept"))
            self.assertEqual({"r0": None, "r1": None, "r2": None}, self.answers_from_every_replica("gone"))
        finally:
            late.shutdown()

    def test_an_add_sent_twice_is_one_add(self):
        """What the library does when a connection dies under an add: the
        same request again, the same stamp, which the replica knows."""
        from multiplexer.threaded_client import ThreadedClient

        from cache_pb2 import CacheEntry, CacheResult
        from multiplexer_constants import types

        raw = ThreadedClient(self.cluster.endpoints, type=peers.CACHE_CLIENT)
        try:
            entry = CacheEntry(key="twice", value=b"v", version=time.time_ns(), writer=raw.instance_id)
            results = []
            for _ in range(2):
                result = CacheResult()
                result.ParseFromString(raw.query(entry, type=types.CACHE_ADD, timeout=5).message)
                results.append(result.done)
                # What Cache.add does on a yes: the entry to every replica, ahead of the next query.
                raw.send_message(entry, type=types.CACHE_SET, multiplexer=ThreadedClient.ALL)
            other = CacheEntry(key="twice", value=b"w", version=time.time_ns(), writer=raw.instance_id)
            result = CacheResult()
            result.ParseFromString(raw.query(other, type=types.CACHE_ADD, timeout=5).message)
            self.assertEqual([True, True, False], results + [result.done])
        finally:
            raw.shutdown()

    def answers_from_every_replica(self, key: str) -> dict[str, bytes | None]:
        """What each replica holds for `key`: reads until every one of them has answered."""
        answers: dict[str, bytes | None] = {}
        for _ in range(60):
            value = self.lookup(key)
            answers[value.replica] = value.value if value.found else None
            if len(answers) == 3:
                break
        return answers

    def test_a_restarted_replica_starts_empty_and_warms_as_keys_are_written(self):
        for index in range(5):
            self.cache.set(f"key-{index}", b"x")
        self.assertEqual([5, 5, 5], [each.entries for each in self.cache.stats()])
        self.replicas[0].stop()
        # connected_peers() is the multiplexer's peers file: (id, name, number) per peer.
        wait_until(
            lambda: all(
                sum(1 for _, _, number in mx.connected_peers() if number == peers.CACHE) == 2 for mx in self.cluster.mx
            ),
            10,
            "r0 gone from both multiplexers",
        )
        self.replicas[0] = BackendThread(lambda: Replica(self.cluster.endpoints, name="r0")).start()
        self.cluster.wait_for_peer(peers.CACHE, count=3)
        self.assertEqual([0, 5, 5], [each.entries for each in self.cache.stats()], "nothing is replayed to it")
        lookups = [self.lookup(f"key-{index % 5}") for index in range(15)]
        self.assertTrue(any(not each.found and each.replica == "r0" for each in lookups), "r0 misses")
        self.assertTrue(all(each.found for each in lookups if each.replica != "r0"), "the others hit")
        for index in range(5):
            self.cache.set(f"key-{index}", b"x")  # what a cache's callers do on a miss
        self.assertEqual([5, 5, 5], [each.entries for each in self.cache.stats()])

    def test_a_multiplexer_may_die_under_writes_and_reads(self):
        """The multiplexer is frozen first, so that the reads the round robin
        gives it wait inside it, and killed half a second later: a read is in
        flight when it dies, and is answered all the same."""
        stale = misses = held = 0
        killer = threading.Timer(0.5, self.cluster.mx[0].kill)  # SIGKILL: no goodbye, the connections drop
        for index in range(300):
            if index == 100:
                self.cluster.mx[0].pause()  # SIGSTOP: its sockets stay open, so nothing notices yet
                killer.start()
            value = str(index).encode()
            started = time.monotonic()
            self.cache.set("counter", value)
            got = self.cache.get("counter")
            if time.monotonic() - started > 0.3:
                held += 1  # waited inside the frozen multiplexer, then was sent again through the other
            if got is None:
                misses += 1
            elif got != value:
                stale += 1
        killer.join()
        self.assertEqual((0, 0, 0), (stale, misses, self.cache.unavailable), "nothing a caller could notice")
        self.assertGreater(held, 0, "a read was inside the multiplexer when it died")
        self.cluster.mx[0].start()
        self.cluster.wait_for_peer(peers.CACHE, count=3)  # the replicas reconnect on their own, within 3 s
        self.cluster.wait_for_peer(peers.CACHE_CLIENT)  # and so does this client

    def test_writes_through_every_multiplexer_are_handled_once_per_replica(self):
        before = {each.replica: each.writes for each in self.cache.stats()}
        for index in range(50):
            self.cache.set(f"dup-{index}", b"x")  # two multiplexers: two copies of each, one per multiplexer
        after = {each.replica: each.writes for each in self.cache.stats()}
        self.assertEqual({"r0": 50, "r1": 50, "r2": 50}, {name: after[name] - before[name] for name in before})

    def test_a_read_after_a_write_is_never_stale_with_writes_through_every_multiplexer(self):
        stale = 0
        for index in range(2000):
            value = str(index).encode()
            self.cache.set("pair", value)
            if self.cache.get("pair") != value:
                stale += 1
        self.assertEqual(0, stale, "the read follows the write's copy on whichever connection it takes")


class JournalTest(unittest.TestCase):
    """The rules change under running multiplexers: the journal's peer type
    and its destinations go into the file the multiplexers read, they put
    it in use on their own, and a journal receives the writes from then
    on; nobody else is restarted. A template for testing a rules change."""

    def test_before_rules_is_cache_rules_without_the_journal(self):
        """The file step 8 starts from; the comment that opens the example's
        part says what it is."""
        with open(RULES) as f:
            expected = without_journal(f.read())
        with open(os.path.join(HERE, "before.rules")) as f:
            before = f.read()
        self.assertEqual(without_header(expected), without_header(before))

    def test_a_journal_joins_while_the_cache_runs(self):
        with open(RULES) as f:
            rules = f.read()
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "cache.rules")
            with open(path, "w") as f:
                f.write(without_journal(rules))
            # rules_check_interval: how often each multiplexer reads its file again, 2 s unless told otherwise
            with Cluster(2, rules=path, rules_check_interval=0.2) as cluster:
                replica = BackendThread(lambda: Replica(cluster.endpoints, name="r0")).start()
                cluster.wait_for_peer(peers.CACHE)
                cache = Cache(cluster.endpoints)
                lines = io.StringIO()
                journal = None
                try:
                    cache.set("before", b"1")
                    with open(path, "w") as f:
                        f.write(
                            rules
                        )  # the file as committed: the journal's peer, and its destination on the three events
                    for mx in cluster.mx:
                        wait_until(lambda mx=mx: mx.log_contains("rules reloaded from"), 5, "the new file put in use")
                    journal = BackendThread(lambda: Journal(cluster.endpoints, lines)).start()
                    cluster.wait_for_peer(peers.CACHE_JOURNAL)
                    cache.set("after", b"22")
                    cache.delete("before")
                    cache.clear()
                    wait_until(lambda: journal.backend is not None and journal.backend.lines == 3, 5, "three lines")
                    entries = [json.loads(line) for line in lines.getvalue().splitlines()]
                    self.assertEqual(["set", "delete", "clear"], [entry["event"] for entry in entries])
                    self.assertEqual(("after", 2), (entries[0]["key"], entries[0]["bytes"]))
                    self.assertEqual("before", entries[1]["key"])
                    self.assertEqual(
                        0, cache.lost_writes, "no delivery error for the journal's destination, before or after"
                    )
                finally:
                    cache.close()
                    if journal is not None:
                        journal.stop()
                    replica.stop()


class NoReplicaTest(unittest.TestCase):
    """A multiplexer with nobody behind it."""

    def test_a_read_is_a_miss_at_once_and_a_write_is_reported_lost(self):
        with Cluster(1, rules=RULES) as cluster:
            cache = Cache(cluster.endpoints)
            try:
                started = time.monotonic()
                self.assertIsNone(cache.get("anything"))
                self.assertLess(time.monotonic() - started, 2, "OperationFailed, not a timeout")
                self.assertEqual(1, cache.unavailable)
                cache.set("anything", b"x")
                wait_until(lambda: cache.lost_writes == 1, 5, "the multiplexer's delivery error")
            finally:
                cache.close()


class DjangoBackendTest(unittest.TestCase):
    """Django's cache API over two replicas, and a view under cache_page."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(2, rules=RULES).__enter__()
        cls.replicas = [
            BackendThread(lambda name=f"r{index}": Replica(cls.cluster.endpoints, name=name)).start()
            for index in range(2)
        ]
        cls.cluster.wait_for_peer(peers.CACHE, count=2)
        # The settings read the multiplexers' addresses from the environment
        # when Django imports them, which setup() does.
        os.environ["MX_ADDRESSES"] = endpoints_text(cls.cluster)
        os.environ["DJANGO_SETTINGS_MODULE"] = "webapp.settings"
        import django

        django.setup()

    @classmethod
    def tearDownClass(cls):
        from mxcache import backend

        for client in backend._clients.values():
            client.close()
        backend._clients.clear()
        for replica in cls.replicas:
            replica.stop()
        cls.cluster.__exit__(None, None, None)

    def setUp(self):
        from django.core.cache import cache

        self.cache = cache
        self.cache.clear()

    def test_the_cache_api(self):
        self.cache.set("a", {"x": 1})
        self.assertEqual({"x": 1}, self.cache.get("a"))
        self.assertEqual("default", self.cache.get("missing", "default"))
        self.assertFalse(self.cache.add("a", 2))
        self.assertTrue(self.cache.add("b", 2))
        self.assertEqual(3, self.cache.incr("b"))
        self.assertEqual("made", self.cache.get_or_set("c", "made"))
        self.assertEqual({"a": {"x": 1}, "b": 3, "c": "made"}, self.cache.get_many(["a", "b", "c"]))
        self.assertTrue(self.cache.has_key("c"))
        self.assertTrue(self.cache.touch("a", 0.3))
        self.assertFalse(self.cache.touch("nothing", 0.3))
        time.sleep(0.4)
        self.assertIsNone(self.cache.get("a"), "touched to expire in 0.3 s")
        self.cache.delete("b")
        self.assertIsNone(self.cache.get("b"))
        self.cache.set("zero", 1, timeout=0)
        self.assertIsNone(self.cache.get("zero"), "a timeout of 0 expires at once")
        self.cache.clear()
        self.assertFalse(self.cache.has_key("c"))

    def test_the_cached_page_is_shared_and_cleared(self):
        from django.test import Client as WebClient

        web = WebClient()
        first = web.get("/slow").content
        started = time.monotonic()
        self.assertEqual(first, web.get("/slow").content, "the cached page, from any web process")
        self.assertLess(time.monotonic() - started, 0.3, "no half second of work the second time")
        self.assertEqual(b"cleared\n", web.get("/clear").content)
        self.assertNotEqual(first, web.get("/slow").content, "computed again after the clear")
        self.assertEqual([b"1\n", b"2\n"], [web.get("/counter").content for _ in range(2)])


class ConstantsTest(unittest.TestCase):
    """The committed constants are what mxcontrol writes from the rules file
    as it is now: a change to the file that left them behind fails here."""

    def test_the_constants_are_generated_from_the_rules_file(self):
        out = tempfile.mkdtemp()
        written = [os.path.join(out, name) for name in ("multiplexer_constants.py", "multiplexer_constants.pyi")]
        command = [mxcontrol_path(), "generate_constants", "cache.rules", "--python", written[0], "--pyi", written[1]]
        subprocess.run(command, cwd=HERE, check=True, capture_output=True)  # the file's name as the header records it
        for path in written:
            with open(path) as generated, open(os.path.join(HERE, os.path.basename(path))) as committed:
                self.assertEqual(committed.read(), generated.read(), os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
