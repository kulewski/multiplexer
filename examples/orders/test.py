"""The example's test on the library's harness (docs/api_python.md,
"Testing"): real multiplexers from `Cluster`, workers served on threads by
`BackendThread` or run as processes, and the shop in the test. An order's
callback hears 1 only once the order is in the ledger, and flush_all()
returns True with every order there; a worker that dies at its fourth
order has what it held sent again, to the other worker, and the ledger
skips the three it had recorded; an order a worker raises on is given up
on, FAILED, and with no worker an order is given up on at its batch's
deadline, NOT_CONFIRMED; a raw peer sees each order go alone and
numbered, one by one, and kept until it confirms them. Then the programs
as the steps run them, and the committed constants checked against the
rules file. The multiplexers are the mxcontrol the package installed
unless MXCONTROL names another; `python -m unittest -v test`.

The walkthrough's "Testing it with the harness" walks through this file."""

import os
import queue
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from typing import Callable

HERE = os.path.dirname(os.path.abspath(__file__))
# The example's own modules import by name, as they do when its scripts run
# from its directory; this test may run from anywhere, so the example's
# directory goes first on the path.
sys.path.insert(0, HERE)

from multiplexer.Multiplexer_pb2 import Confirmation, MessageBatch, MultiplexerMessage
from multiplexer.mxclient import DropReason
from multiplexer.testing import BackendThread, Cluster, RawPeer, mxcontrol_path

from ledger import Ledger
from multiplexer_constants import peers, types
from orders_cli import make_order
from orders_pb2 import Order
from shop import Shop
from worker import Worker

RULES = os.path.join(HERE, "orders.rules")  # what the constants were generated from; Cluster's multiplexers read it
HOUR = 3600.0  # a batch's delay that never runs out here: a batch goes at a flush; twice that, its timeout
PER_BATCH = 10  # orders placed between two flushes, each flush letting their batch go
BATCHES = 10
CRASH_AT = 4  # the doomed worker dies at its fourth order, three recorded
BOUND = 30.0  # seconds that only a failure waits
# A worker process dies with the test process however that ends, where util-linux's setpriv is there.
WITH_THE_TEST = ["setpriv", "--pdeathsig", "KILL", "--"] if shutil.which("setpriv") else []


def place_in_batches(shop: Shop, orders: list[Order], callback: Callable[[Order], Callable[[int], None]]) -> None:
    """Place `orders`, PER_BATCH at a time, each with the callback
    `callback(order)` makes, a flush that waits for nothing letting each
    batch go: with a delay that never runs out, the batches are the test's."""
    for start in range(0, len(orders), PER_BATCH):
        for order in orders[start : start + PER_BATCH]:
            shop.place(order, callback(order))
        shop.flush(0)


def start_worker(test: unittest.TestCase, endpoints: list[tuple[str, int]], ledger: str, name: str, *options: str):
    """worker.py as a process, once it says it is ready; its stderr, where
    a handler's traceback goes, left out."""
    addresses = ",".join(f"{host}:{port}" for host, port in endpoints)
    command = WITH_THE_TEST + [sys.executable, "worker.py", addresses, "--name", name, "--ledger", ledger, *options]
    worker = subprocess.Popen(command, cwd=HERE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    test.addCleanup(end, worker)
    assert worker.stdout is not None
    test.assertEqual(f"ready: {name}\n", worker.stdout.readline())
    return worker


def end(process: subprocess.Popen) -> None:
    """Kill a process the test started, unless it is gone already, and reap it."""
    if process.poll() is None:
        process.kill()
    process.wait(10)
    if process.stdout is not None:
        process.stdout.close()


class LedgerFile(unittest.TestCase):
    """A test with a ledger file of its own, in a directory removed after it."""

    def setUp(self):
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, True)
        self.ledger = os.path.join(directory, "ledger.db")

    def recorded(self) -> tuple[int, dict[str, int], dict[str, tuple[int, int]]]:
        """What the ledger holds: how many orders, by worker, and the repeats."""
        with closing(Ledger(self.ledger)) as ledger:
            return ledger.count(), ledger.by_worker(), ledger.repeats()


class ConfirmedTest(LedgerFile):
    """Two multiplexers, two workers on threads, and the shop."""

    def test_an_order_is_confirmed_once_it_is_in_the_ledger(self):
        cluster = Cluster(2, rules=RULES).__enter__()
        self.addCleanup(cluster.__exit__, None, None, None)
        for name in ("w1", "w2"):
            served = BackendThread(lambda name=name: Worker(cluster.endpoints, name, self.ledger))
            self.addCleanup(served.start().stop)
        # The shop's io thread runs the callbacks, one at a time, and reads
        # the ledger with this connection as each order is confirmed.
        reader = sqlite3.connect(self.ledger, check_same_thread=False)
        self.addCleanup(reader.close)
        ended: list[int] = []
        early: list[str] = []  # orders confirmed before the ledger had them

        def callback(order: Order) -> Callable[[int], None]:
            """What hears `order`'s end: whether the ledger had it then."""

            def confirmed(outcome: int) -> None:
                ended.append(outcome)
                if outcome and reader.execute("SELECT 1 FROM orders WHERE id = ?", (order.id,)).fetchone() is None:
                    early.append(order.id)

            return confirmed

        orders = [make_order(number) for number in range(PER_BATCH * BATCHES)]
        with Shop(cluster.endpoints, max_delay=HOUR, timeout=2 * HOUR) as shop:
            place_in_batches(shop, orders, callback)
            self.assertTrue(shop.flush(BOUND), "every order confirmed")
            count, by_worker, repeats = self.recorded()  # the moment flush_all() returned
            self.assertEqual(0, shop.sent_again)
        self.assertEqual([1] * len(orders), ended)
        self.assertEqual([], early, "confirmed only once in the ledger")
        self.assertEqual(len(orders), count, "every order in the ledger when flush_all() returned")
        self.assertEqual({"w1", "w2"}, set(by_worker), "the batches shared out")
        self.assertEqual({}, repeats)


class DyingWorkerTest(LedgerFile):
    """One multiplexer, so that the batches go to the two workers in turn:
    a survivor on a thread, and a worker that dies at its fourth order, a
    process of its own."""

    def test_what_a_dead_worker_held_goes_again_and_the_ledger_skips_what_it_recorded(self):
        cluster = Cluster(1, rules=RULES).__enter__()
        self.addCleanup(cluster.__exit__, None, None, None)
        self.addCleanup(BackendThread(lambda: Worker(cluster.endpoints, "survivor", self.ledger)).start().stop)
        doomed = start_worker(self, cluster.endpoints, self.ledger, "doomed", "--crash-at", str(CRASH_AT))
        ended: list[int] = []
        orders = [make_order(number) for number in range(PER_BATCH * BATCHES)]
        with Shop(cluster.endpoints, max_delay=HOUR, timeout=2 * HOUR) as shop:
            place_in_batches(shop, orders, lambda order: ended.append)
            # What the doomed worker held, nothing confirmed it for
            # RETRY_AFTER: sent again, to the survivor, and confirmed.
            self.assertTrue(shop.flush(BOUND), "every order confirmed")
            self.assertGreaterEqual(shop.sent_again, PER_BATCH, "the batch it died in, at least")
        self.assertEqual([1] * len(orders), ended)
        self.assertEqual(3, doomed.wait(BOUND))
        assert doomed.stdout is not None
        self.assertEqual(f"doomed: dying at order {CRASH_AT}, before recording it\n", doomed.stdout.read())
        # The three it recorded came again, their confirmations having died
        # with it: the ledger skipped them, each once.
        count, by_worker, repeats = self.recorded()
        self.assertEqual(len(orders), count)
        self.assertEqual(CRASH_AT - 1, by_worker["doomed"])
        self.assertEqual({"doomed": (CRASH_AT - 1, CRASH_AT - 1)}, repeats)


class GivenUpTest(LedgerFile):
    """One multiplexer: an order a worker raises on, and orders with no worker at all."""

    def setUp(self):
        super().setUp()
        self.cluster = Cluster(1, rules=RULES).__enter__()
        self.addCleanup(self.cluster.__exit__, None, None, None)
        self.drops: queue.SimpleQueue[tuple[int, DropReason]] = queue.SimpleQueue()

    def dropped(self, message_id: int, reason: DropReason) -> None:
        """The shop's on_drop."""
        self.drops.put((message_id, reason))

    def test_an_order_a_worker_raises_on_is_failed_and_not_sent_again(self):
        start_worker(self, self.cluster.endpoints, self.ledger, "w1")
        orders = [make_order(number) for number in range(3)]
        orders[1].quantity = 0  # for nothing: the worker raises on it
        ended: dict[str, int] = {}
        with Shop(self.cluster.endpoints, max_delay=HOUR, timeout=2 * HOUR, on_drop=self.dropped) as shop:
            ids = [
                shop.place(order, lambda outcome, order=order: ended.update({order.id: outcome})) for order in orders
            ]
            self.assertFalse(shop.flush(BOUND), "one was given up on")
            self.assertEqual(0, shop.sent_again)
        self.assertEqual((ids[1], DropReason.FAILED), self.drops.get_nowait())
        self.assertTrue(self.drops.empty())
        self.assertEqual({orders[0].id: 1, orders[1].id: 0, orders[2].id: 1}, ended)
        self.assertEqual(2, self.recorded()[0])

    def test_with_no_worker_an_order_is_given_up_on_at_its_deadline(self):
        ended: list[int] = []
        # A deadline of a second, the one wait of the test that is not a failure's.
        with Shop(self.cluster.endpoints, timeout=1.0, on_drop=self.dropped) as shop:
            ids = [shop.place(make_order(number), ended.append) for number in range(3)]
            self.assertFalse(shop.flush(BOUND), "every one given up on")
        self.assertEqual([0, 0, 0], ended)
        self.assertEqual(
            {message_id: DropReason.NOT_CONFIRMED for message_id in ids}, dict(self.drops.get_nowait() for _ in ids)
        )


class WireTest(unittest.TestCase):
    """A raw peer of the workers' type, the harness speaking the wire
    format by hand, sees what the shop sends one by one."""

    def test_one_by_one_each_order_goes_alone_numbered_and_is_kept_until_confirmed(self):
        cluster = Cluster(1, rules=RULES).__enter__()
        self.addCleanup(cluster.__exit__, None, None, None)
        wire = RawPeer(cluster.endpoints[0], peers.FULFILLMENT_WORKER)
        self.addCleanup(wire.close)
        wire.handshake()
        ended: queue.SimpleQueue[int] = queue.SimpleQueue()
        with Shop(cluster.endpoints, one_by_one=True) as shop:
            ids = [shop.place(make_order(number), ended.put) for number in range(3)]
            envelopes = [wire.receive_type(types.ORDER_PLACED, BOUND) for _ in ids]
            # Each order alone, in a batch of one that numbers it: (first, count).
            for number, (message_id, envelope) in enumerate(zip(ids, envelopes, strict=True), start=1):
                inside = MessageBatch.FromString(envelope.batch).message
                self.assertEqual([message_id], [MultiplexerMessage.FromString(body).id for body in inside])
                self.assertEqual([number, 1], list(envelope.confirm))
            self.assertTrue(ended.empty(), "kept: nothing confirmed them yet")
            # One CONFIRMATION for the three, as a worker's library sends it.
            confirmation = Confirmation(processed=[1, len(ids)])
            wire.send(confirmation.SerializeToString(), types.CONFIRMATION, to=envelopes[0].sender)
            self.assertTrue(shop.flush(BOUND))
        self.assertEqual([1, 1, 1], [ended.get(timeout=BOUND) for _ in ids])


class ProgramTest(LedgerFile):
    """worker.py and orders_cli.py as the steps run them: two workers,
    orders placed confirmed and not, an order for nothing, the latency, the
    workers leaving on SIGTERM, the ledger, and orders with no worker."""

    def setUp(self):
        super().setUp()
        self.cluster = Cluster(2, rules=RULES).__enter__()
        self.addCleanup(self.cluster.__exit__, None, None, None)
        self.addresses = ",".join(f"{host}:{port}" for host, port in self.cluster.endpoints)

    def cli(self, *arguments: str) -> str:
        """What orders_cli.py prints."""
        command = [sys.executable, "orders_cli.py", *arguments]
        return subprocess.run(command, cwd=HERE, capture_output=True, text=True, check=True, timeout=120).stdout

    def test_the_steps(self):
        workers = [start_worker(self, self.cluster.endpoints, self.ledger, name) for name in ("w1", "w2")]
        sent_again = r"(the shop sent [\d,]+ again, nothing having confirmed them\n)?"
        self.assertRegex(
            self.cli("place", self.addresses, "--orders", "2000", "--ledger", self.ledger),
            r"^2,000 orders, confirmed in batches: 2,000 confirmed, none given up on\n"
            + sent_again
            + r"flush_all\(\) returned True after \d+\.\d\d s, when the ledger held 2,000 of them\n$",
        )
        self.assertRegex(
            self.cli("place", self.addresses, "--orders", "1000", "--unconfirmed", "--ledger", self.ledger),
            r"^1,000 orders, sent in batches, unconfirmed: 1,000 written, none given up on\n"
            r"flush_all\(\) returned True after \d+\.\d\d s, when the ledger held [\d,]+ of them\n$",
        )
        self.assertRegex(
            self.cli("place", self.addresses, "--orders", "10", "--empty", "2", "--ledger", self.ledger),
            r"^10 orders, confirmed in batches: 8 confirmed, 2 given up on \(FAILED 2\)\n"
            + sent_again
            + r"flush_all\(\) returned False after \d+\.\d\d s, when the ledger held 8 of them\n$",
        )
        for how, options in (("in batches with a delay of 5 ms", []), ("one by one", ["--one-by-one"])):
            self.assertRegex(
                self.cli("latency", self.addresses, "--orders", "20", *options),
                rf"^20 orders, one at a time, confirmed {how}: confirmed \d+\.\d\d ms after the send at the median,"
                r" \d+\.\d\d ms at the 99th percentile\n$",
            )
        # SIGTERM: each worker leaves, saying what it recorded, the plain
        # events among it, which reached it in their own time.
        for worker in workers:
            worker.send_signal(signal.SIGTERM)
        left = 0
        for worker, name in zip(workers, ("w1", "w2"), strict=True):
            assert worker.stdout is not None
            said = re.fullmatch(
                rf"{name} left after recording ([\d,]+) orders and skipping [\d,]+ repeats\n", worker.stdout.read()
            )
            assert said is not None
            left += int(said.group(1).replace(",", ""))
            self.assertEqual(0, worker.wait(30))
        self.assertEqual(3048, left, "the 3,000 placed, 8 of the 10 and the 40 timed")
        self.assertRegex(self.cli("ledger", "--ledger", self.ledger), r"^3,048 orders: w[12] [\d,]+(, w2 [\d,]+)?\n")
        # No worker: the orders are given up on at their deadline, a second.
        self.assertRegex(
            self.cli("place", self.addresses, "--orders", "3", "--timeout", "1", "--ledger", self.ledger),
            r"^3 orders, confirmed in batches: none confirmed, 3 given up on \(NOT_CONFIRMED 3\)\n"
            + sent_again
            + r"flush_all\(\) returned False after 1\.\d\d s, when the ledger held 0 of them\n$",
        )


class ConstantsTest(unittest.TestCase):
    """The committed constants are what mxcontrol writes from the rules file
    as it is now: a change to the file that left them behind fails here."""

    def test_the_constants_are_generated_from_the_rules_file(self):
        out = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, out, True)
        written = [os.path.join(out, name) for name in ("multiplexer_constants.py", "multiplexer_constants.pyi")]
        command = [mxcontrol_path(), "generate_constants", "orders.rules", "--python", written[0], "--pyi", written[1]]
        subprocess.run(command, cwd=HERE, check=True, capture_output=True)  # the file's name as the header records it
        for path in written:
            with open(path) as generated, open(os.path.join(HERE, os.path.basename(path))) as committed:
                self.assertEqual(committed.read(), generated.read(), os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
