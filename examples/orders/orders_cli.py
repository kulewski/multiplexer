"""The command line the steps use: orders placed and how they ended, the
time from an order's send to its confirmation, and what the ledger holds.

    python orders_cli.py place ADDRESSES [--orders N] [--one-by-one] [--unconfirmed]
                                         [--timeout SECONDS] [--empty N] [--ledger FILE]
    python orders_cli.py latency ADDRESSES [--orders N] [--one-by-one]
    python orders_cli.py ledger [--ledger FILE]

ADDRESSES is host:port of every multiplexer, comma-separated. place places
N orders as fast as it goes, the first N of them for nothing with --empty,
which a worker raises on; then it flushes, and prints how the orders
ended, as their callbacks and on_drop heard it, how many the shop sent
again, and how many of them the ledger held when flush_all() returned.
--unconfirmed sends them as plain events, for the contrast. latency places
N orders one at a time, each once the one before was confirmed, and
prints how long a confirmation took, at the median and the 99th
percentile. ledger prints how many orders the ledger holds, by the worker
that recorded each, and the repeats it skipped."""

import argparse
import collections
import queue
import statistics
import time
from contextlib import closing

from multiplexer.mxclient import DropReason

from ledger import Ledger
from orders_pb2 import Order
from shop import MAX_DELAY, TIMEOUT, Shop, new_order_id, parse_addresses

CUSTOMERS = ["alice", "bob", "carol", "dave", "erin"]
ITEMS = ["mug", "teapot", "kettle", "tray"]
SLACK = 10.0  # seconds past its batch's timeout that only a failure waits for an order's end


def make_order(number: int) -> Order:
    """The `number`th order of a run, under a fresh id: the customers and
    the items take turns, one to three of an item."""
    return Order(
        id=new_order_id(),
        customer=CUSTOMERS[number % len(CUSTOMERS)],
        item=ITEMS[number % len(ITEMS)],
        quantity=1 + number % 3,
    )


class Ends:
    """How the orders ended, as their callbacks and on_drop heard it, on the shop's io thread."""

    def __init__(self) -> None:
        self.done = 0
        self.given_up = 0
        self.why: collections.Counter[str] = collections.Counter()

    def ended(self, done: int) -> None:
        """An order's callback: 1 once a worker recorded it, or, unconfirmed,
        once it was written; 0 once the shop gave up on it."""
        if done:
            self.done += 1
        else:
            self.given_up += 1

    def dropped(self, message_id: int, reason: DropReason) -> None:
        """on_drop: why the shop gave up on an order, told ahead of its callback."""
        self.why[reason.name] += 1


def place(args: argparse.Namespace) -> None:
    """Place the orders, flush, and say how they ended."""
    ends = Ends()
    shop = Shop(
        parse_addresses(args.addresses),
        one_by_one=args.one_by_one,
        confirm=not args.unconfirmed,
        timeout=args.timeout,
        on_drop=ends.dropped,
    )
    with closing(Ledger(args.ledger)) as ledger, shop:
        before = ledger.count()
        started = time.monotonic()
        for number in range(args.orders):
            order = make_order(number)
            if number < args.empty:
                order.quantity = 0  # for nothing: a worker raises on it
            shop.place(order, ends.ended)
        flushed = shop.flush(args.timeout + SLACK)
        took = time.monotonic() - started
        held = ledger.count() - before  # read the moment flush_all() returned
        sent_again = shop.sent_again
    say_how_it_went(args, ends, sent_again, flushed, took, held)


def say_how_it_went(
    args: argparse.Namespace, ends: Ends, sent_again: int, flushed: bool, took: float, held: int
) -> None:
    """What place prints: how the orders ended, how many the shop sent
    again, if any, and what flush_all() said and the ledger held then."""
    how = "one by one" if args.one_by_one else "in batches"
    how = f"sent {how}, unconfirmed" if args.unconfirmed else f"confirmed {how}"
    done = f"{ends.done:,} {'written' if args.unconfirmed else 'confirmed'}" if ends.done else "none confirmed"
    given_up = f"{ends.given_up:,} given up on" if ends.given_up else "none given up on"
    if ends.why:
        given_up += f" ({', '.join(f'{reason} {count:,}' for reason, count in sorted(ends.why.items()))})"
    print(f"{args.orders:,} orders, {how}: {done}, {given_up}")
    if sent_again:
        print(f"the shop sent {sent_again:,} again, nothing having confirmed them")
    print(f"flush_all() returned {flushed} after {took:.2f} s, when the ledger held {held:,} of them")


def latency(args: argparse.Namespace) -> None:
    """Place the orders one at a time, each once the one before was
    confirmed, and say how long the confirmations took."""
    took: list[float] = []
    heard: queue.SimpleQueue[tuple[int, float]] = queue.SimpleQueue()
    with Shop(parse_addresses(args.addresses), one_by_one=args.one_by_one) as shop:
        for number in range(args.orders):
            sent = time.perf_counter()
            # The time is taken in the callback, on the io thread, as the confirmation comes.
            shop.place(make_order(number), lambda done: heard.put((done, time.perf_counter())))
            done, at = heard.get(timeout=TIMEOUT + SLACK)
            if not done:
                raise SystemExit(f"order {number} was given up on")
            took.append(at - sent)
    how = "one by one" if args.one_by_one else f"in batches with a delay of {MAX_DELAY * 1000:g} ms"
    p99 = statistics.quantiles(took, n=100)[98] if len(took) > 1 else took[0]
    print(
        f"{args.orders:,} orders, one at a time, confirmed {how}: confirmed {statistics.median(took) * 1000:.2f} ms"
        f" after the send at the median, {p99 * 1000:.2f} ms at the 99th percentile"
    )


def show_ledger(args: argparse.Namespace) -> None:
    """How many orders the ledger holds, by worker, and the repeats it skipped."""
    with closing(Ledger(args.ledger)) as ledger:
        by_worker = ledger.by_worker()
        repeats = ledger.repeats()
    workers = ", ".join(f"{worker} {orders:,}" for worker, orders in by_worker.items())
    print(f"{sum(by_worker.values()):,} orders{': ' + workers if workers else ''}")
    if not repeats:
        print("no repeats")
        return
    times = sum(count for _, count in repeats.values())
    whose = " and ".join(
        f"{orders:,} order{'s' if orders > 1 else ''} {worker} recorded" for worker, (orders, _) in repeats.items()
    )
    print(f"{times:,} repeat{'s' if times > 1 else ''} skipped, of {whose}")


def main() -> None:
    """One command."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    placing = commands.add_parser("place", help="place orders as fast as they go and say how they ended")
    placing.add_argument("addresses", help="host:port of every multiplexer, comma-separated")
    placing.add_argument("--orders", type=int, default=1000, help="how many")
    placing.add_argument("--one-by-one", action="store_true", help="each order at once, alone, not in batches")
    placing.add_argument("--unconfirmed", action="store_true", help="as plain events, for the contrast")
    placing.add_argument("--timeout", type=float, default=TIMEOUT, help=f"seconds a batch has, {TIMEOUT:g} by default")
    placing.add_argument(
        "--empty", type=int, default=0, help="the first N orders for nothing, which a worker raises on"
    )
    placing.add_argument("--ledger", default="ledger.db", help="the ledger's file, counted before and after")
    timing = commands.add_parser("latency", help="place orders one at a time and time their confirmations")
    timing.add_argument("addresses", help="host:port of every multiplexer, comma-separated")
    timing.add_argument("--orders", type=int, default=1000, help="how many")
    timing.add_argument("--one-by-one", action="store_true", help="each order alone, not in batches")
    reading = commands.add_parser("ledger", help="what the ledger holds")
    reading.add_argument("--ledger", default="ledger.db", help="the ledger's file")
    args = parser.parse_args()
    {"place": place, "latency": latency, "ledger": show_ledger}[args.command](args)


if __name__ == "__main__":
    main()
