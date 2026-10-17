"""The command line the steps use: clicks sent from several processes as
fast as they go, the counts asked of every shard, and what the CLICK
messages on the wire carried.

    python clicks_cli.py ADDRESSES send [--clicks N] [--sources S] [--users U] [--pages P]
                                        [--no-batching | --max-delay SECONDS]
    python clicks_cli.py ADDRESSES counts
    python clicks_cli.py ADDRESSES watch [--clicks N] [--seconds S]

ADDRESSES is host:port of every multiplexer, comma-separated. send starts S
processes, each with a ClickSource of its own, which send N clicks between
them as fast as they go, by U users on P pages; then it waits until the
collectors have stored them, and prints how long that took, clicks a
second, how many were lost, and the CPU the multiplexers on this
machine's loopback used meanwhile, found by their ports in /proc. counts
asks every shard and prints the clicks per page and in all. watch taps
every multiplexer, which must run with --allow-tap, until it has seen N
clicks go by or S seconds passed, and prints how the CLICK messages
carried them: alone, or in batches of how many."""

import argparse
import multiprocessing
import os
import statistics
import time

from multiplexer import recording
from multiplexer.clients import SyncClient
from multiplexer.endpoints import Endpoint
from multiplexer.mxclient import MultiplexerClientError, OperationTimedOut
from multiplexer.Multiplexer_pb2 import MessageBatch
from multiplexer.Recording_pb2 import RECORDING_CONTROLLER, RecordingControl, RoutedMessage
from multiplexer.threaded_client import BackendError, ThreadedClient

from clicks_pb2 import Counts
from multiplexer_constants import peers, types
from source import MAX_DELAY, SHARDS, ClickSource, parse_addresses

ASK_EVERY = 0.01  # seconds between two looks at the counts while the clicks are on their way
NOTHING_MORE = float(os.environ.get("NOTHING_MORE", "5"))  # seconds with no click stored, then the rest are lost
LOOPBACK = ("127.0.0.1", "::1", "localhost")


def send_clicks(addresses: str, index: int, clicks: int, users: int, pages: int, delay: float | None, barrier, report):
    """One source, in a process of its own: connect, wait for the others,
    send `clicks` clicks as fast as Python goes, flush, and report whether
    every one was written and how many were given up on."""
    names = [f"user{number}" for number in range(users)]
    paths = [f"/page/{number}" for number in range(pages)]
    with ClickSource(parse_addresses(addresses), batching=delay is not None, max_delay=delay or 0.0) as source:
        barrier.wait(30)
        for number in range(clicks):
            source.click(names[(index * clicks + number) % users], paths[number % pages])
        report.put((source.flush(60), source.dropped))


def shard_counts(client: ThreadedClient, shard: int) -> Counts:
    """What the collector of `shard` has stored; a shard nobody takes fails the query at once."""
    return Counts.FromString(client.query(b"", type=types.CLICK_COUNTS, key=f"shard.{shard}").message)


def stored(client: ThreadedClient) -> int:
    """The clicks the shards' collectors have stored since they started; a
    shard nobody takes counts none."""
    total = 0
    for shard in range(SHARDS):
        try:
            total += shard_counts(client, shard).clicks
        except (MultiplexerClientError, BackendError):
            pass
    return total


def wait_until_stored(client: ThreadedClient, before: int, clicks: int) -> tuple[int, float]:
    """How many of `clicks` the collectors stored beyond `before`, and when
    the last of those was counted. The collectors say nothing when they are
    done, so the counts are asked again and again; a run that stored
    nothing more for NOTHING_MORE seconds lost the rest."""
    count, changed = 0, time.monotonic()
    while count < clicks and time.monotonic() - changed < NOTHING_MORE:
        now = stored(client) - before
        if now != count:
            count, changed = now, time.monotonic()
        else:
            time.sleep(ASK_EVERY)
    return count, changed


def multiplexer_pid(endpoint: Endpoint) -> int | None:
    """The process of this user's listening on the TCP port of `endpoint`, a
    multiplexer on this machine's loopback: the socket from /proc/net/tcp,
    then the process that holds it, from /proc/*/fd. None for another
    host, or when /proc says nothing."""
    host, port = endpoint
    if host not in LOOPBACK:
        return None
    sockets = set()
    for table in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(table) as rows:
                for row in list(rows)[1:]:
                    fields = row.split()
                    if fields[3] == "0A" and int(fields[1].rsplit(":", 1)[1], 16) == port:  # 0A: LISTEN
                        sockets.add(f"socket:[{fields[9]}]")
        except OSError:
            return None
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            if any(os.readlink(f"/proc/{pid}/fd/{fd}") in sockets for fd in os.listdir(f"/proc/{pid}/fd")):
                return int(pid)
        except OSError:  # another user's process, or one gone
            continue
    return None


def cpu_seconds(pid: int) -> float:
    """The CPU a process has used so far, every thread of it, in seconds:
    the first field of each thread's /proc/PID/task/TID/schedstat, in
    nanoseconds, where /proc/PID/stat counts in hundredths."""
    total = 0
    for thread in os.listdir(f"/proc/{pid}/task"):
        with open(f"/proc/{pid}/task/{thread}/schedstat") as schedstat:
            total += int(schedstat.read().split()[0])
    return total / 1e9


def send(args: argparse.Namespace) -> None:
    """Send the clicks from args.sources processes and say how it went."""
    delay = None if args.no_batching else args.max_delay
    addresses = parse_addresses(args.addresses)
    context = multiprocessing.get_context("spawn")  # each source a fresh process: no client crosses a fork
    barrier = context.Barrier(args.sources + 1)
    report = context.Queue()
    shares = [args.clicks // args.sources + (index < args.clicks % args.sources) for index in range(args.sources)]
    sources = [
        context.Process(
            target=send_clicks, args=(args.addresses, index, share, args.users, args.pages, delay, barrier, report)
        )
        for index, share in enumerate(shares)
    ]
    for process in sources:
        process.start()
    multiplexers = [pid for pid in map(multiplexer_pid, addresses) if pid is not None]
    with ThreadedClient(addresses, type=peers.CLICK_SOURCE) as client:
        before = stored(client)
        cpu_before = [cpu_seconds(pid) for pid in multiplexers]
        barrier.wait(30)  # every source connected: the clock starts
        started = time.monotonic()
        reports = [report.get(timeout=300) for _ in sources]  # a bound that only detects a failure
        # Written is not stored: the clicks are on their way through the
        # multiplexers until the collectors' counts say so.
        count, ended = wait_until_stored(client, before, args.clicks)
        cpu = [cpu_seconds(pid) - spent for pid, spent in zip(multiplexers, cpu_before, strict=True)]
    for process in sources:
        process.join()
    say_how_it_went(args, delay, count, ended - started, reports, cpu)


def say_how_it_went(
    args: argparse.Namespace,
    delay: float | None,
    count: int,
    took: float,
    reports: list[tuple[bool, int]],
    cpu: list[float],
) -> None:
    """What send prints: the clicks stored, how fast and how many lost; the
    clicks the sources gave up on, if any; and the multiplexers' CPU."""
    if delay is None:
        how = "not batched"
    elif delay:
        how = f"batched with a delay of {delay:g} s"
    else:
        how = "batched with no delay"
    lost = args.clicks - count
    print(
        f"{args.clicks:,} clicks from {args.sources} source{'s' if args.sources > 1 else ''}, {how}:"
        f" {count:,} stored in {took:.2f} s, {count / took:,.0f} a second; {f'{lost:,} lost' if lost else 'none lost'}"
    )
    dropped = sum(dropped for _, dropped in reports)
    if dropped or not all(flushed for flushed, _ in reports):
        print(f"the sources gave up on {dropped:,} clicks, no multiplexer taking them in time")
    if cpu:
        each = " and ".join(f"{seconds:.3g} s" for seconds in cpu)
        print(f"the multiplexers' CPU: {each}, {sum(cpu) / args.clicks * 1e6:.3g} s a million clicks")


def counts(args: argparse.Namespace) -> None:
    """Every shard's counts, added up per page, and in all."""
    pages: dict[str, int] = {}
    clicks = 0
    with ThreadedClient(parse_addresses(args.addresses), type=peers.CLICK_SOURCE) as client:
        for shard in range(SHARDS):
            try:
                answer = shard_counts(client, shard)
            except (MultiplexerClientError, BackendError) as error:
                print(f"shard.{shard}: no answer, {type(error).__name__}")
                continue
            for page, count in answer.pages.items():
                pages[page] = pages.get(page, 0) + count
            clicks += answer.clicks
    for page in sorted(pages, key=lambda path: (len(path), path)):
        print(f"{page}: {pages[page]:,}")
    print(f"{clicks:,} clicks")


def watch(args: argparse.Namespace) -> None:
    """Tap every multiplexer and tell how the CLICK messages carried the clicks."""
    alone = 0
    batches: list[int] = []  # the clicks in each batch
    clicks = 0
    deadline = time.monotonic() + args.seconds
    client = SyncClient(parse_addresses(args.addresses), type=RECORDING_CONTROLLER)
    try:
        # Every record of what the multiplexers route, from now on, each
        # payload whole; the iteration ends with OperationTimedOut once
        # nothing has gone by for the whole of args.seconds.
        records = recording.tap(client, timeout=args.seconds)
        print("tapping", flush=True)
        try:
            for record in records:
                routed = record.routed
                if routed.type == types.CLICK and routed.disposition == RoutedMessage.DELIVERED:
                    if routed.batch:
                        batches.append(len(MessageBatch.FromString(routed.payload).message))
                        clicks += batches[-1]
                    else:
                        alone += 1
                        clicks += 1
                if clicks >= args.clicks or time.monotonic() >= deadline:
                    break
        except OperationTimedOut:
            pass
        # The tap's losses, from the status of a tap still on, then the untap.
        lost = sum(status.dropped for status in recording.status(client))
        recording.control(client, RecordingControl.UNTAP)
    finally:
        client.shutdown()
    print(f"{alone + len(batches):,} CLICK messages carried {clicks:,} clicks:", end=" ")
    if batches:
        print(
            f"{len(batches):,} batches of {min(batches)} to {max(batches)} clicks,"
            f" {statistics.median(batches):g} the median, and {alone:,} clicks alone"
        )
    else:
        print("every click alone")
    if lost:
        print(f"the tap fell behind, and the multiplexers dropped {lost:,} of its records")


def main() -> None:
    """One command."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("addresses", help="host:port of every multiplexer, comma-separated")
    commands = parser.add_subparsers(dest="command", required=True)
    sending = commands.add_parser("send", help="send clicks from several processes as fast as they go")
    sending.add_argument("--clicks", type=int, default=100_000, help="how many, from all the sources together")
    sending.add_argument("--sources", type=int, default=4, help="how many processes send them")
    sending.add_argument("--users", type=int, default=1000, help="how many users click")
    sending.add_argument("--pages", type=int, default=5, help="on how many pages")
    batching = sending.add_mutually_exclusive_group()
    batching.add_argument("--no-batching", action="store_true", help="every click alone")
    batching.add_argument(
        "--max-delay", type=float, default=MAX_DELAY, help=f"seconds a click waits for others, {MAX_DELAY} by default"
    )
    commands.add_parser("counts", help="every shard's counts, added up")
    watching = commands.add_parser("watch", help="how the CLICK messages carry the clicks, from a tap")
    watching.add_argument("--clicks", type=int, default=100_000, help="stop once this many went by")
    watching.add_argument("--seconds", type=float, default=60.0, help="or once this long has passed")
    args = parser.parse_args()
    {"send": send, "counts": counts, "watch": watch}[args.command](args)


if __name__ == "__main__":
    main()
