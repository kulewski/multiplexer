"""The cache from the command line, for the walkthrough's steps.

    python cache_cli.py ADDRESSES get KEY [--repeat N]
    python cache_cli.py ADDRESSES set KEY VALUE [--ttl SECONDS]
    python cache_cli.py ADDRESSES add KEY VALUE
    python cache_cli.py ADDRESSES delete KEY
    python cache_cli.py ADDRESSES clear
    python cache_cli.py ADDRESSES stats
    python cache_cli.py ADDRESSES fill N
    python cache_cli.py ADDRESSES loop [--pairs N | --seconds S] [--writes-through all|one]

ADDRESSES is host:port of every multiplexer, comma-separated. `get` says
which replica answered, so `--repeat` shows the round robin. `fill` sets N
keys, for the duplicate-suppression step. `loop` writes a value and reads
it back, over and over, and counts the reads that came back stale, missing
or unanswered; with `--writes-through one` the writes and the reads take
the two connections in turn, so every read races the write before it
through the other multiplexer, which is the ordering step of the
walkthrough."""

import argparse
import sys
import time

from mxcache.client import Cache, parse_addresses


def show_stats(cache: Cache) -> None:
    """Every replica's numbers, one line each."""
    replies = cache.stats()
    if not replies:
        print("no replica answered")
    for stats in replies:
        print(
            f"replica {stats.replica} (instance {stats.instance_id}): {stats.entries} entries,"
            f" {stats.writes} writes, {stats.reads} reads, {stats.hits} hits"
        )


def loop(cache: Cache, pairs: int, seconds: float, key: str) -> int:
    """Write-then-read pairs; the summary says what the reads saw. Returns the stale count."""
    stale = misses = 0
    started = time.monotonic()
    count = 0
    while (count < pairs) if seconds == 0 else (time.monotonic() - started < seconds):
        value = str(count).encode()
        cache.set(key, value)
        got = cache.get(key)
        if got is None:
            misses += 1
        elif got != value:
            stale += 1
        count += 1
    elapsed = time.monotonic() - started
    through = "every multiplexer" if cache.writes_through == Cache.ALL else "one multiplexer at a time"
    print(
        f"{count} write-then-read pairs in {elapsed:.1f} s, writes through {through}:"
        f" {stale} stale reads, {misses} misses, of which {cache.unavailable} with no replica reachable"
    )
    return stale


def main() -> int:
    """One command against the cache."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("addresses", help="host:port of every multiplexer, comma-separated")
    parser.add_argument("--writes-through", choices=["all", "one"], default="all")
    commands = parser.add_subparsers(dest="command", required=True)
    get = commands.add_parser("get")
    get.add_argument("key")
    get.add_argument("--repeat", type=int, default=1)
    set_ = commands.add_parser("set")
    set_.add_argument("key")
    set_.add_argument("value")
    set_.add_argument("--ttl", type=float, default=0, help="seconds until it expires; 0 for never")
    add = commands.add_parser("add")
    add.add_argument("key")
    add.add_argument("value")
    delete = commands.add_parser("delete")
    delete.add_argument("key")
    commands.add_parser("clear")
    commands.add_parser("stats")
    fill = commands.add_parser("fill")
    fill.add_argument("count", type=int)
    loop_ = commands.add_parser("loop")
    loop_.add_argument("--pairs", type=int, default=1000)
    loop_.add_argument("--seconds", type=float, default=0, help="run for this long instead of a count of pairs")
    loop_.add_argument("--key", default="loop")
    args = parser.parse_args()

    cache = Cache(
        parse_addresses(args.addresses), writes_through=Cache.ALL if args.writes_through == "all" else Cache.ONE
    )
    status = 0
    try:
        if args.command == "get":
            for _ in range(args.repeat):
                value = cache.lookup(args.key)
                if value is None:
                    print(f"{args.key}: miss, no replica reachable")
                elif value.found:
                    print(f"{args.key} = {value.value.decode(errors='replace')}  (replica {value.replica})")
                else:
                    print(f"{args.key}: miss  (replica {value.replica})")
        elif args.command == "set":
            cache.set(args.key, args.value.encode(), time.time() + args.ttl if args.ttl else 0.0)
            print(f"{args.key} set")
        elif args.command == "add":
            print(f"{args.key} {'added' if cache.add(args.key, args.value.encode()) else 'was there already'}")
        elif args.command == "delete":
            cache.delete(args.key)
            print(f"{args.key} deleted")
        elif args.command == "clear":
            cache.clear()
            print("cleared")
        elif args.command == "stats":
            show_stats(cache)
        elif args.command == "fill":
            for index in range(args.count):
                cache.set(f"key-{index}", f"value-{index}".encode())
            print(f"{args.count} keys set, one write each, sent through every multiplexer")
        elif args.command == "loop":
            status = 1 if loop(cache, args.pairs, args.seconds, args.key) else 0
        # close() writes what is still queued, but drops what arrives while it
        # closes: a write that reached no replica comes back as one delivery
        # error per multiplexer, a round trip later, and counts in
        # lost_writes only if it arrives before then.
        time.sleep(0.05)
    finally:
        cache.close()
    if cache.lost_writes:
        print(f"{cache.lost_writes} write(s) reached no replica, as a multiplexer reported")
    return status


if __name__ == "__main__":
    sys.exit(main())
