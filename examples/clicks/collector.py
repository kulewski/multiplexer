"""A collector: a backend on `BaseMultiplexerServer` that stores the clicks
of the shards it takes, one line per click in a file per shard, counts
them per page, and answers CLICK_COUNTS with a shard's counts.

    python collector.py [ADDRESSES] --shards 0,1 [--dir DIR]

ADDRESSES is host:port of every multiplexer, comma-separated, default
127.0.0.1:1980. A click is keyed with its shard, shard.N, and the
collector's key filters for CLICK and CLICK_COUNTS name its shards, so a
multiplexer routes it the clicks of those shards and no other, a batch
of them whole; the library splits a batch back into its clicks, and
handle_message() sees each alone. Every shard needs a collector that
takes it: the clicks of a shard nobody takes are dropped. The files are
clicks-shard.N.log under DIR, appended to, a line `user page time_ns`
per click; the counts are of the clicks stored since the collector
started. SIGTERM makes it leave: the multiplexers route it nothing new,
it stores what was on its way, writes its files out and exits."""

import argparse
import collections
import os
import re
import signal
import sys
import time
from typing import TextIO

from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.mxclient import CLOSE_FLUSH_SECONDS
from multiplexer.servers import BaseMultiplexerServer

from clicks_pb2 import Click, Counts
from multiplexer_constants import peers, types
from source import SHARDS, parse_addresses

WRITE_EVERY = 1.0  # seconds between writes of the files' buffers to the files


def shard_name(shard: int) -> str:
    """The key of shard number `shard`, as the sources set it."""
    return f"shard.{shard}"


class Collector(BaseMultiplexerServer):
    """The files and the counts of its shards, and the handler for clicks
    and for questions about them; on the serve loop's one thread."""

    multiplexer_client_type = peers.CLICK_COLLECTOR

    def __init__(self, addresses: list[tuple[str, int]], shards: list[int], directory: str = "."):
        """The collector of `shards` for the multiplexers in `addresses`, its
        files opened under `directory`; connect() or serve_forever() connects."""
        super().__init__(addresses)
        self.shards = sorted(shards)
        self.files: dict[str, TextIO] = {
            shard_name(shard): open(os.path.join(directory, f"clicks-{shard_name(shard)}.log"), "a")
            for shard in self.shards
        }
        self.pages: dict[str, collections.Counter[str]] = {key: collections.Counter() for key in self.files}
        self.stored = 0
        self.leave = False  # set by SIGTERM; periodic_task() starts the drain
        self.ready_pending = False  # set by main(): "ready" is printed once a multiplexer welcomed the collector
        self._written_at = time.monotonic()
        # Before connecting, so that the welcome carries them: these shards'
        # keys and no other, for the clicks and for the questions.
        only = [re.escape(key) for key in self.files]
        self.set_key_filters({types.CLICK: only, types.CLICK_COUNTS: only})

    def handle_message(self, mxmsg: MultiplexerMessage) -> None:
        """A click stored and counted, or a shard's counts answered."""
        out = self.files.get(mxmsg.key)
        if mxmsg.type == types.CLICK and out is not None:
            click = Click.FromString(mxmsg.message)
            out.write(f"{click.user} {click.page} {click.time_ns}\n")
            self.pages[mxmsg.key][click.page] += 1
            self.stored += 1
            self.no_response()
        elif mxmsg.type == types.CLICK_COUNTS and out is not None:
            out.flush()  # the file holds every click the answer counts
            pages = self.pages[mxmsg.key]
            self.send_message(message=Counts(pages=pages, clicks=pages.total()), type=types.CLICK_COUNTS_ANSWER)
        elif mxmsg.type == types.CLICK_COUNTS:  # no key, or a shard sent here with `to`
            self.report_error(f"collector of shards {self.shards} has no shard {mxmsg.key!r}")
        else:  # a click without a key, which every filter lets through: no shard to store it in
            self.no_response()

    def periodic_task(self) -> None:
        """After every message and every poll: the files' buffers written
        out once a second, the drain started once SIGTERM came, and "ready"
        said once a multiplexer welcomed the collector."""
        now = time.monotonic()
        if now - self._written_at >= WRITE_EVERY:
            self._written_at = now
            for out in self.files.values():
                out.flush()
        if self.leave:
            self.start_draining()
        self.say_ready_once_connected()

    def say_ready_once_connected(self) -> None:
        """Print the "ready" main() asked for once a multiplexer has
        welcomed the collector, its filters with it: whoever runs the
        steps, and the test, wait for the line and then send."""
        if self.ready_pending and self.conn.connections_count() > 0:
            print(f"ready: shards {', '.join(map(str, self.shards))}", flush=True)
            self.ready_pending = False

    def close(self, timeout: float = CLOSE_FLUSH_SECONDS) -> None:
        """The connections closed, then the files written out and closed."""
        super().close(timeout)
        for out in self.files.values():
            if not out.closed:
                out.close()


def main() -> None:
    """Serve until SIGTERM, then leave."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "addresses", nargs="?", default="127.0.0.1:1980", help="host:port of every multiplexer, comma-separated"
    )
    parser.add_argument("--shards", required=True, help=f"the shards it takes, comma-separated, of 0 to {SHARDS - 1}")
    parser.add_argument("--dir", default=".", help="where the files go, one per shard, appended to")
    args = parser.parse_args()
    shards = sorted({int(text) for text in args.shards.split(",")})
    if any(shard not in range(SHARDS) for shard in shards):
        parser.error(f"a shard is a number from 0 to {SHARDS - 1}")
    collector = Collector(parse_addresses(args.addresses), shards, args.dir)

    def leave(signum: int, frame: object) -> None:
        """SIGTERM: a flag the serve loop reads, between two messages."""
        collector.leave = True

    # The handler runs when the serve loop's wait comes back to Python, at
    # the latest once its poll is over.
    signal.signal(signal.SIGTERM, leave)
    collector.ready_pending = True
    collector.connect()  # waits for the handshakes, so that "ready" can come at once
    collector.say_ready_once_connected()
    if collector.ready_pending:
        print("no multiplexer reachable yet; ready once one is", file=sys.stderr, flush=True)
    collector.serve_forever(poll=0.5, drain_seconds=5)
    print(f"left after storing {collector.stored} clicks", flush=True)


if __name__ == "__main__":
    main()
