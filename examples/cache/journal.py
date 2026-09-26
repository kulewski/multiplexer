"""A journal of the cache's writes: a backend that receives every
CACHE_SET, CACHE_DELETE and CACHE_CLEAR, as the replicas do, and writes
one line of JSON per event, so that the cache's history can be read. It
joins a running cache: its peer type and a second destination on the
three events went into the rules file while the multiplexers, the
replicas and the web app ran, which the walkthrough's section 6 shows.

    python journal.py [ADDRESSES] [--file PATH]

ADDRESSES is host:port of every multiplexer, comma-separated, default
127.0.0.1:1980; the lines go to PATH, journal.log by default."""

import argparse
import json
import time
from typing import TextIO

from multiplexer.servers import BaseMultiplexerServer

from cache_pb2 import CacheEntry, CacheKey
from multiplexer_constants import peers, types
from mxcache.client import parse_addresses


class Journal(BaseMultiplexerServer):
    """One line per write, delete or clear: the event, when, and from whom."""

    multiplexer_client_type = peers.CACHE_JOURNAL

    def __init__(self, addresses: list[tuple[str, int]], out: TextIO):
        super().__init__(addresses)
        self.out = out
        self.lines = 0

    def handle_message(self, mxmsg) -> None:
        """An event written down; anything else ignored. Nothing is answered."""
        kind = mxmsg.type
        line: dict[str, str | int | float]
        if kind == types.CACHE_SET:
            entry = self.parse_message(CacheEntry)
            line = {"event": "set", "key": entry.key, "bytes": len(entry.value), "expires": entry.expires}
        elif kind == types.CACHE_DELETE:
            line = {"event": "delete", "key": self.parse_message(CacheKey).key}
        elif kind == types.CACHE_CLEAR:
            line = {"event": "clear"}
        else:
            self.no_response()
            return
        line["at"] = round(time.time(), 3)
        line["from"] = mxmsg.from_
        self.out.write(json.dumps(line) + "\n")
        self.out.flush()
        self.lines += 1
        self.no_response()


def main() -> None:
    """Write the journal until stopped."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "addresses", nargs="?", default="127.0.0.1:1980", help="host:port of every multiplexer, comma-separated"
    )
    parser.add_argument("--file", default="journal.log", help="where the lines go, appended")
    args = parser.parse_args()
    with open(args.file, "a") as out:
        journal = Journal(parse_addresses(args.addresses), out)
        print(f"ready: journal to {args.file}, instance {journal.conn.instance_id}", flush=True)
        journal.serve_forever(poll=0.5)
        print(f"left after {journal.lines} lines", flush=True)


if __name__ == "__main__":
    main()
