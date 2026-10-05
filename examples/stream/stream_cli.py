"""Ask from the command line and watch the answer arrive, for the
walkthrough's steps and the numbers: which multiplexer the answer goes
through, the tokens as they come, then how many, how fast, from which
generator, and what a dead connection cost.

    python stream_cli.py ADDRESSES "a prompt" [--tokens N] [--at-once N] [--pinned]

ADDRESSES is host:port of every multiplexer, comma-separated. --at-once
asks the same prompt N times at once and reports each answer's line, a
failed one as `failed:` and the error's name; --pinned ends an answer
with NotConnected when its multiplexer dies rather than going on through
the other."""

import argparse
import asyncio
import sys

from multiplexer.aio import AsyncClient

from multiplexer_constants import peers
from mxstream import Streams, parse_addresses


async def ask(streams: Streams, prompt: str, tokens: int, pinned: bool, show: bool) -> str:
    """One answer, printed as it comes when `show`; its summary line."""
    async with streams.open(prompt, tokens, timeout=120, pinned=pinned) as stream:
        async for text in stream:
            if show:
                if stream.received == 1:
                    endpoint = stream.lane.connection.endpoint
                    print(f"via {endpoint[0]}:{endpoint[1]}", flush=True)
                print(text, end=" ", flush=True)
    if show:
        print(flush=True)
    summary = stream.summary()
    line = (
        f"{summary['tokens']} tokens in {summary['seconds']} s, {summary['tokens_per_second']} tokens/s,"
        f" by {summary['worker']}"
    )
    if summary["reattached"]:
        line += f"; asked again through another multiplexer {summary['reattached']} time(s)"
    if summary["gaps"]:
        line += f"; {summary['gaps']} gap(s) asked for"
    if summary["resent"]:
        line += f"; {summary['resent']} tokens sent again"
    return line


async def main_async(args: argparse.Namespace) -> int:
    """The client, the answers, the lines."""
    client = AsyncClient(parse_addresses(args.addresses), type=peers.STREAM_CLIENT)
    streams = Streams(client)
    try:
        if args.at_once > 1:
            results = await asyncio.gather(
                *(ask(streams, args.prompt, args.tokens, args.pinned, False) for _ in range(args.at_once)),
                return_exceptions=True,  # one answer's failure is not the others'
            )
            for result in results:
                print(f"failed: {type(result).__name__}" if isinstance(result, BaseException) else result)
            return 1 if any(isinstance(result, BaseException) for result in results) else 0
        print(await ask(streams, args.prompt, args.tokens, args.pinned, True))
    except Exception as error:  # no generator, a timeout, a pinned lane's multiplexer gone
        print(f"\nfailed: {type(error).__name__}")
        return 1
    finally:
        streams.close()
        await client.aclose()
    return 0


def main() -> int:
    """The command line: the multiplexers, the prompt, how many answers at once and how long."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("addresses", help="host:port of every multiplexer, comma-separated")
    parser.add_argument("prompt")
    parser.add_argument("--tokens", type=int, default=40, help="how long an answer")
    parser.add_argument("--at-once", type=int, default=1, help="the same prompt this many times at once")
    parser.add_argument("--pinned", action="store_true", help="the hard pin: fail when the answer's multiplexer dies")
    return asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
