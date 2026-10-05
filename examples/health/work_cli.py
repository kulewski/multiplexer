"""The command line the steps use: asks the workers for work, one request
after another, and prints who answered and how long the answer took.

    python work_cli.py ADDRESSES [--seconds S] [--repeat N] [--timeout T]

ADDRESSES is host:port of every multiplexer, comma-separated. Each request
asks for S seconds of work; T is each stage's timeout, after which the
client searches for another worker and sends the request again there."""

import argparse
import time

from multiplexer.endpoints import parse_endpoint
from multiplexer.mxclient import MultiplexerClientError
from multiplexer.threaded_client import BackendError, ThreadedClient

from multiplexer_constants import peers, types


def main() -> None:
    """Ask for work N times, one request after another."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("addresses", help="host:port of every multiplexer, comma-separated")
    parser.add_argument("--seconds", type=float, default=0.1, help="how long each request's work takes")
    parser.add_argument("--repeat", type=int, default=1, help="how many requests, one after another")
    parser.add_argument("--timeout", type=float, default=10.0, help="each stage's timeout, in seconds")
    args = parser.parse_args()
    addresses = [parse_endpoint(text) for text in args.addresses.split(",")]
    with ThreadedClient(addresses, type=peers.WORK_CLIENT) as client:
        for _ in range(args.repeat):
            started = time.monotonic()
            try:
                reply = client.query(f"{args.seconds:g}", type=types.WORK, timeout=args.timeout)
                answer = reply.message.decode()
            except (MultiplexerClientError, BackendError) as error:
                answer = f"failed: {type(error).__name__}"
            print(f"{answer}, in {time.monotonic() - started:.1f} s", flush=True)


if __name__ == "__main__":
    main()
