"""Asks the workers, from the command line: sends each image as an
INFER_REQUEST and prints the answer with who gave it and how long it took.

    python client.py ADDRESSES IMAGE... [--repeat N] [--concurrency N] [--timeout SECONDS]

The web app does the same from its views; this is the version to measure
with. --repeat sends the images N times over and prints a summary: the
answers per worker, the latency percentiles, and the requests that failed
by the exception they raised, which is how the walkthrough shows what a
killed worker or multiplexer costs; a line per request only without it.
--concurrency sends from that many threads at once through the one
client, as a web app's threads do."""

import argparse
import statistics
import sys
import threading
import time
from collections import Counter

from multiplexer.mxclient import MultiplexerClientError
from multiplexer.threaded_client import BackendError, ThreadedClient

from backend import parse_addresses
from inference_pb2 import InferRequest, InferResponse
from multiplexer_constants import peers, types


def classify(client: ThreadedClient, image: bytes, timeout: float = 10.0) -> InferResponse:
    """One request, its answer parsed; raises what query() raises."""
    reply = client.query(InferRequest(image=image), type=types.INFER_REQUEST, timeout=timeout)
    response = InferResponse()
    response.ParseFromString(reply.message)
    return response


def main() -> int:
    """Send the images, print the answers, and the summary when repeating."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("addresses", help="host:port of every multiplexer, comma-separated")
    parser.add_argument("images", nargs="+", help="image files, PNG or JPEG")
    parser.add_argument("--repeat", type=int, default=1, help="send the images this many times over")
    parser.add_argument("--concurrency", type=int, default=1, help="threads sending at once")
    parser.add_argument(
        "--timeout", type=float, default=10.0, help="seconds each stage of a query waits (docs/query.md)"
    )
    args = parser.parse_args()
    client = ThreadedClient(parse_addresses(args.addresses), type=peers.WEB)
    images = [(path, open(path, "rb").read()) for path in args.images]
    latencies: list[float] = []
    workers: Counter[str] = Counter()
    failures: Counter[str] = Counter()
    lock = threading.Lock()

    def ask(path: str, image: bytes) -> None:
        """One request, its outcome counted."""
        started = time.monotonic()
        try:
            response = classify(client, image, args.timeout)
        except (MultiplexerClientError, BackendError) as error:
            with lock:
                failures[type(error).__name__] += 1
            if args.repeat == 1:
                print(f"{path}: {type(error).__name__} after {time.monotonic() - started:.3f} s", flush=True)
            return
        elapsed = time.monotonic() - started
        with lock:
            latencies.append(elapsed)
            workers[response.worker] += 1
        if args.repeat == 1:
            print(
                f"{path}: {response.label}  p={response.probabilities[response.label]:.3f}"
                f"  worker={response.worker}  model {response.seconds * 1000:.1f} ms  round trip {elapsed * 1000:.1f} ms",
                flush=True,
            )

    def loop(mine: list[tuple[str, bytes]]) -> None:
        """This thread's share of the requests, one after another."""
        for path, image in mine:
            ask(path, image)

    started = time.monotonic()
    try:
        # The requests dealt out to the threads; every thread shares the one client.
        jobs = [(path, image) for _ in range(args.repeat) for path, image in images]
        threads = [
            threading.Thread(target=loop, args=(jobs[first :: args.concurrency],))
            for first in range(min(args.concurrency, len(jobs)))
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        client.shutdown()
    if args.repeat > 1:
        sent = args.repeat * len(images)
        print(f"{len(latencies)} of {sent} answered in {time.monotonic() - started:.1f} s", end="")
        if latencies:
            ordered = sorted(latencies)
            print(
                f"; round trip median {statistics.median(ordered) * 1000:.1f} ms,"
                f" p95 {ordered[int(len(ordered) * 0.95) - 1] * 1000:.1f} ms, max {ordered[-1] * 1000:.0f} ms",
                end="",
            )
        print()
        for worker, count in sorted(workers.items()):
            print(f"  {count:5d} by {worker}")
        for name, count in sorted(failures.items()):
            print(f"  {count:5d} failed with {name}")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
