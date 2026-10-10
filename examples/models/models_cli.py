"""The command line the steps use: asks for predictions, one request after
another, and tells a worker to load or unload a model.

    python models_cli.py ADDRESSES predict MODEL [TEXT] [--repeat N | --seconds S] [--tally]
    python models_cli.py ADDRESSES load WORKER MODEL
    python models_cli.py ADDRESSES unload WORKER MODEL

ADDRESSES is host:port of every multiplexer, comma-separated. predict
asks N times, or again and again for S seconds, and prints each answer
and how long it took, or with --tally one line once all are in: how many
answers each worker gave, and how many requests failed. load and unload
print the worker's answer, which comes once every multiplexer has its new
filters. --timeout, before the command, is each stage's timeout."""

import argparse
import collections
import time

from multiplexer.endpoints import parse_endpoint
from multiplexer.mxclient import MultiplexerClientError
from multiplexer.threaded_client import BackendError, ThreadedClient

from multiplexer_constants import peers, types
from worker import model_key, worker_key


def predict(client: ThreadedClient, args: argparse.Namespace) -> None:
    """Ask `args.model` for predictions, one request after another:
    `args.repeat` of them, or as many as `args.seconds` take."""
    answered: collections.Counter[str] = collections.Counter()
    failed = 0
    deadline = time.monotonic() + args.seconds
    asked = 0
    while (time.monotonic() < deadline) if args.seconds else (asked < args.repeat):
        asked += 1
        started = time.monotonic()
        try:
            reply = client.query(args.text, type=types.PREDICT, key=model_key(args.model), timeout=args.timeout)
            answer = reply.message.decode()
            answered[answer.rsplit(" by ", 1)[1]] += 1
        except (MultiplexerClientError, BackendError) as error:
            answer = f"failed: {type(error).__name__}"
            failed += 1
        if not args.tally:
            print(f"{answer}, in {time.monotonic() - started:.1f} s", flush=True)
    if args.tally:
        workers = ", ".join(f"{worker} {count}" for worker, count in sorted(answered.items()))
        print(f"{answered.total()} answers, {workers or 'from nobody'}; {failed} failed", flush=True)


def control(client: ThreadedClient, args: argparse.Namespace) -> None:
    """Ask the worker `args.worker` to load or unload `args.model`."""
    started = time.monotonic()
    try:
        request = f"{args.command} {args.model}"
        reply = client.query(request, type=types.MODEL_CONTROL, key=worker_key(args.worker), timeout=args.timeout)
        answer = reply.message.decode()
    except (MultiplexerClientError, BackendError) as error:
        answer = f"failed: {type(error).__name__}"
    print(f"{answer}, in {time.monotonic() - started:.1f} s", flush=True)


def main() -> None:
    """One command."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("addresses", help="host:port of every multiplexer, comma-separated")
    parser.add_argument("--timeout", type=float, default=10.0, help="each stage's timeout, in seconds")
    commands = parser.add_subparsers(dest="command", required=True)
    asking = commands.add_parser("predict", help="ask a model for a prediction")
    asking.add_argument("model")
    asking.add_argument("text", nargs="?", default="hello")
    how_many = asking.add_mutually_exclusive_group()
    how_many.add_argument("--repeat", type=int, default=1, help="how many requests, one after another")
    how_many.add_argument("--seconds", type=float, default=0.0, help="ask again and again for this long instead")
    asking.add_argument("--tally", action="store_true", help="one line at the end instead of one per answer")
    for command in ("load", "unload"):
        changing = commands.add_parser(command, help=f"ask a worker to {command} a model")
        changing.add_argument("worker")
        changing.add_argument("model")
    args = parser.parse_args()
    addresses = [parse_endpoint(text) for text in args.addresses.split(",")]
    with ThreadedClient(addresses, type=peers.MODEL_CLIENT) as client:
        (predict if args.command == "predict" else control)(client, args)


if __name__ == "__main__":
    main()
