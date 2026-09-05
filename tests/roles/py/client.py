"""Client role: sends requests and reports the answers, in four modes.

  default          a synchronous Client per worker (--parallel N), one query
                   at a time;
  --threaded       a ThreadedClient per worker, one query at a time;
  --threaded --async N   up to N queries in flight through callbacks;
  --workers N      N threads sharing one ThreadedClient, each with its own
                   reply queue, the pattern of examples/echo/workers.py.

Every mode produces the same events: connected, response, error, done.
--query TYPE:payload (repeatable) is the list each worker sends --count
times; {worker}, {round} and {i} in a payload are filled in. --timeout,
--payload-size, --sleep-before and --sleep-between shape the run.
tests/README.md is the reference for options and events.
"""

import argparse
import queue
import threading
import time
from typing import Any

from tests.roles.py import common
from tests.roles.py.common import clients, emit

Query = tuple[int, bytes]  # (message type, payload)

answered_lock = threading.Lock()
answered = 0  # queries answered so far, across workers, for --memory-every


def count_answered(args: argparse.Namespace) -> None:
    """One more query answered; emit a memory event every --memory-every."""
    global answered
    if not args.memory_every:
        return
    with answered_lock:
        answered += 1
        count = answered
    if count % args.memory_every == 0:
        common.memory_event(count)


def expand(payload: bytes, worker: int, round_: int, index: int, args: argparse.Namespace) -> bytes:
    """The payload actually sent: --payload-size bytes of 'x' if asked for,
    else the template with {worker}, {round} and {i} filled in."""
    if args.payload_size:
        payload = b"x" * args.payload_size
    return (
        payload.replace(b"{worker}", str(worker).encode())
        .replace(b"{round}", str(round_).encode())
        .replace(b"{i}", str(index).encode())
    )


def report_result(worker: int, round_: int, index: int, type_: int, started: float, result: Any) -> bool:
    """Emit a response event for a reply, or an error event for an exception;
    returns True for a response."""
    fields = dict(
        worker=worker, round=round_, index=index, query_type=type_, ms=round((time.time() - started) * 1000, 1)
    )
    if isinstance(result, Exception):
        emit("error", kind=common.error_name(result), **fields)
        return False
    emit(
        "response",
        type=result.type,
        from_=result.from_,
        references=result.references,
        **common.payload_summary(result.message),
        **fields,
    )
    return True


def report_done(worker: int, responses: int, errors: int, client: Any) -> None:
    """Emit the done event that closes a worker's run."""
    emit(
        "done",
        worker=worker,
        responses=responses,
        errors=errors,
        connections=client.connections_count(),
        instance_id=client.instance_id,
    )


def run_sequential(worker: int, args: argparse.Namespace, queries: list[Query], client: Any) -> None:
    """One query at a time with the blocking query(), on any client."""
    responses = errors = 0
    for round_ in range(args.count):
        for index, (type_, payload) in enumerate(queries):
            if args.sleep_between and (round_ or index):
                time.sleep(args.sleep_between)
            started = time.time()
            try:
                result = client.query(expand(payload, worker, round_, index, args), type=type_, timeout=args.timeout)
            except Exception as error:  # report every failure kind by name
                result = error
            if report_result(worker, round_, index, type_, started, result):
                responses += 1
            else:
                errors += 1
            count_answered(args)
    report_done(worker, responses, errors, client)


def run_async(worker: int, args: argparse.Namespace, queries: list[Query], client: Any) -> None:
    """Up to args.async_ queries in flight at once through query with a callback; the
    callbacks report from the client's io thread."""
    lock = threading.Condition()
    state = {"in_flight": 0, "responses": 0, "errors": 0}

    def on_result(round_: int, index: int, type_: int, started: float, result: Any) -> None:
        """The callback: report, then let the issuing loop go on."""
        key = "responses" if report_result(worker, round_, index, type_, started, result) else "errors"
        count_answered(args)
        with lock:
            state[key] += 1
            state["in_flight"] -= 1
            lock.notify_all()

    for round_ in range(args.count):
        for index, (type_, payload) in enumerate(queries):
            if args.sleep_between and (round_ or index):
                time.sleep(args.sleep_between)
            with lock:
                while state["in_flight"] >= args.async_:
                    lock.wait()
                state["in_flight"] += 1
            started = time.time()
            client.query(
                expand(payload, worker, round_, index, args),
                type=type_,
                callback=lambda result, r=round_, i=index, t=type_, s=started: on_result(r, i, t, s, result),
                timeout=args.timeout,
            )
    with lock:
        while state["in_flight"]:
            lock.wait()
    report_done(worker, state["responses"], state["errors"], client)


def run_worker(worker: int, args: argparse.Namespace, queries: list[Query]) -> None:
    """One worker of --parallel: its own client, then the sequential or the
    async mode."""
    if args.threaded:
        client = common.threaded_client.ThreadedClient(common.endpoints(args), type=args.type)
    else:
        client = clients.Client(common.endpoints(args), type=args.type)
    emit(
        "connected",
        worker=worker,
        instance_id=client.instance_id,
        connections=client.connections_count(),
        name=args.name,
        threaded=args.threaded,
    )
    if args.sleep_before:
        time.sleep(args.sleep_before)
    if args.threaded and args.async_ > 1:
        run_async(worker, args, queries, client)
    else:
        run_sequential(worker, args, queries, client)
    client.shutdown()


def run_workers(args: argparse.Namespace, queries: list[Query]) -> None:
    """--workers N: N threads share one ThreadedClient. Each thread issues its
    queries with query with a callback, does its own work (sleep_between) meanwhile,
    and takes the replies from its own queue, which the io thread fills."""
    client = common.threaded_client.ThreadedClient(common.endpoints(args), type=args.type)
    emit(
        "connected",
        instance_id=client.instance_id,
        connections=client.connections_count(),
        name=args.name,
        threaded=True,
        workers=args.workers,
    )

    def worker_thread(worker: int) -> None:
        """One worker: issue, work, drain the inbox, repeat; then wait for the rest."""
        inbox: queue.Queue = queue.Queue()  # replies land here, from the io thread
        pending = responses = errors = 0

        def take(item: tuple) -> bool:
            """Report one reply from the inbox; True for a response."""
            round_, index, type_, started, result = item
            replied = report_result(worker, round_, index, type_, started, result)
            count_answered(args)
            return replied

        for round_ in range(args.count):
            for index, (type_, payload) in enumerate(queries):
                started = time.time()
                client.query(
                    expand(payload, worker, round_, index, args),
                    type=type_,
                    callback=lambda result, r=round_, i=index, t=type_, s=started: inbox.put((r, i, t, s, result)),
                    timeout=args.timeout,
                )
                pending += 1
                if args.sleep_between:
                    time.sleep(args.sleep_between)  # the worker's own work
                while pending:
                    try:
                        item = inbox.get_nowait()
                    except queue.Empty:
                        break
                    pending -= 1
                    if take(item):
                        responses += 1
                    else:
                        errors += 1
        while pending:
            pending -= 1
            if take(inbox.get()):
                responses += 1
            else:
                errors += 1
        report_done(worker, responses, errors, client)

    threads = [threading.Thread(target=worker_thread, args=(worker,)) for worker in range(args.workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    client.shutdown()


def main() -> None:
    """Parse the options and run the mode they select."""
    parser = common.parser("multiplexer client role")
    parser.add_argument("--query", action="append", help="TYPE:payload, repeatable, sent in order")
    parser.add_argument("--count", type=int, default=1, help="repeat the query list N times")
    parser.add_argument("--parallel", type=int, default=1, help="workers, each with its own client")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--payload-size", type=int, default=0, help="replace payloads with N bytes")
    parser.add_argument("--sleep-before", type=float, default=0.0, help="idle after connecting")
    parser.add_argument("--sleep-between", type=float, default=0.0, help="idle between queries")
    parser.add_argument("--threaded", action="store_true", help="use ThreadedClient (an io thread of its own)")
    parser.add_argument(
        "--async", dest="async_", type=int, default=0, help="with --threaded: up to N queries in flight at once"
    )
    parser.add_argument(
        "--workers", type=int, default=0, help="N threads sharing one ThreadedClient, each with its own reply queue"
    )
    parser.add_argument("--memory-every", type=int, default=0, help="emit a memory event every N answered queries")
    args = parser.parse_args()
    common.start_memory_tracing(args.memory_every)
    queries = common.typed_payloads(args.query)
    if args.workers:
        run_workers(args, queries)
        return
    threads = [threading.Thread(target=run_worker, args=(worker, args, queries)) for worker in range(args.parallel)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
