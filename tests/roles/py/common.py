"""Shared pieces of the Python roles: argument parsing, Event lines, signals."""

import argparse
import os
import signal
import sys
import threading
from typing import Any

from google.protobuf import text_format

from multiplexer import clients as clients  # re-exported for roles
from multiplexer import servers as servers  # re-exported for roles
from multiplexer import threaded_client as threaded_client  # re-exported for roles
from multiplexer import threaded_server as threaded_server  # re-exported for roles
from multiplexer import events_pb2
from multiplexer.Multiplexer_pb2 import Routing
from multiplexer.mxclient import NotConnected as NotConnected  # re-exported for roles
from multiplexer.mxclient import OperationFailed as OperationFailed  # re-exported for roles
from multiplexer.mxclient import OperationTimedOut as OperationTimedOut  # re-exported for roles

STOP = threading.Event()


def drain_routing(flags: str) -> Routing:
    """--drain-routing's comma-separated flag names as a Routing."""
    kept = {flag for flag in flags.split(",") if flag}
    unknown = kept - {"any", "all", "last_resort"}
    if unknown:
        raise ValueError("unknown routing flags: %s" % ", ".join(sorted(unknown)))
    return Routing(any="any" in kept, all="all" in kept, last_resort="last_resort" in kept)


_emit_lock = threading.Lock()  # a threaded backend reports from its workers


def emit(event: str, **fields: Any) -> None:
    """Print one Event (multiplexer/events.proto) as a line of protocol
    buffer text format on stdout, for the harness."""
    message = events_pb2.Event(event=event, **fields)
    with _emit_lock:
        sys.stdout.write(text_format.MessageToString(message, as_one_line=True) + "\n")
        sys.stdout.flush()


def end_with_the_harness() -> None:
    """Under the test harness, which hands every process it starts the read
    end of a pipe only it writes to, named by MX_TEST_PARENT_FD: end as
    SIGTERM ends this role once the pipe reaches its end, which is when the
    test process is gone, however it died, as lib/program.h has the C++
    processes do. The variable is cleared and the descriptor kept from the
    role's own children, which could take an unrelated descriptor of that
    number for it."""
    named = os.environ.pop("MX_TEST_PARENT_FD", None)
    if named is None:
        return
    descriptor = int(named)
    try:
        os.set_inheritable(descriptor, False)
    except OSError:
        return  # not open: nothing to watch

    def watch() -> None:
        """Reads the pipe until its end, then SIGTERM to this process."""
        while True:
            try:
                if not os.read(descriptor, 1):
                    break
            except InterruptedError:
                continue
            except OSError:
                return
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=watch, name="end-with-the-harness", daemon=True).start()


def parser(description: str) -> argparse.ArgumentParser:
    """An argument parser with the options every role takes: --mx, --type,
    --name; every role makes one first, which is where it starts ending
    with the harness (end_with_the_harness)."""
    end_with_the_harness()
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--mx", action="append", required=True, help="host:port, repeatable")
    p.add_argument("--type", type=int, required=True, help="peer type id")
    p.add_argument("--name", default="", help="label used in events")
    return p


def endpoints(args: argparse.Namespace) -> list[tuple[str, int]]:
    """The --mx addresses as (host, port) pairs."""
    result = []
    for address in args.mx:
        host, port = address.rsplit(":", 1)
        result.append((host, int(port)))
    return result


def kv_ints(items: list[str] | None) -> dict[int, int]:
    """['201=203', ...] -> {201: 203}"""
    out = {}
    for item in items or []:
        k, v = item.split("=", 1)
        out[int(k)] = int(v)
    return out


def typed_payloads(items: list[str] | None) -> list[tuple[int, bytes]]:
    """['201:hello', ...] -> [(201, b'hello'), ...]"""
    out = []
    for item in items or []:
        t, payload = item.split(":", 1)
        out.append((int(t), payload.encode("utf-8")))
    return out


def payload_summary(data: bytes) -> dict[str, Any]:
    """Short description of a payload: itself when it is short and UTF-8,
    else its size and SHA-256, as the C++ roles give it, but for the hash,
    which they leave out: bytes changed to make them text would read as
    another payload."""
    if len(data) <= 256:
        try:
            return {"payload": data.decode("utf-8"), "size": len(data)}
        except UnicodeDecodeError:
            pass
    import hashlib

    return {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def stop_on_sigterm() -> None:
    """Make SIGTERM and SIGINT set STOP instead of killing the process."""
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())


def memory_event(after: int) -> None:
    """Emit a "memory" event: the C heap in use, the interpreter's own
    allocations (tracemalloc, if started) and its object count."""
    import gc
    import tracemalloc

    from multiplexer import _native

    gc.collect()  # cycles are garbage, not growth; count what is really retained
    py_bytes = tracemalloc.get_traced_memory()[0] if tracemalloc.is_tracing() else 0
    emit(
        "memory", after=after, heap_bytes=_native.heap_in_use_bytes(), py_bytes=py_bytes, objects=len(gc.get_objects())
    )


def start_memory_tracing(every: int) -> None:
    """Start tracemalloc when memory events are asked for."""
    if every:
        import tracemalloc

        tracemalloc.start()


def error_name(exc: BaseException) -> str:
    """The exception's class name, the way the C++ roles report it too."""
    return type(exc).__name__
