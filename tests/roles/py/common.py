"""Shared pieces of the Python roles: argument parsing, Event lines, signals."""

import argparse
import signal
import sys
import threading
from typing import Any

from google.protobuf import text_format

from multiplexer import clients  # noqa: F401  (re-exported for roles)
from multiplexer import servers  # noqa: F401  (re-exported for roles)
from multiplexer import threaded_client  # noqa: F401  (re-exported for roles)
from multiplexer.testing import events_pb2
from multiplexer.mxclient import NotConnected, OperationFailed, OperationTimedOut  # noqa: F401

STOP = threading.Event()


def emit(event: str, **fields: Any) -> None:
    """Print one Event (multiplexer/testing/events.proto) as a line of protocol
    buffer text format on stdout, for the harness."""
    message = events_pb2.Event(event=event, **fields)
    sys.stdout.write(text_format.MessageToString(message, as_one_line=True) + "\n")
    sys.stdout.flush()


def parser(description: str) -> argparse.ArgumentParser:
    """An argument parser with the options every role takes: --mx, --type, --name."""
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
    """Short, JSON-safe description of a payload."""
    if len(data) <= 256:
        return {"payload": data.decode("utf-8", "replace"), "size": len(data)}
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
