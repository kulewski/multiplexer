"""Ships this process's binary log stream to the multiplexers.

enable_single_thread_log_streaming() forks an `mxcontrol streamlogs` child
reading from a pipe and points the C++ logging at the pipe's write end; the
child sends LOGS_STREAM messages to every address given. Re-armed after a
fork, since the child process would otherwise share the parent's streamer.
"""

import os
import socket

import multiplexer.mxlog

__all__ = ["enable_single_thread_log_streaming"]

logging_fd_set_from_pid = None
logging_fd = None


def _spawn_streamer(multiplexer_addresses: list[tuple[str, int]], mxcontrol: str | None = None) -> None:
    """Fork the `mxcontrol streamlogs` child and point our logging at the pipe to it."""
    global logging_fd_set_from_pid, logging_fd

    if logging_fd is not None:
        try:
            os.close(logging_fd)
        except Exception:
            pass

    if mxcontrol is None:
        mxcontrol = os.path.abspath(os.path.dirname(os.path.dirname(multiplexer.__file__)) + "/mxcontrol/mxcontrol")

    logging_fd_set_from_pid = os.getpid()

    # Create a pipe that will become a binary logging stream.
    (reading, writing) = os.pipe()

    # Spawn a log streamer for this stream.
    if os.fork():
        # parent
        os.close(reading)
        multiplexer.mxlog.set_logging_fd(writing, True)
        logging_fd = writing

    else:
        # child
        os.close(writing)
        if reading != 0:
            os.dup2(reading, 0)
            os.close(reading)

        command = (
            [mxcontrol, "streamlogs"]
            + [
                e
                for host, port in multiplexer_addresses
                for e in ["--multiplexer", "%s:%d" % (socket.gethostbyname(host), port)]
            ]
            + ["--chunksize", "16"]
        )
        os.execvp(command[0], command)


def enable_single_thread_log_streaming(
    multiplexer_addresses: list[tuple[str, int]], mxcontrol: str | None = None
) -> int | None:
    """Start streaming this process's log to the multiplexers, once per
    process (re-armed after a fork); returns the pipe's write end."""

    if logging_fd_set_from_pid is None or logging_fd_set_from_pid != os.getpid():
        _spawn_streamer(multiplexer_addresses, mxcontrol=mxcontrol)

    return logging_fd
