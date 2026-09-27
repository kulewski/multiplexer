"""Ships this process's binary log stream to the multiplexers.

enable_single_thread_log_streaming() forks an `mxcontrol streamlogs` child
reading from a pipe and points the C++ logging at the pipe's write end; the
child sends LOGS_STREAM messages to every address given. Re-armed after a
fork, since the child process would otherwise share the parent's streamer.
The mxcontrol is the one that came with the package unless one is named.
"""

import os
import socket

import multiplexer.mxlog
from multiplexer.mxcontrol import binary_path

__all__ = ["enable_single_thread_log_streaming"]

logging_fd_set_from_pid = None
logging_fd = None


def _spawn_streamer(multiplexer_addresses: list[tuple[str, int]], mxcontrol: str | None = None) -> None:
    """Fork the `mxcontrol streamlogs` child and point our logging at the pipe to it."""
    global logging_fd_set_from_pid, logging_fd

    if mxcontrol is None:
        mxcontrol = binary_path()  # FileNotFoundError here, in the caller, when there is none

    logging_fd_set_from_pid = os.getpid()

    # Create a pipe that will become a binary logging stream.
    (reading, writing) = os.pipe()

    # Spawn a log streamer for this stream.
    if os.fork():
        # parent: the stream set before, the one inherited across a fork say,
        # is the C++ logging's, which closes it as it sets this one; closed
        # here too, its number could go to the pipe above, which C++ would
        # then close
        os.close(reading)
        multiplexer.mxlog.set_logging_fd(writing, True)
        logging_fd = writing

    else:
        # child: becomes the streamer or exits, and never returns into the
        # caller's code, which would then run a second time in this process
        try:
            os.close(writing)
            if reading != 0:
                os.dup2(reading, 0)
                os.close(reading)
            else:
                # the caller's stdin was closed and the pipe took its place:
                # made close-on-exec, as os.pipe() makes it, the streamer
                # would start with no stdin and read nothing
                os.set_inheritable(0, True)

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
        finally:
            os._exit(127)


def enable_single_thread_log_streaming(
    multiplexer_addresses: list[tuple[str, int]], mxcontrol: str | None = None
) -> int | None:
    """Start streaming this process's log to the multiplexers, once per
    process (re-armed after a fork); returns the pipe's write end. Without
    `mxcontrol`, the one that came with the package, or FileNotFoundError
    when there is none."""

    if logging_fd_set_from_pid is None or logging_fd_set_from_pid != os.getpid():
        _spawn_streamer(multiplexer_addresses, mxcontrol=mxcontrol)

    return logging_fd
