"""Echo backend in Python: answers every ECHO_REQUEST with the upper-cased payload.

Usage: backend_py [host:port] [drain-file]     (default 127.0.0.1:1980, /tmp/echo-backend-leave-PID)

Creating the drain file asks the backend to leave the way a deployment's
preStop hook would: for five seconds it drains as the last resort of its
type, so beside another backend the multiplexers route it nothing new,
and alone it keeps serving to the end. The file is removed once seen, so
a replacement given the same path does not leave at once; the default
path carries the pid, so that two backends on one host each have their
own. The library handles no signals, so a plain kill ends the process
at once, requests in hand included.
"""

import os
import sys
import time

from multiplexer.servers import BaseMultiplexerServer
from multiplexer.Multiplexer_pb2 import Routing
from multiplexer.multiplexer_constants import peers, types

DRAIN_CHECK_EVERY = 0.5  # seconds between looks for the drain file


class EchoBackend(BaseMultiplexerServer):
    """Answers every ECHO_REQUEST with the payload upper-cased."""

    def __init__(self, addresses: list[tuple[str, int]], drain_file: str) -> None:
        # The drain as the last resort of the type: alone, the backend keeps
        # serving through it; beside another, it gets nothing new.
        super().__init__(
            addresses, type=peers.ECHO_BACKEND, drain_routing=Routing(any=False, all=False, last_resort=True)
        )
        self.drain_file = drain_file
        self._drain_checked_at = 0.0

    def handle_message(self, mxmsg):
        """Reply to one request."""
        self.send_message(message=mxmsg.message.upper(), type=types.ECHO_RESPONSE)

    def periodic_task(self):
        """After every message and every poll: start draining once the drain file exists, looked for
        twice a second rather than on every message, and remove it."""
        now = time.monotonic()
        if now - self._drain_checked_at < DRAIN_CHECK_EVERY:
            return
        self._drain_checked_at = now
        if os.path.exists(self.drain_file):
            os.remove(self.drain_file)
            self.start_draining()


def main(argv: list[str]) -> None:
    """Connect to the multiplexer named on the command line and serve until asked to leave."""
    host, port = (argv[1] if len(argv) > 1 else "127.0.0.1:1980").rsplit(":", 1)
    drain_file = argv[2] if len(argv) > 2 else f"/tmp/echo-backend-leave-{os.getpid()}"
    backend = EchoBackend([(host, int(port))], drain_file)
    # The echo test reads "ready" and queries at once, so the line must mean
    # reachable: connect() first; serve_forever() would otherwise.
    backend.connect()
    print(f"drain file: {drain_file}", file=sys.stderr, flush=True)
    print("ready", flush=True)
    # Drain for five seconds once asked, as the last resort: while another
    # backend is there the multiplexers route nothing new here, and alone
    # this one keeps serving to the end. A rolling restart costs nobody a
    # request.
    backend.serve_forever(poll=0.5, drain_seconds=5)


if __name__ == "__main__":
    main(sys.argv)
