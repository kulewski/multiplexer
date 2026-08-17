"""Echo backend in Python: answers every ECHO_REQUEST with the upper-cased payload.

Usage: backend_py [host:port] [drain-file]     (default 127.0.0.1:1980, /tmp/echo-backend-leave)

Creating the drain file asks the backend to leave the way a deployment's
preStop hook would: it declines backend searches, serves what still arrives
for five seconds, then exits. The library handles no signals, so a plain
kill ends the process at once, requests in hand included.
"""

import os
import sys

from multiplexer.servers import BaseMultiplexerServer
from multiplexer.multiplexer_constants import peers, types


class EchoBackend(BaseMultiplexerServer):
    """Answers every ECHO_REQUEST with the payload upper-cased."""

    def __init__(self, addresses: list[tuple[str, int]], drain_file: str) -> None:
        super().__init__(addresses, type=peers.ECHO_BACKEND)
        self.drain_file = drain_file

    def handle_message(self, mxmsg):
        """Reply to one request."""
        self.send_message(message=mxmsg.message.upper(), type=types.ECHO_RESPONSE)

    def periodic_task(self):
        """Start draining once the drain file exists; runs after every iteration."""
        if os.path.exists(self.drain_file):
            self.start_draining()


def main(argv: list[str]) -> None:
    """Connect to the multiplexer named on the command line and serve until asked to leave."""
    host, port = (argv[1] if len(argv) > 1 else "127.0.0.1:1980").rsplit(":", 1)
    drain_file = argv[2] if len(argv) > 2 else "/tmp/echo-backend-leave"
    backend = EchoBackend([(host, int(port))], drain_file)
    print("ready", flush=True)
    # Drain for five seconds once asked: searches are declined so no retried
    # request comes here, requests that still arrive are served, then the
    # process exits. A rolling restart costs nobody a timeout.
    backend.serve_forever(poll=0.5, drain_seconds=5)


if __name__ == "__main__":
    main(sys.argv)
