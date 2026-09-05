"""A backend of its own that plays the "backend" role for label_role.py: the
smallest program following the role contract in tests/README.md.

It takes --mx (repeatable), --type and --name, prints `connected` once it is
registered and `request` for every request, answers with the payload in
upper case as TEST_RESPONSE, and leaves on SIGTERM.
"""

import argparse
import signal
import sys

from google.protobuf import text_format

from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import events_pb2
from tests.testing_constants import types


def emit(event: str, **fields) -> None:
    """One Event line on stdout, in protocol buffer text format."""
    line = text_format.MessageToString(events_pb2.Event(event=event, **fields), as_one_line=True)
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


class Upper(BaseMultiplexerServer):
    """Replies to every request with its payload in upper case."""

    def handle_message(self, mxmsg):
        emit("request", type=mxmsg.type, id=mxmsg.id, from_=mxmsg.from_, size=len(mxmsg.message))
        self.send_message(message=mxmsg.message.upper(), type=types.TEST_RESPONSE, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mx", action="append", required=True)
    parser.add_argument("--type", type=int, required=True)
    parser.add_argument("--name", default="")
    args = parser.parse_args()
    addresses = [(host, int(port)) for host, port in (address.rsplit(":", 1) for address in args.mx)]
    backend = Upper(addresses, type=args.type)
    signal.signal(signal.SIGTERM, lambda *_: backend.stop())
    emit(
        "connected", instance_id=backend.conn.instance_id, connections=backend.conn.connections_count(), name=args.name
    )
    backend.serve_forever(poll=0.1)


if __name__ == "__main__":
    main()
