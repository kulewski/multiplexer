"""The chat server: a backend that answers each line and broadcasts the
ones that start with "shout " to every gateway.

    bazel run //:backend -- 127.0.0.1:1980
"""

import sys

from multiplexer.endpoints import parse_endpoint
from multiplexer.multiplexer_constants import peers, types
from multiplexer.servers import BaseMultiplexerServer


class ChatServer(BaseMultiplexerServer):
    """Upper-cases what it is asked; a "shout" goes to everyone as well."""

    def handle_message(self, mxmsg):
        """The line upper-cased to its sender; after "shout ", the rest to every gateway first."""
        line = mxmsg.message.decode(errors="replace")
        if line.startswith("shout "):
            self.send_message(message=line[6:].upper().encode(), type=types.CHAT_BROADCAST, to=0, references=0)
        self.send_message(message=line.upper().encode(), type=types.CHAT_RESPONSE)


def main() -> None:
    """The multiplexers' addresses from the command line: serve until killed."""
    addresses = [parse_endpoint(address) for address in sys.argv[1:]]  # host:port, [address]:port for IPv6
    ChatServer(addresses, type=peers.CHAT_SERVER).serve_forever()


if __name__ == "__main__":
    main()
