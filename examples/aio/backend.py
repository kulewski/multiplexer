"""The chat server: a backend that answers each line and broadcasts the
ones that start with "shout " to every gateway.

    bazel run //:backend -- 127.0.0.1:1980
"""

import sys

from multiplexer.multiplexer_constants import peers, types
from multiplexer.servers import BaseMultiplexerServer


class ChatServer(BaseMultiplexerServer):
    """Upper-cases what it is asked; a "shout" goes to everyone as well."""

    def handle_message(self, mxmsg):
        line = mxmsg.message.decode(errors="replace")
        if line.startswith("shout "):
            self.send_message(message=line[6:].upper().encode(), type=types.CHAT_BROADCAST, to=0, references=0)
        self.send_message(message=line.upper().encode(), type=types.CHAT_RESPONSE)


def main() -> None:
    addresses = [(host, int(port)) for host, port in (address.rsplit(":", 1) for address in sys.argv[1:])]
    ChatServer(addresses, type=peers.CHAT_SERVER).serve_forever()


if __name__ == "__main__":
    main()
