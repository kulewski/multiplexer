"""Echo client: sends one ECHO_REQUEST and prints the answer.

Usage: client [host:port] [text]
"""

import sys

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types


def main(argv: list[str]) -> None:
    """Send one request and print the reply."""
    host, port = (argv[1] if len(argv) > 1 else "127.0.0.1:1980").rsplit(":", 1)
    text = argv[2] if len(argv) > 2 else "hello multiplexer"
    client = Client([(host, int(port))], type=peers.ECHO_CLIENT)
    response = client.query(text.encode(), type=types.ECHO_REQUEST, timeout=10)
    print(response.message.decode())
    client.shutdown()


if __name__ == "__main__":
    main(sys.argv)
