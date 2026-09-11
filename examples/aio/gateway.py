"""The gateway: an asyncio TCP server. Each line a client sends becomes a
request to the chat server, awaited on the event loop, and its reply goes
back to that client; whatever the chat server broadcasts goes to every
client of this gateway. One AsyncClient per process, from a holder, the
shape an ASGI server's worker uses.

    bazel run //:gateway -- 127.0.0.1:1980 8765
"""

import asyncio
import sys

from multiplexer.aio import AsyncClient
from multiplexer.multiplexer_constants import peers, types

MX = AsyncClient.holder(peers.CHAT_GATEWAY, lambda: ENDPOINTS)
ENDPOINTS: list[tuple[str, int]] = []


class Gateway:
    """The TCP side: the clients, and what reaches them."""

    def __init__(self) -> None:
        self.clients: set[asyncio.StreamWriter] = set()
        self.unsubscribe = None

    async def start(self, host: str, port: int) -> asyncio.AbstractServer:
        """Listen, and subscribe to broadcasts for as long as the server runs."""
        self.unsubscribe = MX.get().subscribe(types.CHAT_BROADCAST, self.broadcast)
        return await asyncio.start_server(self.serve, host, port)

    async def broadcast(self, mxmsg) -> None:
        """A coroutine handler: runs on the loop, writes to every client."""
        for writer in list(self.clients):
            writer.write(b"* " + mxmsg.message + b"\n")
            await writer.drain()

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """One client: forward each line as a request, answer with the reply."""
        self.clients.add(writer)
        try:
            while line := await reader.readline():
                reply = await MX.get().query(line.rstrip(b"\n"), types.CHAT_REQUEST, timeout=10)
                writer.write(reply.message + b"\n")
                await writer.drain()
        finally:
            self.clients.discard(writer)
            writer.close()


async def main(addresses: list[str], port: int) -> None:
    ENDPOINTS[:] = [(host, int(mx_port)) for host, mx_port in (address.rsplit(":", 1) for address in addresses)]
    server = await Gateway().start("127.0.0.1", port)
    print("ready", server.sockets[0].getsockname()[1], flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:-1], int(sys.argv[-1])))
