"""The gateway: an asyncio TCP server. Each line a client sends becomes a
request to the chat server, awaited on the event loop, and its reply goes
back to that client, or "! " and the error's name when there was none;
whatever the chat server broadcasts goes to every client of this gateway.
Each client has an outbox and a task writing it, so a client that reads
slowly or leaves holds up nobody else, and one that falls a thousand
lines behind is dropped. One AsyncClient per process, from a holder, the
shape an ASGI server's worker uses.

    bazel run //:gateway -- 127.0.0.1:1980 8765
"""

import asyncio
import sys

from multiplexer.aio import AsyncClient
from multiplexer.endpoints import parse_endpoint
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import NotConnected, OperationFailed, OperationTimedOut
from multiplexer.threaded_client import BackendError

MX = AsyncClient.holder(peers.CHAT_GATEWAY, lambda: ENDPOINTS)
ENDPOINTS: list[tuple[str, int]] = []
LAST_WRITES = 10  # seconds a client that has sent its last line gets to read the answers it is owed


class Gateway:
    """The TCP side: the clients, and what reaches them."""

    def __init__(self, outbox: int = 1000) -> None:
        self.clients: dict[asyncio.StreamWriter, asyncio.Queue[bytes]] = {}  # each client's outbox
        self.outbox = outbox
        self.unsubscribe = None

    async def start(self, host: str, port: int) -> asyncio.Server:
        """Listen, and subscribe to broadcasts for as long as the server runs."""
        self.unsubscribe = MX.get().subscribe(types.CHAT_BROADCAST, self.broadcast)
        return await asyncio.start_server(self.serve, host, port)

    def broadcast(self, mxmsg) -> None:
        """A plain handler, called on the loop in the order the broadcasts
        arrive: the line into every client's outbox, waiting for none."""
        line = b"* " + mxmsg.message + b"\n"
        for writer, outbox in list(self.clients.items()):
            try:
                outbox.put_nowait(line)
            except asyncio.QueueFull:
                writer.transport.abort()  # too far behind: dropped at once, unsent lines and all; its serve() ends

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """One client: forward each line as a request, answer with the reply;
        once it has sent its last line, the answers it is owed go out before
        the socket closes."""
        outbox: asyncio.Queue[bytes] = asyncio.Queue(self.outbox)
        self.clients[writer] = outbox
        sending = asyncio.ensure_future(self.send(writer, outbox))
        try:
            while line := await reader.readline():
                try:
                    reply = await MX.get().query(line.rstrip(b"\n"), types.CHAT_REQUEST, timeout=10)
                    answer = reply.message
                except (OperationFailed, OperationTimedOut, NotConnected, BackendError) as error:
                    answer = b"! " + type(error).__name__.encode()
                if not await self.enqueue(outbox, sending, answer + b"\n"):
                    return  # its writer ended: the client is gone
            del self.clients[writer]  # no broadcast after its last line
            await asyncio.wait_for(self.finish(outbox, sending), LAST_WRITES)
        except asyncio.TimeoutError:
            pass  # it stopped reading as well
        except (ConnectionError, ValueError):
            pass  # the client went, or sent a line longer than the reader's 64 KiB
        finally:
            self.clients.pop(writer, None)
            sending.cancel()
            writer.close()

    @staticmethod
    async def enqueue(outbox: "asyncio.Queue[bytes]", sending: "asyncio.Future[None]", data: bytes) -> bool:
        """Into the client's outbox, waiting while it is full; False when its
        writer has ended, since nothing would ever take from it then."""
        try:
            outbox.put_nowait(data)
            return True
        except asyncio.QueueFull:
            pass
        put = asyncio.ensure_future(outbox.put(data))
        try:
            await asyncio.wait((put, sending), return_when=asyncio.FIRST_COMPLETED)
            return put.done()
        finally:
            put.cancel()  # nothing once it is done; a put still waiting would never be taken

    async def finish(self, outbox: "asyncio.Queue[bytes]", sending: "asyncio.Future[None]") -> None:
        """The end of the outbox, after what is in it, and its writer done."""
        if await self.enqueue(outbox, sending, b""):
            await sending

    async def send(self, writer: asyncio.StreamWriter, outbox: "asyncio.Queue[bytes]") -> None:
        """One client's writer: its outbox in order, at the pace its socket
        takes it, up to the empty line that ends it."""
        try:
            while data := await outbox.get():
                writer.write(data)
                await writer.drain()
        except ConnectionError:
            writer.close()  # gone: serve() sees the end of its reader, or of this task


async def main(addresses: list[str], port: int) -> None:
    """The multiplexers' addresses, then the port: serve until interrupted."""
    ENDPOINTS[:] = [parse_endpoint(address) for address in addresses]  # host:port, [address]:port for IPv6
    server = await Gateway().start("127.0.0.1", port)
    print("ready", server.sockets[0].getsockname()[1], flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:-1], int(sys.argv[-1])))
