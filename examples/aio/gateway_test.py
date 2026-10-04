"""The gateway in-process on the test's loop, the chat server on a
BackendThread, a real multiplexer from Cluster: two TCP clients, a line
answered to the one that sent it, a shout reaching both; a client that
stops reading, dropped without holding up the others; a client that
half-closes still getting its answer, and one that resets with its
outbox full forgotten; and a line nobody serves, answered with the
error's name."""

import asyncio
import socket
import struct
import unittest

from multiplexer.multiplexer_constants import peers
from multiplexer.testing import BackendThread, Cluster

import backend
import gateway
from multiplexer.testing import runfile

RULES = runfile("chat.rules")  # the file the constants were generated from


class GatewayTest(unittest.IsolatedAsyncioTestCase):
    async def test_lines_are_answered_and_shouts_reach_everyone(self):
        with (
            Cluster(1, rules=RULES) as cluster,
            BackendThread(lambda: backend.ChatServer(cluster.endpoints, type=peers.CHAT_SERVER)),
        ):
            cluster.wait_for_peer("CHAT_SERVER")
            gateway.ENDPOINTS[:] = cluster.endpoints
            server = await gateway.Gateway().start("127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            try:
                first = await asyncio.open_connection("127.0.0.1", port)
                second = await asyncio.open_connection("127.0.0.1", port)
                first[1].write(b"hello\n")
                await first[1].drain()
                self.assertEqual(b"HELLO\n", await asyncio.wait_for(first[0].readline(), 10))
                second[1].write(b"shout everyone\n")
                await second[1].drain()
                lines = sorted(
                    [
                        await asyncio.wait_for(second[0].readline(), 10),
                        await asyncio.wait_for(second[0].readline(), 10),
                    ]
                )
                self.assertEqual([b"* EVERYONE\n", b"SHOUT EVERYONE\n"], lines)
                self.assertEqual(b"* EVERYONE\n", await asyncio.wait_for(first[0].readline(), 10))
                for _, writer in (first, second):
                    writer.close()
            finally:
                server.close()
                await server.wait_closed()
                gateway.MX.close()

    async def test_a_client_that_stops_reading_holds_up_nobody(self):
        with (
            Cluster(1, rules=RULES) as cluster,
            BackendThread(lambda: backend.ChatServer(cluster.endpoints, type=peers.CHAT_SERVER)),
        ):
            cluster.wait_for_peer("CHAT_SERVER")
            gateway.ENDPOINTS[:] = cluster.endpoints
            chat = gateway.Gateway(outbox=10)  # ten lines behind is too far here; a thousand in use
            server = await chat.start("127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            stalled = socket.socket()  # never read, and a small buffer, so that it fills soon
            stalled.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            writer = None
            try:
                stalled.connect(("127.0.0.1", port))
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                await asyncio.sleep(0.1)
                self.assertEqual(2, len(chat.clients))
                padding = b"x" * 60000  # sixty of these are more than a stalled socket takes
                for n in range(60):
                    writer.write(b"shout %02d " % n + padding + b"\n")
                shouts = []
                for _ in range(120):  # every broadcast and every answer, in order
                    line = await asyncio.wait_for(reader.readline(), 10)
                    if line.startswith(b"* "):
                        shouts.append(int(line[2:4]))
                self.assertEqual(list(range(60)), shouts)
                self.assertEqual(1, len(chat.clients), "the stalled one dropped, the other kept")
            finally:
                stalled.close()
                if writer is not None:
                    writer.close()
                server.close()
                await server.wait_closed()
                gateway.MX.close()

    async def test_a_client_that_half_closes_gets_its_last_answer(self):
        with (
            Cluster(1, rules=RULES) as cluster,
            BackendThread(lambda: backend.ChatServer(cluster.endpoints, type=peers.CHAT_SERVER)),
        ):
            cluster.wait_for_peer("CHAT_SERVER")
            gateway.ENDPOINTS[:] = cluster.endpoints
            server = await gateway.Gateway().start("127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.write(b"hello\n")
                writer.write_eof()  # at once: the end comes while the query is out
                self.assertEqual(b"HELLO\n", await asyncio.wait_for(reader.read(), 10), "the answer, then the end")
                writer.close()
            finally:
                server.close()
                await server.wait_closed()
                gateway.MX.close()

    async def test_a_client_that_resets_with_its_outbox_full_is_forgotten(self):
        with (
            Cluster(1, rules=RULES) as cluster,
            BackendThread(lambda: backend.ChatServer(cluster.endpoints, type=peers.CHAT_SERVER)),
        ):
            cluster.wait_for_peer("CHAT_SERVER")
            gateway.ENDPOINTS[:] = cluster.endpoints
            chat = gateway.Gateway(outbox=2)
            server = await chat.start("127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            loop = asyncio.get_running_loop()
            client = socket.socket()  # asks and never reads
            client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            client.setblocking(False)
            asking = None
            try:
                await loop.sock_connect(client, ("127.0.0.1", port))
                for _ in range(100):
                    if chat.clients:
                        break
                    await asyncio.sleep(0.01)
                # The gateway's side of this one connection buffers little too, so that a few answers fill it.
                (served,) = chat.clients
                served.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
                lines = b"".join(b"line %02d " % n + b"x" * 60000 + b"\n" for n in range(20))
                asking = asyncio.ensure_future(loop.sock_sendall(client, lines))
                for _ in range(200):
                    if chat.clients[served].full():
                        break
                    await asyncio.sleep(0.05)
                self.assertTrue(chat.clients[served].full(), "the answers it does not read fill its outbox")
                asking.cancel()
                client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                client.close()  # a reset: the writer task ends, its outbox full
                for _ in range(100):
                    if not chat.clients:
                        break
                    await asyncio.sleep(0.05)
                self.assertEqual({}, chat.clients, "serve() ended rather than waiting on the outbox for good")
            finally:
                if asking is not None:
                    asking.cancel()
                client.close()
                server.close()
                await server.wait_closed()
                gateway.MX.close()

    async def test_a_line_nobody_serves_gets_the_errors_name(self):
        with Cluster(1, rules=RULES) as cluster:
            gateway.ENDPOINTS[:] = cluster.endpoints
            server = await gateway.Gateway().start("127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.write(b"hello\n")
                self.assertEqual(b"! OperationFailed\n", await asyncio.wait_for(reader.readline(), 15))
                writer.write(b"again\n")
                self.assertEqual(b"! OperationFailed\n", await asyncio.wait_for(reader.readline(), 15), "still served")
                writer.close()
            finally:
                server.close()
                await server.wait_closed()
                gateway.MX.close()


if __name__ == "__main__":
    unittest.main()
