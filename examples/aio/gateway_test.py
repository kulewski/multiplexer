"""The gateway in-process on the test's loop, the chat server on a
BackendThread, a real multiplexer from Cluster: two TCP clients, a line
answered to the one that sent it, a shout reaching both."""

import asyncio
import unittest

from multiplexer.multiplexer_constants import peers
from multiplexer.testing import BackendThread, Cluster

import backend
import gateway


class GatewayTest(unittest.IsolatedAsyncioTestCase):
    async def test_lines_are_answered_and_shouts_reach_everyone(self):
        with (
            Cluster(1) as cluster,
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


if __name__ == "__main__":
    unittest.main()
