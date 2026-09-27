"""A whole MultiplexerMessage sent without an id or a sender gets both, in
every client, as new_message() and a reply fill them, and send_message()
returns that id: it went out as id 0, which every library receiver drops,
and send_message() returned 0. The caller's message is left as it was.
AsyncClient has new_message() too. Ordered, not timed: each message is read
off a raw peer's socket.
"""

import asyncio
import unittest

from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, runfile
from multiplexer.testing.raw_peer import RawPeer
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")


class WholeMessageTest(unittest.TestCase):
    """See the module docstring."""

    def check(self, receiver: RawPeer, bare: MultiplexerMessage, sent: int, sender: int) -> None:
        """What `receiver` reads next is `bare` with the id `sent` and the sender `sender`, and `bare` is as it was."""
        received = receiver.receive_type(types.PYTHON_TEST_REQUEST)
        self.assertEqual(bare.message, received.message)
        self.assertNotEqual(0, sent)
        self.assertEqual(sent, received.id)
        self.assertEqual(sender, getattr(received, "from"))
        self.assertEqual(0, bare.id)

    def test_every_client_fills_in_the_id_and_the_sender(self) -> None:
        with Cluster(1, rules=RULES) as cluster:
            receiver = RawPeer(cluster.endpoints[0], peers.PYTHON_TEST_SERVER)
            try:
                receiver.handshake()
                cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)

                def bare(payload: bytes) -> MultiplexerMessage:
                    """A message with neither id nor sender, addressed to the receiver."""
                    return MultiplexerMessage(type=types.PYTHON_TEST_REQUEST, message=payload, to=receiver.instance_id)

                sync = Client(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
                try:
                    message = bare(b"sync")
                    self.check(receiver, message, sync.send_message(message, flush=True), sync.instance_id)
                finally:
                    sync.shutdown()
                with ThreadedClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT) as threaded:
                    message = bare(b"threaded")
                    self.check(receiver, message, threaded.send_message(message, flush=True), threaded.instance_id)

                async def send() -> tuple[int, int, MultiplexerMessage]:
                    """Through an AsyncClient: (the id it returned, its instance id, the message it built)."""
                    client = AsyncClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT)
                    try:
                        sent = await client.send_message(bare(b"async"), flush=True)
                        return sent, client.instance_id, client.new_message(type=types.PYTHON_TEST_REQUEST)
                    finally:
                        client.close()

                sent, sender, built = asyncio.run(send())
                self.check(receiver, bare(b"async"), sent, sender)
                self.assertNotEqual(0, built.id)
                self.assertEqual(sender, getattr(built, "from"))
            finally:
                receiver.close()


if __name__ == "__main__":
    unittest.main()
