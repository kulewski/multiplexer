"""A whole MultiplexerMessage is the message itself, in every Python client
and server. Sent without an id or a sender it gets both, as new_message()
and a reply fill them, and send_message() returns that id: it went out as
id 0, which every library receiver drops, and send_message() returned 0;
the caller's message is left as it was, and AsyncClient has new_message()
too. Message fields beside a whole message are a TypeError, where they
were dropped unsaid; a query takes a whole message as its request, typed
and addressed by its own fields, its workflow and payload kept, where
ThreadedClient and AsyncClient sent it serialized as the payload of
another request and SyncClient refused it with an assert; and a plain
server's reply given as one gets its empty fields from the request it
answers, as a threaded server's does, where it went out as it was, an
answer to nothing. Ordered, not timed: each message is read off a raw
peer's socket, or is a query's answer.
"""

import asyncio
import unittest

from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, runfile
from multiplexer.testing.raw_peer import RawPeer
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")

REQUEST = types.PYTHON_TEST_REQUEST
RESPONSE = types.PYTHON_TEST_RESPONSE


class Backend(BaseMultiplexerServer):
    """Answers a request with its payload upper-cased, as a whole
    MultiplexerMessage that sets nothing but its type and payload: the
    server fills in the rest from the request, its workflow among them."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)

    def handle_message(self, mxmsg) -> None:
        """The reply, a whole message."""
        self.send_message(message=MultiplexerMessage(type=RESPONSE, message=mxmsg.message.upper()))


class WholeMessageTest(unittest.TestCase):
    """See the module docstring."""

    def check(self, receiver: RawPeer, bare: MultiplexerMessage, sent: int, sender: int) -> None:
        """What `receiver` reads next is `bare` with the id `sent` and the sender `sender`, and `bare` is as it was."""
        received = receiver.receive_type(types.PYTHON_TEST_REQUEST)
        self.assertEqual(bare.message, received.message)
        self.assertNotEqual(0, sent)
        self.assertEqual(sent, received.id)
        self.assertEqual(sender, received.sender)
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
                self.assertEqual(sender, built.sender)
            finally:
                receiver.close()

    def test_fields_beside_a_whole_message_are_a_type_error(self) -> None:
        """A send or a query given a whole message and message fields
        beside it, and a query of a payload without its type, raise
        TypeError before anything goes out, where the fields were dropped
        unsaid."""
        with Cluster(1, rules=RULES) as cluster:
            with Client(cluster.endpoints, type=peers.WEBSITE) as client:
                whole = client.new_message(type=REQUEST, message=b"question")
                with self.assertRaises(TypeError):
                    client.send_message(whole, type=RESPONSE)
                with self.assertRaises(TypeError):
                    client.query(whole, REQUEST)
                with self.assertRaises(TypeError):
                    client.query(whole, to=1)
                with self.assertRaises(TypeError):
                    client.query(b"question")
            with ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT) as threaded:
                whole = threaded.new_message(type=REQUEST, message=b"question")
                with self.assertRaises(TypeError):
                    threaded.send_message(whole, type=RESPONSE)
                with self.assertRaises(TypeError):
                    threaded.query(whole, REQUEST)
                with self.assertRaises(TypeError):
                    threaded.query(b"question")

            async def ask() -> None:
                async with AsyncClient(cluster.endpoints, peers.TEST_ACTIVE_CLIENT) as asynchronous:
                    whole = asynchronous.new_message(type=REQUEST, message=b"question")
                    with self.assertRaises(TypeError):
                        await asynchronous.send_message(whole, type=RESPONSE)
                    with self.assertRaises(TypeError):
                        await asynchronous.query(whole, REQUEST)

            asyncio.run(ask())

    def test_a_query_takes_a_whole_message_as_its_request(self) -> None:
        """Each client's query of a whole message sends that message: the
        backend upper-cases its payload, and the reply carries its workflow
        back, the backend's whole-message reply filled in from the request.
        Addressed by its own `to`, the same."""
        with Cluster(1, rules=RULES) as cluster, BackendThread(lambda: Backend(cluster.endpoints)) as served:
            assert served.backend is not None
            backend_id = served.backend.conn.instance_id
            with Client(cluster.endpoints, type=peers.WEBSITE) as client:
                request = client.new_message(type=REQUEST, message=b"question", workflow=b"trace")
                reply = client.query(request, timeout=10)
                self.assertEqual((b"QUESTION", b"trace"), (reply.message, reply.workflow))
                addressed = client.new_message(type=REQUEST, message=b"addressed", workflow=b"trace", to=backend_id)
                reply = client.query(addressed, timeout=10)
                self.assertEqual((b"ADDRESSED", b"trace"), (reply.message, reply.workflow))
            with ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT) as threaded:
                request = threaded.new_message(type=REQUEST, message=b"question", workflow=b"trace")
                reply = threaded.query(request, timeout=10)
                self.assertEqual((b"QUESTION", b"trace"), (reply.message, reply.workflow))
                addressed = threaded.new_message(type=REQUEST, message=b"addressed", workflow=b"trace", to=backend_id)
                reply = threaded.query(addressed, timeout=10)
                self.assertEqual((b"ADDRESSED", b"trace"), (reply.message, reply.workflow))

            async def ask() -> MultiplexerMessage:
                async with AsyncClient(cluster.endpoints, peers.TEST_ACTIVE_CLIENT) as asynchronous:
                    request = asynchronous.new_message(type=REQUEST, message=b"question", workflow=b"trace")
                    return await asynchronous.query(request, timeout=10)

            reply = asyncio.run(ask())
            self.assertEqual((b"QUESTION", b"trace"), (reply.message, reply.workflow))

    def test_a_plain_servers_whole_message_reply_answers_the_request(self) -> None:
        """The backend's reply sets only its type and payload; the server
        fills in to, references and workflow from the request it handles,
        so the query of a payload gets it as its answer, where it went out
        with none of them, routed by its type, and the query timed out."""
        with Cluster(1, rules=RULES) as cluster, BackendThread(lambda: Backend(cluster.endpoints)):
            with Client(cluster.endpoints, type=peers.WEBSITE) as client:
                reply = client.query(b"question", REQUEST, timeout=10)
                self.assertEqual(b"QUESTION", reply.message)


if __name__ == "__main__":
    unittest.main()
