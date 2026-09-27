"""The layer's test and the chat's, and the way to test a Channels
application on the library's harness (docs/api_python.md, "Testing"):
real multiplexers from `Cluster`, two layers on them standing in for two
gateway processes, a multiplexer frozen and then killed under a stream of
sends, sync code through async_to_sync, and the consumer driven by
Channels' own `WebsocketCommunicator` against Django configured for the
cluster; and the committed constants checked against the rules file.
Needs Channels, which test.sh arranges; the multiplexers are the
mxcontrol the package installed unless MXCONTROL names another; `python
-m unittest -v test`.

The walkthrough's "Testing it with the harness" section walks through
this file."""

import asyncio
import datetime
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "web"))

import msgpack  # noqa: E402
from asgiref.sync import async_to_sync  # noqa: E402
from channels.exceptions import ChannelFull  # noqa: E402
from multiplexer.aio import AsyncClient  # noqa: E402
from multiplexer.testing import Cluster, mxcontrol_path  # noqa: E402

from channels_pb2 import ChannelEnvelope  # noqa: E402
from multiplexer_constants import peers, types  # noqa: E402
from mxchannels import MultiplexerChannelLayer  # noqa: E402

RULES = os.path.join(HERE, "channels.rules")  # what the constants were generated from; Cluster's multiplexers read it


def endpoints_text(cluster: Cluster) -> str:
    """The cluster's multiplexers as the addresses a settings file names."""
    return ",".join(f"{host}:{port}" for host, port in cluster.endpoints)


async def receive(layer: MultiplexerChannelLayer, channel: str, timeout: float = 5) -> dict:
    """The next message on the channel, within `timeout` seconds."""
    return await asyncio.wait_for(layer.receive(channel), timeout)


class LayerTest(unittest.IsolatedAsyncioTestCase):
    """Two layers on one cluster, as two gateway processes would be. A
    fresh pair per test, since a client belongs to the loop it was made
    on and this test case makes a loop per test."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(2, rules=RULES).__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.__exit__(None, None, None)

    async def asyncSetUp(self):
        self.a = MultiplexerChannelLayer(addresses=self.cluster.endpoints, capacity=1000)
        self.b = MultiplexerChannelLayer(addresses=self.cluster.endpoints, capacity=1000)
        self.channel_a = await self.a.new_channel()  # naming a channel makes the process's client
        self.channel_b = await self.b.new_channel()
        # The clients connect on their io threads; the multiplexers' peers
        # files say when both are registered on every multiplexer.
        await asyncio.to_thread(self.cluster.wait_for_peer, peers.CHANNELS, 2)

    async def asyncTearDown(self):
        await self.a.close()
        await self.b.close()

    async def test_a_group_send_reaches_the_members_in_every_process(self):
        await self.a.group_add("room", self.channel_a)
        await self.b.group_add("room", self.channel_b)
        message = {"type": "chat.message", "text": "hello"}
        await self.a.group_send("room", message)
        self.assertEqual(message, await receive(self.a, self.channel_a), "the sender's own process")
        self.assertEqual(message, await receive(self.b, self.channel_b), "the other process")
        await self.b.group_discard("room", self.channel_b)
        await self.a.group_send("room", {"type": "chat.message", "text": "again"})
        self.assertEqual("again", (await receive(self.a, self.channel_a))["text"])
        with self.assertRaises(asyncio.TimeoutError):  # the builtin TimeoutError from 3.11 on
            await receive(self.b, self.channel_b, 0.3)

    async def test_a_send_to_a_channel_of_another_process_is_addressed(self):
        for n in range(200):  # through both multiplexers, and in order
            await self.a.send(self.channel_b, {"type": "x", "n": n})
        self.assertEqual(list(range(200)), [(await receive(self.b, self.channel_b))["n"] for _ in range(200)])
        await self.a.send(self.channel_a, {"type": "x", "n": 2})
        self.assertEqual(
            {"type": "x", "n": 2}, await receive(self.a, self.channel_a), "a channel of its own, in memory"
        )

    async def test_channel_names_carry_the_process(self):
        self.assertRegex(self.channel_a, r"^specific\.mx[0-9a-f]{16}![A-Za-z]{12}$")
        self.a.require_valid_channel_name(self.channel_a)  # raises for a name Channels would refuse
        self.assertNotEqual(
            MultiplexerChannelLayer.owner_of(self.channel_a), MultiplexerChannelLayer.owner_of(self.channel_b)
        )
        self.assertIsNone(MultiplexerChannelLayer.owner_of("specific..inmemory!abc"), "another layer's name")
        self.assertIsNone(MultiplexerChannelLayer.owner_of("specific.abc!x"), "hex, but not this layer's")
        worker = await self.b.new_channel("worker")  # a prefix without the dot, as channels_redis takes it
        self.assertEqual(MultiplexerChannelLayer.owner_of(self.channel_b), MultiplexerChannelLayer.owner_of(worker))
        await self.a.send(worker, {"type": "x"})
        self.assertEqual({"type": "x"}, await receive(self.b, worker))

    async def test_what_the_others_cannot_unpack_is_refused_for_everyone(self):
        await self.a.group_add("room", self.channel_a)
        await self.b.group_add("room", self.channel_b)
        with self.assertRaises(TypeError):
            await self.a.group_send("room", {"type": "x", "when": datetime.datetime.now()})
        with self.assertRaises(ValueError):
            await self.a.group_send("room", {"type": "x", "by number": {1: "one"}})
        with self.assertRaises(ValueError):
            await self.a.send(self.channel_b, {"type": "x", "by number": {1: "one"}})
        await self.a.group_send("room", {"type": "x", "pair": (1, 2)})
        self.assertEqual([1, 2], (await receive(self.a, self.channel_a))["pair"], "as msgpack gives it, here too")
        self.assertEqual([1, 2], (await receive(self.b, self.channel_b))["pair"])
        with self.assertRaises(asyncio.TimeoutError):
            await receive(self.a, self.channel_a, 0.3)  # the refused ones reached nobody, here either

    async def test_a_stream_is_delivered_in_order_each_message_once(self):
        """A sender's envelopes out of order and repeated, as a failing
        multiplexer can let them come: the older ones are dropped."""
        await self.b.group_add("room", self.channel_b)
        async with AsyncClient(self.cluster.endpoints, peers.CHANNELS) as sender:
            await asyncio.to_thread(self.cluster.wait_for_peer, peers.CHANNELS, 3)
            lane = sender.lane()  # one connection: the order sent is the order they come in, as the test needs
            for seq in (1, 3, 2, 3, 4):
                envelope = ChannelEnvelope(
                    group="room", stream=7, seq=seq, payload=msgpack.packb({"type": "x", "n": seq})
                )
                await sender.send_message(envelope.SerializeToString(), type=types.CHANNEL_GROUP_SEND, multiplexer=lane)
            owner = MultiplexerChannelLayer.owner_of(self.channel_b)
            for seq in (2, 1, 3):  # another stream of the same sender: numbered on its own
                envelope = ChannelEnvelope(
                    channel=self.channel_b, stream=8, seq=seq, payload=msgpack.packb({"n": -seq})
                )
                await sender.send_message(
                    envelope.SerializeToString(), type=types.CHANNEL_SEND, to=owner, multiplexer=lane
                )
            self.assertEqual([1, 3, 4, -2, -3], [(await receive(self.b, self.channel_b))["n"] for _ in range(5)])
            with self.assertRaises(asyncio.TimeoutError):
                await receive(self.b, self.channel_b, 0.3)

    async def test_a_normal_channel_is_refused(self):
        with self.assertRaises(NotImplementedError):
            await self.a.send("background-tasks", {"type": "x"})

    async def test_a_full_channel_drops(self):
        small = MultiplexerChannelLayer(addresses=self.cluster.endpoints, capacity=3)
        try:
            channel = await small.new_channel()
            for n in range(3):
                await small.send(channel, {"type": "x", "n": n})
            with self.assertRaises(ChannelFull):
                await small.send(channel, {"type": "x", "n": 3})
            with self.assertLogs("mxchannels", "WARNING"):
                for n in range(5):  # from another process: dropped there, with a warning, not raised here
                    await self.a.send(channel, {"type": "y", "n": n})
                await asyncio.sleep(0.3)
            self.assertEqual([0, 1, 2], [(await receive(small, channel))["n"] for _ in range(3)])
            with self.assertRaises(asyncio.TimeoutError):
                await receive(small, channel, 0.3)
            await small.group_add("room", channel)
            for n in range(3):
                await small.send(channel, {"type": "x", "n": n})
            await small.group_send("room", {"type": "z"})  # a group send drops too, here and from elsewhere
            await self.a.group_send("room", {"type": "z"})
            await asyncio.sleep(0.3)
            self.assertEqual(7, small.dropped, "five sends from the other process, two group sends")
        finally:
            await small.close()

    async def test_a_frozen_then_killed_multiplexer_loses_nothing(self):
        """The first multiplexer freezes under a stream of sends, holding
        what went through it, and is then killed: only the copies through
        the other one arrive, all of them, in order."""
        await self.b.group_add("room", self.channel_b)
        other = await self.b.new_channel()
        self.addCleanup(self._restart_first)
        for n in range(200):
            if n == 50:
                self.cluster.mx[0].pause()  # SIGSTOP: its sockets stay open, what it holds goes nowhere
            if n == 100:
                self.cluster.mx[0].kill()
            await self.a.group_send("room", {"type": "x", "n": n})
            await self.a.send(other, {"type": "x", "n": n})
        self.assertEqual(list(range(200)), [(await receive(self.b, self.channel_b))["n"] for _ in range(200)])
        self.assertEqual(list(range(200)), [(await receive(self.b, other))["n"] for _ in range(200)])

    def _restart_first(self) -> None:
        """The first multiplexer back, however the test ended; the next
        test's setup waits for its clients on it."""
        self.cluster.mx[0].kill()  # frozen still, if the test failed before killing it
        self.cluster.mx[0].start()

    async def test_sync_code_sends_through_async_to_sync(self):
        """A layer used only from sync code, a new loop each call, as a
        Django shell, a Celery task or a WSGI view uses it."""
        await self.b.group_add("room", self.channel_b)
        from_sync = MultiplexerChannelLayer(addresses=self.cluster.endpoints)

        def one_line() -> None:  # a shell's one line, and the shell stays open
            async_to_sync(from_sync.group_send)("room", {"type": "x", "n": 0})

        def more() -> None:
            for n in (1, 2):
                async_to_sync(from_sync.group_send)("room", {"type": "x", "n": n})
            async_to_sync(from_sync.send)(self.channel_b, {"type": "x", "n": 3})

        try:
            await asyncio.to_thread(one_line)
            client = from_sync._subscribed  # the client that call made, without asking the layer again
            assert client is not None
            self.assertTrue(client.loop.is_closed(), "made on the call's loop, which is gone")
            # Counted on the io thread, where a subscription's test runs for every message that comes: a
            # process with no channel is routed no group event, whatever group it is for.
            arrived: list[int] = []
            client.subscribe(
                types.CHANNEL_GROUP_SEND, lambda mxmsg: None, matching=lambda mxmsg: arrived.append(1) or False
            )
            for n in range(3):
                await self.b.group_send("elsewhere", {"type": "x", "n": n})
            await asyncio.sleep(0.3)
            self.assertEqual([], arrived, "no group event to a process that only sends")
            await asyncio.to_thread(more)
            self.assertEqual([0, 1, 2, 3], [(await receive(self.b, self.channel_b))["n"] for _ in range(4)])
            channel = await from_sync.new_channel()  # a channel needs a client on a loop that delivers
            self.assertIsNot(client, await from_sync.get_client())
            await self.b.send(channel, {"type": "x"})
            self.assertEqual({"type": "x"}, await receive(from_sync, channel))
        finally:
            await from_sync.close()

    async def test_flush_empties_this_process(self):
        await self.a.send(self.channel_a, {"type": "x"})
        await self.a.flush()
        with self.assertRaises(asyncio.TimeoutError):
            await receive(self.a, self.channel_a, 0.3)


class ChatTest(unittest.IsolatedAsyncioTestCase):
    """The consumer through Channels' communicator, with Django configured against a real multiplexer."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(1, rules=RULES).__enter__()
        # The settings read the multiplexers' addresses from the environment
        # when Django imports them, which setup() does.
        os.environ["MX_ADDRESSES"] = endpoints_text(cls.cluster)
        os.environ["DJANGO_SETTINGS_MODULE"] = "webapp.settings"
        import django

        django.setup()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.__exit__(None, None, None)

    async def asyncTearDown(self):
        from channels.layers import get_channel_layer

        layer = get_channel_layer()
        assert isinstance(layer, MultiplexerChannelLayer)
        await layer.close()  # the next test runs on a new loop, and a client belongs to one

    async def test_a_line_reaches_everyone_in_the_room(self):
        from channels.testing import WebsocketCommunicator

        from webapp.asgi import application

        origin = [(b"origin", b"http://localhost")]  # what a browser sends, and what the origin validator wants
        alice = WebsocketCommunicator(application, "/ws/chat/lobby/", headers=origin)
        bob = WebsocketCommunicator(application, "/ws/chat/lobby/", headers=origin)
        carol = WebsocketCommunicator(application, "/ws/chat/elsewhere/", headers=origin)
        for socket in (alice, bob, carol):
            connected, _ = await socket.connect()
            self.assertTrue(connected)
            self.assertIn("connected to gateway", (await socket.receive_json_from())["message"])
        await alice.send_json_to({"message": "hi bob"})
        self.assertEqual("hi bob", (await bob.receive_json_from())["message"])
        self.assertEqual("hi bob", (await alice.receive_json_from())["message"], "the sender hears it too")
        self.assertTrue(await carol.receive_nothing(0.3), "another room")
        for socket in (alice, bob, carol):
            await socket.disconnect()

    async def test_the_pages_render(self):
        from django.test import AsyncClient

        web = AsyncClient()
        self.assertIn(b"Which room?", (await web.get("/")).content)
        self.assertIn(b"Room lobby", (await web.get("/lobby/")).content)


class ConstantsTest(unittest.TestCase):
    """The committed constants are what mxcontrol writes from the rules file
    as it is now: a change to the file that left them behind fails here."""

    def test_the_constants_are_generated_from_the_rules_file(self):
        out = tempfile.mkdtemp()
        written = [os.path.join(out, name) for name in ("multiplexer_constants.py", "multiplexer_constants.pyi")]
        command = [
            mxcontrol_path(),
            "generate_constants",
            "channels.rules",
            "--python",
            written[0],
            "--pyi",
            written[1],
        ]
        subprocess.run(command, cwd=HERE, check=True, capture_output=True)  # the file's name as the header records it
        for path in written:
            with open(path) as generated, open(os.path.join(HERE, os.path.basename(path))) as committed:
                self.assertEqual(committed.read(), generated.read(), os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
