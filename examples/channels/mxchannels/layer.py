"""A Django Channels channel layer on the multiplexer: the in-memory layer
in every process, and the multiplexer between the processes.

    CHANNEL_LAYERS = {
        "default": {
            "BACKEND": "mxchannels.MultiplexerChannelLayer",
            "CONFIG": {"addresses": "10.0.0.1:1980,10.0.0.2:1980"},   # every multiplexer
        }
    }

group_send() delivers to this process's members of the group and sends
one event, routed to every process, which delivers to its own members.
Membership is kept by the process that called group_add(), which is the
one that owns the channel when consumers call it, as they do. send() to a
specific channel of another process is addressed to that process, whose
instance id the channel name carries, the way channels_redis embeds a
prefix in it; a channel of this process is delivered in memory. Both go
through every multiplexer, so one dying loses nothing. What one event
loop sends is numbered: a receiver takes it in that order, each message
once, and drops one that a later one overtook when a multiplexer failed.
receive() is the in-memory queue.

Normal channels, the ones `runworker` consumes, are not routed: send() to
one raises NotImplementedError, since a backend on the multiplexer is the
better worker. Capacity and expiry are the in-memory layer's: at most
once, ChannelFull from send() when a channel of this process is full,
and anything else for a full channel dropped by the process that holds
it, with a warning.

The layer works from any event loop: sync code calling it through
async_to_sync, a new loop each call, sends through the same client. The
client is routed no group event until the process makes a channel, so a
process that only sends, a Celery worker or a WSGI server, never carries
the application's traffic. What arrives is delivered on the loop the
client was made on; new_channel() makes a new client on the running loop
when that one has closed."""

import asyncio
import itertools
import logging
import random
import string
import time
import weakref

import msgpack
from channels.exceptions import ChannelFull
from channels.layers import InMemoryChannelLayer
from multiplexer.aio import AsyncClient
from multiplexer.Multiplexer_pb2 import Routing
from multiplexer.threaded_client import ThreadedClient

from channels_pb2 import ChannelEnvelope
from multiplexer_constants import peers, types

log = logging.getLogger("mxchannels")

FORGET_STREAM = 300  # seconds of silence after which a stream's last number is forgotten


def parse_addresses(addresses) -> list[tuple[str, int]]:
    """ "host:port,host:port", or a list of (host, port), as the list the library takes."""
    if isinstance(addresses, str):
        addresses = [item.rsplit(":", 1) for item in addresses.split(",")]
    return [(host, int(port)) for host, port in addresses]


class MultiplexerChannelLayer(InMemoryChannelLayer):
    """Channels' layer API; the in-memory layer for this process, the multiplexer for the others."""

    def __init__(self, addresses, timeout: float = 10, **kwargs):
        super().__init__(**kwargs)
        self.timeout = timeout
        self._holder = AsyncClient.holder(peers.CHANNELS, parse_addresses(addresses), timeout=timeout)
        self._subscribed: AsyncClient | None = None  # the client whose messages this layer already takes
        self._receiving: AsyncClient | None = None  # the client routed the group events, once a channel was made
        # Sending: each loop's stream, [its id, the last number sent]. Only
        # on one loop is the order of the numbers the order of the sends.
        self._streams: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, list[int]] = weakref.WeakKeyDictionary()
        self._stream_ids = itertools.count(1)
        # Receiving: (sender, stream) -> (the last number delivered, when).
        self._last: dict[tuple[int, int], tuple[int, float]] = {}
        self._swept = time.monotonic()
        self.dropped = 0  # messages for a full channel of this process, from group sends and other processes

    async def get_client(self) -> AsyncClient:
        """The process's client, connected on first use; for a consumer
        that queries a backend too. It sends and queries from any loop. A
        new one is routed nothing by the rules: no group event comes to a
        process before it has a channel for one to go to."""
        client = await self._holder.aget()
        if client is not self._subscribed:
            client.set_routing(Routing(any=False, all=False))
            client.subscribe(types.CHANNEL_GROUP_SEND, self._on_group_send)
            client.subscribe(types.CHANNEL_SEND, self._on_send)
            self._subscribed = client
        return client

    # Names: a specific channel carries the instance id of the process that owns it.

    async def new_channel(self, prefix="specific."):
        """A channel of this process: the prefix, `mx` and this client's
        instance id in 16 hex digits, `!`, a random suffix. The first asks
        the multiplexers for the group events, which a channel can take."""
        client = await self.get_client()
        if client.loop.is_closed():
            await self._holder.aclose()  # a channel needs a client whose loop delivers
            client = await self.get_client()
        if client is not self._receiving:
            client.set_routing(Routing())  # the group events, from now on
            self._receiving = client
        suffix = "".join(random.choice(string.ascii_letters) for _ in range(12))
        return f"{prefix.rstrip('.')}.mx{client.instance_id:016x}!{suffix}"

    @staticmethod
    def owner_of(channel: str) -> int | None:
        """The instance id a specific channel's name carries; None for a name this layer did not make."""
        if "!" not in channel:
            return None
        node = channel.split("!", 1)[0].rsplit(".", 1)[-1]
        if len(node) != 18 or not node.startswith("mx"):
            return None
        try:
            return int(node[2:], 16)
        except ValueError:
            return None

    # The layer API.

    async def send(self, channel, message):
        """In memory for a channel of this process, addressed to its process for any other."""
        assert isinstance(message, dict), "message is not a dict"
        self.require_valid_channel_name(channel)
        if "!" not in channel:
            raise NotImplementedError(
                f"{channel!r} is a normal channel, which this layer does not route: consumers use specific channels"
                " and groups, and a worker is a backend on the multiplexer that a consumer queries"
            )
        owner = self.owner_of(channel)
        if owner is None:
            raise ValueError(f"{channel!r} is not a channel this layer named")
        client = await self.get_client()
        if owner == client.instance_id:
            await super().send(channel, message)
            return
        payload = msgpack.packb(message, use_bin_type=True)
        msgpack.unpackb(payload, raw=False)  # a message its owner could not unpack is refused here
        await self._send_to_others(client, ChannelEnvelope(channel=channel, payload=payload), types.CHANNEL_SEND, owner)

    async def group_send(self, group, message):
        """This process's members now, every other process's through one event to all of them."""
        assert isinstance(message, dict), "Message is not a dict"
        self.require_valid_group_name(group)
        # Packed before anyone gets it, and unpacked again: every member,
        # here and elsewhere, gets the same, and a message the other
        # processes could not unpack is refused here, for everyone.
        payload = msgpack.packb(message, use_bin_type=True)
        await self._to_members(group, msgpack.unpackb(payload, raw=False))
        client = await self.get_client()
        await self._send_to_others(client, ChannelEnvelope(group=group, payload=payload), types.CHANNEL_GROUP_SEND)

    async def _send_to_others(self, client: AsyncClient, envelope: ChannelEnvelope, type: int, to: int = 0) -> None:
        """Number the envelope in the running loop's stream and send it
        through every multiplexer; nothing between the number and the send
        lets another send of this loop in."""
        stream = self._streams.get(asyncio.get_running_loop())
        if stream is None:
            stream = self._streams[asyncio.get_running_loop()] = [next(self._stream_ids), 0]
        stream[1] += 1
        envelope.stream, envelope.seq = stream
        await client.send_message(
            envelope.SerializeToString(), type=type, to=to, multiplexer=ThreadedClient.ALL, timeout=self.timeout
        )

    async def close(self):
        """Close the process's client; the in-memory part needs nothing."""
        await self._holder.aclose()
        self._subscribed = self._receiving = None

    # What the other processes send, delivered on the client's loop.

    async def _on_group_send(self, mxmsg) -> None:
        """Another process's group_send: this process's members of the group get it."""
        client = self._subscribed
        if client is None or mxmsg.from_ == client.instance_id:
            return  # this process's own send, delivered already
        envelope = ChannelEnvelope()
        envelope.ParseFromString(mxmsg.message)
        if self._in_order(mxmsg.from_, envelope):
            await self._to_members(envelope.group, msgpack.unpackb(envelope.payload, raw=False))

    async def _on_send(self, mxmsg) -> None:
        """Another process's send to a channel of this one."""
        envelope = ChannelEnvelope()
        envelope.ParseFromString(mxmsg.message)
        if self._in_order(mxmsg.from_, envelope):
            await self._to_channel(envelope.channel, msgpack.unpackb(envelope.payload, raw=False))

    def _in_order(self, sender: int, envelope: ChannelEnvelope) -> bool:
        """Whether the envelope is the newest of its stream yet, which it
        then is; an older one is a repeat, or was overtaken, and is dropped."""
        now = time.monotonic()
        key = (sender, envelope.stream)
        last = self._last.get(key)
        if last is not None and envelope.seq <= last[0]:
            return False
        self._last[key] = (envelope.seq, now)
        if now - self._swept > FORGET_STREAM:
            # A stream silent that long has ended, and no copy of it can
            # still be on its way: forgetting it bounds the map.
            self._swept = now
            self._last = {key: value for key, value in self._last.items() if now - value[1] < FORGET_STREAM}
        return True

    async def _to_members(self, group: str, message: dict) -> None:
        """Every channel of this process in the group; a full one drops it,
        with a warning. Expired messages and memberships go at receive(), as
        in the in-memory layer, not on this path, once per message."""
        for channel in list(self.groups.get(group, ())):
            await self._to_channel(channel, message)

    async def _to_channel(self, channel: str, message: dict) -> None:
        """A channel of this process; a full one drops it, with a warning."""
        try:
            await InMemoryChannelLayer.send(self, channel, message)
        except ChannelFull:
            self.dropped += 1
            if self.dropped & (self.dropped - 1) == 0:  # the 1st, 2nd, 4th, 8th...: a trickle, not a flood
                log.warning("channel %s is full: a message dropped, %d so far", channel, self.dropped)
