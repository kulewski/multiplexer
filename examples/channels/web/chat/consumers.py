"""The chat consumer, as in the Channels tutorial: a socket joins its
room's group, every line it sends goes to the group, and every member's
consumer forwards what the group receives to its socket. Nothing here
knows about the multiplexer; the channel layer is the multiplexer. Each
line carries the pid of the process that received it, so that a line
relayed from another process shows as such."""

import json
import os
from typing import Any, cast

from channels.generic.websocket import AsyncWebsocketConsumer


class ChatConsumer(AsyncWebsocketConsumer):
    """One socket in one room."""

    async def connect(self):
        """Join the room's group, then say which process this socket landed on."""
        self.room_name = cast(dict[str, Any], self.scope)["url_route"]["kwargs"]["room_name"]
        self.group = f"chat.{self.room_name}"
        await self.channel_layer.group_add(self.group, self.channel_name)
        await self.accept()
        await self.send(
            text_data=json.dumps({"message": f"connected to gateway {os.getpid()}", "gateway": os.getpid()})
        )

    async def disconnect(self, code):
        """Leave the group."""
        await self.channel_layer.group_discard(self.group, self.channel_name)

    async def receive(self, text_data=None, bytes_data=None):
        """A line from the socket: to everyone in the room, through every process."""
        message = json.loads(text_data or "{}")["message"]
        await self.channel_layer.group_send(
            self.group, {"type": "chat.message", "message": message, "gateway": os.getpid()}
        )

    async def chat_message(self, event):
        """A line from the group: to the socket."""
        await self.send(text_data=json.dumps({"message": event["message"], "gateway": event["gateway"]}))
