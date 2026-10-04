"""The room consumer: one socket, one participant. Every 10 ms frame the
socket sends goes through the multiplexers to a worker as a query, and
the worker's answer, the frame with the effect applied and its spectrum,
goes to the room's group, which every gateway process delivers to its
sockets in that room, this one included. The browser mixes what it
receives and draws it.

A stream sticks to one worker: the effects keep state per stream, so
after the first answer the frames are addressed to the worker that gave
it. When it is gone the addressed query fails at once, the same frame
goes again unaddressed, the multiplexer gives it to a worker round
robin, and the effect starts afresh there. A frame that times out is
lost, and the stream stays: a worker that answers in microseconds and
missed one frame is still the stream's, until three in a row say it is
stuck.

A frame that waited in front of the consumer, behind a slow moment, is
dropped rather than sent to the room late: the page would skip it
anyway, and every hop would carry it first.

The frames are binary. Up: seq (uint32) and the browser's capture time
in ms (float64), then 960 bytes of PCM. Down: participant, seq, capture
time, the gateway's round trip through the broker in microseconds, the
worker's microseconds, the worker's name, its first 16 bytes, then 32
bytes of spectrum and the PCM; all little-endian. A text message sets
the effect."""

import json
import logging
import os
import random
import struct
import time
from typing import Any, cast

from channels.generic.websocket import AsyncWebsocketConsumer
from multiplexer.aio import AsyncClient
from multiplexer.mxclient import NotConnected, OperationFailed, OperationTimedOut
from multiplexer.threaded_client import BackendError

from audio_pb2 import AudioFrame, AudioProcessed
from multiplexer_constants import types
from mxchannels import MultiplexerChannelLayer

UP = struct.Struct("<Id")  # seq, captured_at
DOWN = struct.Struct("<IIdII16s")  # participant, seq, captured_at, gateway_us, worker_us, worker
FRAME_BYTES = 960  # 480 samples of 16-bit PCM: 10 ms at 48 kHz
EFFECTS = ["none", "telephone", "robot", "echo"]
QUERY_TIMEOUT = 0.05  # per attempt; a frame lost with its worker costs at most three of these
STUCK = 3  # timeouts in a row after which the stream leaves its worker
STALE_MS = 60  # later than the fastest frame by this much: the page's six frames of buffer, dropped here
CATCH_UP_MS = 0.01  # per frame, the most the fastest may drift up: clocks of two machines run apart

log = logging.getLogger("room")


class AudioConsumer(AsyncWebsocketConsumer):
    """One participant's frames out to a worker and into the room."""

    async def connect(self):
        """Join the room, and tell the page who it is here."""
        self.room_name = cast(dict[str, Any], self.scope)["url_route"]["kwargs"]["room_name"]
        self.group = f"audio.{self.room_name}"
        self.participant = random.getrandbits(32)
        self.effect = "none"
        self.worker = 0  # the instance id of the worker this stream sticks to, once one answered
        self.timeouts = 0  # in a row, on that worker
        self.fastest: float | None = None  # the least our clock minus the capture time, in ms
        self.lost = self.stale = 0
        await self.channel_layer.group_add(self.group, self.channel_name)
        await self.accept()
        await self.send(
            text_data=json.dumps({"participant": self.participant, "gateway": os.getpid(), "effects": EFFECTS})
        )

    async def disconnect(self, code):
        """Leave the room, and say what the stream lost."""
        await self.channel_layer.group_discard(self.group, self.channel_name)
        if self.lost or self.stale:
            log.info(
                "participant %d left: %d frames lost, %d dropped as stale", self.participant, self.lost, self.stale
            )

    async def receive(self, text_data=None, bytes_data=None):
        """A frame to the worker and into the room; or a setting."""
        if text_data is not None:
            effect = json.loads(text_data).get("effect")
            if effect in EFFECTS:
                self.effect = effect
            return
        if bytes_data is None or len(bytes_data) < UP.size + FRAME_BYTES:
            return
        seq, captured_at = UP.unpack_from(bytes_data)
        if self._stale(captured_at):
            self.stale += 1
            return
        frame = AudioFrame(
            participant=self.participant,
            seq=seq,
            effect=self.effect,
            pcm=bytes_data[UP.size : UP.size + FRAME_BYTES],
            captured_at=captured_at,
        )
        layer = self.channel_layer
        assert isinstance(layer, MultiplexerChannelLayer)
        client = await layer.get_client()  # the layer's own client: the same connections carry the frames
        started = time.perf_counter()
        reply = await self._ask(client, frame.SerializeToString())
        if reply is None:
            self.lost += 1
            return
        gateway_us = int((time.perf_counter() - started) * 1e6)
        self.worker = reply.sender
        self.timeouts = 0
        processed = AudioProcessed.FromString(reply.message)
        await layer.group_send(
            self.group,
            {
                "type": "audio.frame",
                "participant": processed.participant,
                "seq": processed.seq,
                "captured_at": processed.captured_at,
                "gateway_us": gateway_us,
                "worker_us": processed.micros,
                "worker": processed.worker,
                "spectrum": processed.spectrum,
                "pcm": processed.pcm,
            },
        )

    def _stale(self, captured_at: float) -> bool:
        """Whether the frame waited long enough in front of the consumer
        to be worth nothing: later, against the capture time, than the
        fastest frame of the stream by more than the page would buffer."""
        behind = time.monotonic() * 1000 - captured_at  # the clocks' difference, plus the wait
        self.fastest = behind if self.fastest is None else min(self.fastest + CATCH_UP_MS, behind)
        return behind - self.fastest > STALE_MS

    async def _ask(self, client: AsyncClient, payload: bytes):
        """The worker's answer: from the stream's worker while it lives, from any worker after it is gone."""
        if self.worker:
            try:
                return await client.query(payload, type=types.AUDIO_FRAME, timeout=QUERY_TIMEOUT, to=self.worker)
            except OperationFailed:
                self.worker = 0  # gone: this frame again, to a worker the multiplexer picks, afresh there
            except OperationTimedOut:
                self.timeouts += 1
                if self.timeouts >= STUCK:
                    self.worker = 0  # stuck: the next frame to another; the worker starts afresh if it comes back
                return None
            except (NotConnected, BackendError):
                return None
        try:
            return await client.query(payload, type=types.AUDIO_FRAME, timeout=QUERY_TIMEOUT)
        except (OperationFailed, OperationTimedOut, NotConnected, BackendError):
            return None

    async def audio_frame(self, event):
        """A frame from the room, anyone's: to the socket, packed."""
        header = DOWN.pack(
            event["participant"],
            event["seq"],
            event["captured_at"],
            event["gateway_us"],
            event["worker_us"],
            event["worker"].encode()[:16],
        )
        await self.send(bytes_data=header + event["spectrum"] + event["pcm"])
