"""A participant for the command line, for the walkthrough's steps and the
numbers: joins a room on a gateway, streams a tone as 10 ms frames at
the real pace for a while, and reports what came back: how many of its
own frames, how many of everyone else's, and the round trips.

    python audio_cli.py GATEWAY ROOM [--seconds S] [--effect NAME] [--hz HZ]

GATEWAY is host:port of a gateway process, e.g. 127.0.0.1:8000. The
frames are what the page would send from a microphone; the summary is
what the page's meter shows, as medians and 99th percentiles."""

import argparse
import asyncio
import json
import math
import struct
import sys
import time

import websockets
from websockets.typing import Origin

UP = struct.Struct("<Id")
DOWN = struct.Struct("<IIdII16s")
FRAME = 480
SAMPLE_RATE = 48000


def tone(hz: float, frame_index: int) -> bytes:
    """One frame of a sine at `hz`, half scale, continuing from the frame before."""
    start = frame_index * FRAME
    samples = [int(16383 * math.sin(2 * math.pi * hz * (start + i) / SAMPLE_RATE)) for i in range(FRAME)]
    return struct.pack(f"<{FRAME}h", *samples)


def percentile(values: list[float], fraction: float) -> float:
    """The value `fraction` of the way through the sorted list, 0 for none."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


async def participate(gateway: str, room: str, seconds: float, effect: str, hz: float) -> int:
    """Stream the tone, take in the room, say which worker answers, print the summary; 1 if a frame of ours was lost."""
    origin = Origin(f"http://{gateway}")
    async with websockets.connect(f"ws://{gateway}/ws/audio/{room}/", origin=origin, max_size=None) as socket:
        hello = json.loads(await socket.recv())
        me = hello["participant"]
        print(f"participant {me} on gateway {hello['gateway']}, effect {effect}", flush=True)
        await socket.send(json.dumps({"effect": effect}))
        mine: list[tuple[float, int, int, str]] = []  # (end to end ms, gateway us, worker us, worker)
        others: dict[int, int] = {}
        sent = 0
        via = ""

        async def receiving() -> None:
            nonlocal via
            async for message in socket:
                if isinstance(message, str):
                    continue
                participant, seq, captured_at, gateway_us, worker_us, worker = DOWN.unpack_from(message)
                if participant == me:
                    name = worker.rstrip(b"\0").decode(errors="replace")  # 16 bytes may cut a character
                    if name != via:  # the first frame, and every time the stream moves
                        print(f"frame {seq}: back via {name}", flush=True)
                        via = name
                    mine.append((time.perf_counter() * 1000 - captured_at, gateway_us, worker_us, name))
                else:
                    others[participant] = others.get(participant, 0) + 1

        receiver = asyncio.create_task(receiving())
        started = time.perf_counter()
        while time.perf_counter() - started < seconds:
            await socket.send(UP.pack(sent, time.perf_counter() * 1000) + tone(hz, sent))
            sent += 1
            await asyncio.sleep(max(0.0, started + sent * 0.01 - time.perf_counter()))
        await asyncio.sleep(0.3)  # the last frames come back
        receiver.cancel()
    lost = sent - len(mine)
    workers = sorted({each[3] for each in mine})
    print(
        f"{sent} frames sent, {len(mine)} came back{f', {lost} lost' if lost else ''}, by {', '.join(workers) or 'nobody'}"
    )
    if mine:
        e2e = [each[0] for each in mine]
        gateway_ms = [each[1] / 1000 for each in mine]
        worker_ms = [each[2] / 1000 for each in mine]
        print(
            f"capture to arrival: median {percentile(e2e, 0.5):.1f} ms, p99 {percentile(e2e, 0.99):.1f} ms;"
            f" broker round trip at the gateway: median {percentile(gateway_ms, 0.5):.2f} ms, p99 {percentile(gateway_ms, 0.99):.2f} ms;"
            f" the worker: median {percentile(worker_ms, 0.5):.3f} ms"
        )
    for participant, count in sorted(others.items()):
        print(f"heard participant {participant}: {count} frames")
    return 1 if lost else 0


def main() -> int:
    """One participant, one room."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("gateway", help="host:port of a gateway process")
    parser.add_argument("room")
    parser.add_argument("--seconds", type=float, default=5)
    parser.add_argument("--effect", default="none")
    parser.add_argument("--hz", type=float, default=440)
    args = parser.parse_args()
    return asyncio.run(participate(args.gateway, args.room, args.seconds, args.effect, args.hz))


if __name__ == "__main__":
    sys.exit(main())
