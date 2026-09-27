"""A chat client for the command line, for the walkthrough's steps: joins a
room on a gateway, prints every line the room receives with the gateway
that received it, and sends the lines given.

    python chat_cli.py GATEWAY ROOM [--say LINE ...] [--listen SECONDS]

GATEWAY is host:port of a gateway process, e.g. 127.0.0.1:8000. Lines are
sent one per second; then the client listens for --listen seconds."""

import argparse
import asyncio
import json
import sys

import websockets
from websockets.typing import Origin


async def chat(gateway: str, room: str, lines: list[str], listen: float) -> None:
    """Join, say the lines, print what the room receives meanwhile and for `listen` seconds after."""
    async with websockets.connect(f"ws://{gateway}/ws/chat/{room}/", origin=Origin(f"http://{gateway}")) as socket:

        async def printing() -> None:
            async for text in socket:
                data = json.loads(text)
                print(f"{data['message']}   [via gateway {data['gateway']}]", flush=True)

        printer = asyncio.create_task(printing())
        for line in lines:
            await socket.send(json.dumps({"message": line}))
            await asyncio.sleep(1)
        await asyncio.sleep(listen)
        printer.cancel()


def main() -> int:
    """One client, one room."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("gateway", help="host:port of a gateway process")
    parser.add_argument("room")
    parser.add_argument("--say", action="append", default=[], help="a line to send; repeatable")
    parser.add_argument("--listen", type=float, default=2, help="seconds to keep listening after the last line")
    args = parser.parse_args()
    asyncio.run(chat(args.gateway, args.room, args.say, args.listen))
    return 0


if __name__ == "__main__":
    sys.exit(main())
