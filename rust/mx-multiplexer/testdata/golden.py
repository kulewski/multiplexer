"""Writes golden_frames.txt beside it: frames of messages with every field
the protocol has set, as the Python package's protobuf encodes them, one
`NAME HEX` line each, for the crate's frame tests (src/frame_test.rs),
which build the same messages by name with prost and compare the bytes, so
that the field order and the proto2 encoding are pinned. Run it with
--check to say whether the committed file is what it would write, as the
Bazel test //rust/mx-multiplexer:golden_frames_test does.

A message added here is added to frame_test.rs's `golden_messages()` with
the same name and the same values.
"""

import argparse
import os
import sys
import zlib

from multiplexer.Multiplexer_pb2 import (
    BackendForPacketSearch,
    Compression,
    DeliveryError,
    KeyFilter,
    LoggingMethod,
    MultiplexerMessage,
    MultiplexerMessageDescription,
    PeerControl,
    PeerStatus,
    Routing,
    WelcomeMessage,
)

HERE = os.path.dirname(os.path.abspath(__file__))
OUTPUT = os.path.join(HERE, "golden_frames.txt")
MAX_U64 = 2**64 - 1


def payload(size: int) -> bytes:
    """`size` bytes counting up from 0, wrapping at 256."""
    return bytes(index % 256 for index in range(size))


def key_filters() -> list[KeyFilter]:
    """Two filters: one with two patterns, an empty one among them, and one with none."""
    return [KeyFilter(type=200, pattern=["user-[0-9]+", ""]), KeyFilter(type=4294967295)]


def messages() -> list[tuple[str, MultiplexerMessage]]:
    """The table: a name and a message, each a case the encoding could get wrong."""
    every_field = MultiplexerMessage(
        id=MAX_U64,
        sender=0x0123456789ABCDEF,
        to=MAX_U64 - 1,
        report_delivery_error=True,
        include_original_packet_in_report=False,
        type=4294967295,
        message=payload(300),
        compression=Compression.GZIP,
        timestamp=4294967295,
        references=1,
        workflow="\x00\xffworkflow",
        logging_method=LoggingMethod.CONSOLE,
        key="ключ-κλειδί",
    )
    every_field.override_rrules.add(
        peer="backend",
        peer_type=7,
        whom=MultiplexerMessageDescription.RoutingRule.ALL,
        delivery_error_is_error=False,
        report_delivery_error=False,
        include_original_packet_in_report=True,
    )
    every_field.override_rrules.add()
    welcome = WelcomeMessage(
        type=65, id=MAX_U64, routing=Routing(any=False, all=True, last_resort=True), key_filter=key_filters()
    )
    original = MultiplexerMessage(id=3, sender=4, to=5, type=300, message=b"original", key="k")
    delivery_error = DeliveryError(
        packet_id=MAX_U64,
        failed_type=[1, 300, 4294967295],
        failed_to=9,
        is_known_type=True,
        original_message=original,
        original_message_omitted=False,
    )
    return [
        ("minimal", MultiplexerMessage(type=0)),
        ("ping", MultiplexerMessage(id=1, sender=2, type=1)),
        ("empty_payload", MultiplexerMessage(id=1, sender=2, type=100, message=b"")),
        ("every_field", every_field),
        ("large_payload", MultiplexerMessage(id=10, sender=11, type=101, message=payload(20000))),
        ("welcome", MultiplexerMessage(id=12, sender=13, type=2, message=welcome.SerializeToString())),
        (
            "welcome_bare",
            MultiplexerMessage(id=14, sender=15, type=2, message=WelcomeMessage(type=1, id=0).SerializeToString()),
        ),
        (
            "peer_control",
            MultiplexerMessage(
                id=16,
                sender=17,
                type=11,
                message=PeerControl(routing=Routing(), key_filter=key_filters()).SerializeToString(),
            ),
        ),
        (
            "peer_status",
            MultiplexerMessage(
                id=18,
                sender=19,
                type=12,
                message=PeerStatus(
                    multiplexer_id=20, routing=Routing(last_resort=True), error="refused", key_filter=key_filters()
                ).SerializeToString(),
            ),
        ),
        (
            "search",
            MultiplexerMessage(
                id=21, sender=22, type=3, message=BackendForPacketSearch(packet_type=300).SerializeToString(), key="a"
            ),
        ),
        (
            "delivery_error",
            MultiplexerMessage(id=23, sender=24, to=25, type=5, message=delivery_error.SerializeToString()),
        ),
    ]


def frame(message: MultiplexerMessage) -> bytes:
    """The frame of `message`: the length and CRC-32 of its body, little-endian, then the body."""
    body = message.SerializeToString()
    return len(body).to_bytes(4, "little") + zlib.crc32(body).to_bytes(4, "little") + body


def generate() -> str:
    """The text of golden_frames.txt."""
    lines = ["# NAME FRAME-IN-HEX, written by golden.py from the Python package's protobuf; do not edit"]
    lines.extend(f"{name} {frame(message).hex()}" for name, message in messages())
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="exit 1 if the committed file is not what it would write")
    parser.add_argument("--output", default=OUTPUT, help="the file to write or compare with")
    arguments = parser.parse_args()
    text = generate()
    if arguments.check:
        with open(arguments.output) as file:
            if file.read() != text:
                print(f"{arguments.output} is stale: run golden.py again", file=sys.stderr)
                return 1
        return 0
    with open(arguments.output, "w") as file:
        file.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
