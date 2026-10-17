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


# XXH3 (xxHash 0.8, seed 0), a lane at a time, written apart from the
# libraries' (lib/xxh3.cc, src/xxh3.rs), which the frames pin: its
# constants, and the default secret, which the specification fixes.
MASK64 = 2**64 - 1
PRIME32_1, PRIME32_2, PRIME32_3 = 0x9E3779B1, 0x85EBCA77, 0xC2B2AE3D
PRIME64_1, PRIME64_2, PRIME64_3 = 0x9E3779B185EBCA87, 0xC2B2AE3D27D4EB4F, 0x165667B19E3779F9
PRIME64_4, PRIME64_5 = 0x85EBCA77C2B2AE63, 0x27D4EB2F165667C5
SECRET = bytes.fromhex(
    "b8fe6c3923a44bbe7c01812cf721ad1cded46de9839097db7240a4a4b7b3671f"
    "cb79e64eccc0e578825ad07dccff7221b8084674f743248ee03590e6813a264c"
    "3c2852bb91c300cb88d0658b1b532ea371644897a20df94e3819ef46a9deacd8"
    "a8fa763fe39c343ff9dcbbc7c70b4f1d8a51e04bcdb45931c89f7ec9d9787364"
    "eac5ac8334d3ebc3c581a0fffa1363eb170ddd51b7f0da49d316552629d4689e"
    "2b16be587d47a1fc8ff8b8d17ad031ce45cb3a8f95160428afd7fbcabb4b407e"
)


def read64(data: bytes, at: int) -> int:
    """The 8 bytes of `data` at `at` as a little-endian number."""
    return int.from_bytes(data[at : at + 8], "little")


def read32(data: bytes, at: int) -> int:
    """The 4 bytes of `data` at `at` as a little-endian number."""
    return int.from_bytes(data[at : at + 4], "little")


def fold(left: int, right: int) -> int:
    """The 128-bit product of the two, its halves xored together."""
    product = left * right
    return (product ^ (product >> 64)) & MASK64


def avalanche(value: int) -> int:
    """The last mix of every input over 8 bytes."""
    value ^= value >> 37
    value = (value * 0x165667919E3779F9) & MASK64
    return value ^ (value >> 32)


def xxh64_avalanche(value: int) -> int:
    """The last mix of an input of up to 3 bytes: XXH64's."""
    value ^= value >> 33
    value = (value * PRIME64_2) & MASK64
    value ^= value >> 29
    value = (value * PRIME64_3) & MASK64
    return value ^ (value >> 32)


def rrmxmx(value: int, length: int) -> int:
    """The last mix of an input of 4 to 8 bytes."""
    value ^= ((value << 49 | value >> 15) ^ (value << 24 | value >> 40)) & MASK64
    value = (value * 0x9FB21C651E98DF25) & MASK64
    value ^= (value >> 35) + length
    value = (value * 0x9FB21C651E98DF25) & MASK64
    return value ^ (value >> 28)


def mix16(data: bytes, at: int, secret: int) -> int:
    """16 bytes of `data` at `at` against 16 of the secret at `secret`."""
    return fold(read64(data, at) ^ read64(SECRET, secret), read64(data, at + 8) ^ read64(SECRET, secret + 8))


def xxh3_64(data: bytes) -> int:
    """XXH3-64 of `data`."""
    length = len(data)
    if length > 8 and length <= 16:
        low = read64(data, 0) ^ read64(SECRET, 24) ^ read64(SECRET, 32)
        high = read64(data, length - 8) ^ read64(SECRET, 40) ^ read64(SECRET, 48)
        swapped = int.from_bytes(low.to_bytes(8, "little"), "big")
        return avalanche((length + swapped + high + fold(low, high)) & MASK64)
    if length >= 4 and length <= 8:
        both = read32(data, length - 4) | read32(data, 0) << 32
        return rrmxmx(both ^ read64(SECRET, 8) ^ read64(SECRET, 16), length)
    if length > 0 and length < 4:
        combined = data[0] << 16 | data[length >> 1] << 24 | data[length - 1] | length << 8
        return xxh64_avalanche(combined ^ read32(SECRET, 0) ^ read32(SECRET, 4))
    if length == 0:
        return xxh64_avalanche(read64(SECRET, 56) ^ read64(SECRET, 64))
    accumulator = length * PRIME64_1
    if length <= 128:
        # 16 bytes from each end, and 16 more from each end for every 32
        # over 32; a sum, so in any order.
        for pair in range((length - 1) // 32 + 1):
            accumulator += mix16(data, 16 * pair, 32 * pair) + mix16(data, length - 16 * (pair + 1), 32 * pair + 16)
        return avalanche(accumulator & MASK64)
    if length <= 240:
        for round in range(8):
            accumulator += mix16(data, 16 * round, 16 * round)
        accumulator = avalanche(accumulator & MASK64)
        for round in range(8, length // 16):
            accumulator += mix16(data, 16 * round, 16 * (round - 8) + 3)
        return avalanche((accumulator + mix16(data, length - 16, 136 - 17)) & MASK64)
    lanes = [PRIME32_3, PRIME64_1, PRIME64_2, PRIME64_3, PRIME64_4, PRIME32_2, PRIME64_5, PRIME32_1]

    def stripe(at: int, secret: int) -> None:
        for lane in range(8):
            value = read64(data, at + 8 * lane)
            keyed = value ^ read64(SECRET, secret + 8 * lane)
            lanes[lane ^ 1] = (lanes[lane ^ 1] + value) & MASK64
            lanes[lane] = (lanes[lane] + (keyed & 0xFFFFFFFF) * (keyed >> 32)) & MASK64

    blocks = (length - 1) // 1024
    for block in range(blocks):
        for index in range(16):
            stripe(1024 * block + 64 * index, 8 * index)
        for lane in range(8):
            value = lanes[lane] ^ lanes[lane] >> 47 ^ read64(SECRET, 128 + 8 * lane)
            lanes[lane] = (value * PRIME32_1) & MASK64
    for index in range((length - 1 - 1024 * blocks) // 64):
        stripe(1024 * blocks + 64 * index, 8 * index)
    stripe(length - 64, 192 - 64 - 7)
    result = length * PRIME64_1
    for pair in range(4):
        secret = 11 + 16 * pair
        result += fold(lanes[2 * pair] ^ read64(SECRET, secret), lanes[2 * pair + 1] ^ read64(SECRET, secret + 8))
    return avalanche(result & MASK64)


def check_xxh3() -> None:
    """Fails unless xxh3_64 gives the values of xxHash's own sanity check,
    over its buffer, at lengths that reach every way XXH3 hashes."""
    state, buffer = 2654435761, bytearray()
    for _ in range(2367):
        buffer.append(state >> 56)
        state = (state * 11400714785074694797) & MASK64
    known = {
        0: 0x2D06800538D394C2,
        1: 0xC44BDFF4074EECDB,
        6: 0x27B56A84CD2D7325,
        12: 0xA713DAF0DFBB77E7,
        24: 0xA3FE70BF9D3510EB,
        48: 0x397DA259ECBA1F11,
        80: 0xBCDEFBBB2C47C90A,
        195: 0xCD94217EE362EC3A,
        403: 0xCDEB804D65C6DEA4,
        512: 0x617E49599013CB6B,
        2048: 0xDD59E2C3A5F038E0,
        2099: 0xC6B9D9B3FC9AC765,
        2240: 0x6E73A90539CF2948,
        2367: 0xCB37AEB9E5D361ED,
    }
    for length, expected in known.items():
        assert xxh3_64(bytes(buffer[:length])) == expected, f"XXH3 of {length} bytes"


def frame(message: MultiplexerMessage) -> bytes:
    """The frame of `message`: the length of its body and its checksum, the
    low 32 bits of its XXH3-64, little-endian, then the body."""
    body = message.SerializeToString()
    return len(body).to_bytes(4, "little") + (xxh3_64(body) & 0xFFFFFFFF).to_bytes(4, "little") + body


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
    check_xxh3()
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
