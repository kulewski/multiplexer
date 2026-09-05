"""Reading a multiplexer's recording (`mxcontrol run_multiplexer --record`).

The file is a stream of Record messages (multiplexer/Recording.proto), each
prefixed with its length as a varint, the same framing as the binary log.
read() yields them in order; describe() renders one as a line with the
peer and message type names taken from the generated constants, which is
also what `python -m multiplexer.recording FILE` prints.

    for record in read("mx.rec"):
        if record.HasField("routed") and record.routed.type == types.SEARCH_REQUEST:
            ...

A recording carries the SHA-1 of the rules file it was made with; read()
refuses one made with different rules than the constants were generated
from, since type numbers would then mean other things.
"""

import argparse
import datetime
import sys
from typing import Iterator

from google.protobuf.internal.decoder import _DecodeVarint

from multiplexer.Recording_pb2 import PeerEvent, Record, RoutedMessage
from multiplexer import multiplexer_constants


class RulesMismatch(Exception):
    """The recording was made with a rules file other than the one the constants come from."""


def read(path: str, check_rules: bool = True, constants=multiplexer_constants) -> Iterator[Record]:
    """Yield every Record in the file, in order. With `check_rules`, the
    first record's rules hash must match RULES_SHA1 of `constants`, the
    generated constants module of the rules file the multiplexer ran with."""
    with open(path, "rb") as recording:
        data = recording.read()
    position = 0
    first = True
    while position < len(data):
        size, position = _DecodeVarint(data, position)
        record = Record()
        record.ParseFromString(data[position : position + size])
        position += size
        if first:
            first = False
            if check_rules and record.HasField("header") and record.header.rules_sha1 != constants.RULES_SHA1:
                raise RulesMismatch(
                    "recorded with rules %s, these constants are from %s"
                    % (record.header.rules_sha1, constants.RULES_SHA1)
                )
        yield record


def peer_name(peer_type: int, constants=multiplexer_constants) -> str:
    """The peer type's name from the constants, or the number."""
    return constants.peers.get_name(peer_type, str(peer_type)) if peer_type else "-"


def type_name(message_type: int, constants=multiplexer_constants) -> str:
    """The message type's name from the constants, or the number."""
    return constants.types.get_name(message_type, str(message_type))


def describe(record: Record, constants=multiplexer_constants) -> str:
    """One line for a record: the time, the kind, and the fields with names
    resolved from `constants`."""
    when = datetime.datetime.fromtimestamp(record.timestamp_us / 1e6).strftime("%H:%M:%S.%f")
    kind = record.WhichOneof("event")
    if kind == "header":
        header = record.header
        return "%s header multiplexer=%d rules=%s payload_limit=%d" % (
            when,
            header.multiplexer_id,
            header.rules_sha1[:12],
            header.payload_limit,
        )
    if kind == "peer":
        peer = record.peer
        return "%s peer %s id=%d type=%s" % (
            when,
            PeerEvent.Kind.Name(peer.kind),
            peer.peer_id,
            peer_name(peer.peer_type, constants),
        )
    routed = record.routed
    line = "%s routed %s type=%s id=%d from=%d (%s)" % (
        when,
        RoutedMessage.Disposition.Name(routed.disposition),
        type_name(routed.type, constants),
        routed.id,
        getattr(routed, "from"),
        peer_name(routed.from_peer_type, constants),
    )
    if routed.recipient or routed.recipient_peer_type:
        line += " -> %d (%s)" % (routed.recipient, peer_name(routed.recipient_peer_type, constants))
    if routed.to:
        line += " to=%d" % routed.to
    if routed.references:
        line += " references=%d" % routed.references
    if routed.error_reported:
        line += " error_reported"
    line += " payload=%r%s" % (routed.payload[:64], "..." if routed.truncated or len(routed.payload) > 64 else "")
    return line


def involves_peer(record: Record, peer_id: int) -> bool:
    """Whether the record is about `peer_id`: a peer event for it, or a message it sent, received or was addressed with."""
    if record.HasField("peer"):
        return record.peer.peer_id == peer_id
    if record.HasField("routed"):
        routed = record.routed
        return peer_id in (getattr(routed, "from"), routed.recipient, routed.to)
    return False


def main(argv: list[str] | None = None) -> int:
    """Print a recording one line per record, optionally filtered."""
    parser = argparse.ArgumentParser(description="print a multiplexer recording")
    parser.add_argument("path")
    parser.add_argument("--type", help="only routed messages of this type, by name or number")
    parser.add_argument("--peer", type=int, help="only records involving this instance id")
    parser.add_argument("--no-rules-check", action="store_true", help="accept a recording made with other rules")
    args = parser.parse_args(argv)
    constants = multiplexer_constants
    wanted_type = None
    if args.type is not None:
        wanted_type = int(args.type) if args.type.isdigit() else getattr(constants.types, args.type)
    for record in read(args.path, check_rules=not args.no_rules_check):
        if wanted_type is not None and not (record.HasField("routed") and record.routed.type == wanted_type):
            continue
        if args.peer is not None and not involves_peer(record, args.peer):
            continue
        print(describe(record))
    return 0


if __name__ == "__main__":
    sys.exit(main())
