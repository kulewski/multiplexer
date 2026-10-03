"""A multiplexer's recording: reading it, and starting, stopping or tapping
it over the protocol.

The file is a stream of Record messages (multiplexer/Recording.proto), each
prefixed with its length as a varint, the same framing as the binary log.
read() yields them in order, read_many() merges several files by time;
describe() renders one as a line with the peer and message type names
taken from the generated constants, which is also what
`python -m multiplexer.recording FILE...` prints.

    for record in read("mx.rec"):
        if record.HasField("routed") and record.routed.type == types.SEARCH_REQUEST:
            ...

A recording carries the fingerprint, a CRC-32, of the rules file it was
made with, in its header, and of each other file put in use while it ran,
in a `rules` record; read() refuses one whose rules differ from those the
constants were generated from, at the header or partway through at such a
record, since type numbers would then mean other things.

start(), stop() and status() drive recording sessions on running
multiplexers through a client connected to them, one RecordingStatus per
multiplexer back; tap() yields every record as it is routed. The
multiplexers must allow it (`--recording-dir`, `--allow-tap`); the client
is any multiplexer.clients.SyncClient, for example one of the reserved
controller type:

    client = SyncClient(endpoints, type=RECORDING_CONTROLLER)
    for status in start(client, "checkout-bug"):
        print(status.multiplexer_id, status.path)
"""

import argparse
import datetime
import sys
import time
from typing import Any, Iterator

from google.protobuf.message import DecodeError

from multiplexer.Multiplexer_pb2 import RULES_CONTROLLER
from multiplexer.mxclient import NotConnected, OperationTimedOut, wait_seconds


from multiplexer.Recording_pb2 import (  # the reserved numbers, re-exported
    RECORDING_CONTROL,
    RECORDING_CONTROLLER,
    RECORDING_RECORD,
    RECORDING_STATUS,
    PeerEvent,
    Record,
    RecordingControl,
    RecordingStatus,
    RoutedMessage,
)
from multiplexer import multiplexer_constants


class RulesMismatch(Exception):
    """The recording was made with a rules file other than the one the constants come from."""


class TruncatedRecording(Exception):
    """The file ends partway through a record, or a record in it does not
    parse: what a multiplexer leaves while it writes the file, or when it
    dies. Raised after every whole record before it."""


def _decode_varint(data: bytes, position: int) -> tuple[int, int] | None:
    """The base-128 varint at `position`: (value, position after it), as the
    length prefix of every record in a recording is written; None when the
    data ends inside it."""
    result = 0
    shift = 0
    while position < len(data):
        byte = data[position]
        position += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, position
        shift += 7
    return None


def read(path: str, check_rules: bool = True, constants=multiplexer_constants) -> Iterator[Record]:
    """Yield every Record in the file, in order. With `check_rules`, the
    first record's rules fingerprint must match RULES_FINGERPRINT of `constants`, the
    generated constants module of the rules file the multiplexer ran with,
    and so must every later `rules` record's, one for a file put in use
    while recording: RulesMismatch is raised there, after the records
    before it were yielded. A file that ends partway through a record, as
    one a session is still writing or a multiplexer that died left can,
    raises TruncatedRecording there, after every whole record."""
    with open(path, "rb") as recording:
        data = recording.read()
    position = 0
    first = True
    while position < len(data):
        prefix = _decode_varint(data, position)
        if prefix is None or prefix[1] + prefix[0] > len(data):
            raise TruncatedRecording("%s ends partway through the record at byte %d" % (path, position))
        size, start = prefix
        record = Record()
        try:
            record.ParseFromString(data[start : start + size])
        except DecodeError as error:
            raise TruncatedRecording(
                "%s: the record at byte %d does not parse: %s" % (path, position, error)
            ) from error
        position = start + size
        if first:
            first = False
            if (
                check_rules
                and record.HasField("header")
                and record.header.rules_fingerprint != constants.RULES_FINGERPRINT
            ):
                raise RulesMismatch(
                    "recorded with rules %s, these constants are from %s"
                    % (record.header.rules_fingerprint, constants.RULES_FINGERPRINT)
                )
        elif check_rules and record.HasField("rules") and record.rules.fingerprint != constants.RULES_FINGERPRINT:
            # The multiplexer put another file in use while recording.
            raise RulesMismatch(
                "the rules changed to %s while recording, these constants are from %s"
                % (record.rules.fingerprint, constants.RULES_FINGERPRINT)
            )
        yield record


def read_many(paths: list[str], check_rules: bool = True, constants=multiplexer_constants) -> Iterator[Record]:
    """Every record of every file, merged by timestamp, each with
    `multiplexer_id` set from its file's header: the recordings of several
    multiplexers as one session. Clocks of different hosts differ, so the
    order between multiplexers is as good as their clocks. A file that
    ends partway through a record ends there, the others merge on to
    their ends, and TruncatedRecording then names every such file."""
    broken: list[str] = []

    def advance(records: Iterator[Record]) -> Record | None:
        """The stream's next record; None at its end, or where it breaks off, which is noted."""
        try:
            return next(records, None)
        except TruncatedRecording as error:
            broken.append(str(error))
            return None

    streams = []
    for path in paths:
        records = read(path, check_rules=check_rules, constants=constants)
        multiplexer_id = 0
        first = advance(records)
        if first is None:
            continue
        if first.HasField("header"):
            multiplexer_id = first.header.multiplexer_id
        streams.append((first, records, multiplexer_id))
    while streams:
        index = min(range(len(streams)), key=lambda position: streams[position][0].timestamp_us)
        record, records, multiplexer_id = streams[index]
        if multiplexer_id and not record.multiplexer_id:
            record.multiplexer_id = multiplexer_id
        yield record
        following = advance(records)
        if following is None:
            del streams[index]
        else:
            streams[index] = (following, records, multiplexer_id)
    if broken:
        raise TruncatedRecording("; ".join(broken))


def peer_name(peer_type: int, constants=multiplexer_constants) -> str:
    """The peer type's name from the constants, or the number."""
    if peer_type == RECORDING_CONTROLLER:
        return "RECORDING_CONTROLLER"
    if peer_type == RULES_CONTROLLER:
        return "RULES_CONTROLLER"
    return constants.peers.get_name(peer_type, str(peer_type)) if peer_type else "-"


def type_name(message_type: int, constants=multiplexer_constants) -> str:
    """The message type's name from the constants, or the number."""
    return constants.types.get_name(message_type, str(message_type))


def describe(record: Record, constants=multiplexer_constants) -> str:
    """One line for a record: the time, the kind, and the fields with names
    resolved from `constants`."""
    when = datetime.datetime.fromtimestamp(record.timestamp_us / 1e6).strftime("%H:%M:%S.%f")
    if record.multiplexer_id:
        when += " mx=%d" % record.multiplexer_id
    kind = record.WhichOneof("event")
    if kind == "header":
        header = record.header
        return "%s header multiplexer=%d rules=%s payload_limit=%d" % (
            when,
            header.multiplexer_id,
            header.rules_fingerprint[:12],
            header.payload_limit,
        )
    if kind == "rules":
        return "%s rules %s from %s (%d message types, %d peer types)" % (
            when,
            record.rules.fingerprint[:12],
            record.rules.path,
            record.rules.message_types,
            record.rules.peer_types,
        )
    if kind == "peer":
        peer = record.peer
        line = "%s peer %s id=%d type=%s" % (
            when,
            PeerEvent.Kind.Name(peer.kind),
            peer.peer_id,
            peer_name(peer.peer_type, constants),
        )
        if peer.kind == PeerEvent.ROUTING:
            line += " any=%s all=%s last_resort=%s" % tuple(
                "yes" if flag else "no" for flag in (peer.any, peer.all, peer.last_resort)
            )
        return line
    routed = record.routed
    line = "%s routed %s type=%s id=%d sender=%d (%s)" % (
        when,
        RoutedMessage.Disposition.Name(routed.disposition),
        type_name(routed.type, constants),
        routed.id,
        routed.sender,
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
        return peer_id in (routed.sender, routed.recipient, routed.to)
    return False


# Driving a recording over the protocol: RecordingControl to every
# multiplexer the client is connected to, one RecordingStatus back from each.


def control(client, action: "RecordingControl.Action", timeout: float = 5.0, **fields: Any) -> list[RecordingStatus]:
    """Send a RecordingControl with `action` and `fields` on every
    connection of `client` and return the statuses that came back within
    `timeout`, one per multiplexer; fewer when one did not answer. A
    negative `timeout` waits as long as it takes, as math.inf does; 0 and
    NaN wait for none, and read none. Raises NotConnected when the client
    has no connection."""
    request = RecordingControl(action=action, **fields)
    mxmsg = client.new_message(message=request.SerializeToString(), type=RECORDING_CONTROL)
    # A copy on every live connection now, and one status expected from each;
    # with none live, nothing to wait for.
    expected = client._schedule_all(mxmsg.SerializeToString(), mxmsg.id, mxmsg.type, timeout)
    if not expected:
        raise NotConnected()
    request_id = mxmsg.id
    statuses = []
    deadline = time.monotonic() + wait_seconds(timeout)
    while len(statuses) < expected:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            mxmsg, _ = client._receive([request_id], ignore_types=(RECORDING_RECORD,), timeout=remaining)
        except OperationTimedOut:
            break
        if mxmsg.type != RECORDING_STATUS:
            continue
        status = RecordingStatus()
        status.ParseFromString(mxmsg.message)
        statuses.append(status)
    return statuses


def start(
    client,
    label: str,
    payload_limit: int = 0,
    max_bytes: int | None = None,
    max_seconds: int = 0,
    timeout: float = 5.0,
) -> list[RecordingStatus]:
    """Open a file session named `label` on every multiplexer; each status
    carries the path, or `error` when it was refused. `max_bytes` unset
    leaves the multiplexer's default cap, 0 removes it."""
    fields: dict[str, Any] = dict(label=label, payload_limit=payload_limit, max_seconds=max_seconds)
    if max_bytes is not None:
        fields["max_bytes"] = max_bytes
    return control(client, RecordingControl.START, timeout=timeout, **fields)


def stop(client, timeout: float = 5.0) -> list[RecordingStatus]:
    """Close the file session on every multiplexer; the statuses say what was written."""
    return control(client, RecordingControl.STOP, timeout=timeout)


def status(client, timeout: float = 5.0) -> list[RecordingStatus]:
    """What every multiplexer is doing: recording, taps, the last session."""
    return control(client, RecordingControl.STATUS, timeout=timeout)


def tap(client, payload_limit: int = 0, timeout: float = 10.0) -> Iterator[Record]:
    """Subscribe on every connection, now, and return an iterator over the
    records as the multiplexers route them, `multiplexer_id` set; it raises
    OperationTimedOut when nothing arrives for `timeout` seconds. The
    subscriptions end with the client's connections, or with an UNTAP
    (`control(client, RecordingControl.UNTAP)`). Records the client does not
    read in time are dropped by the multiplexer, counted in its status."""
    control(client, RecordingControl.TAP, timeout=timeout, payload_limit=payload_limit)

    def records() -> Iterator[Record]:
        while True:
            mxmsg = client.read_message(timeout=timeout)
            if mxmsg.type != RECORDING_RECORD:
                continue
            record = Record()
            record.ParseFromString(mxmsg.message)
            yield record

    return records()


def main(argv: list[str] | None = None) -> int:
    """Print recordings one line per record, several files merged by time, optionally filtered."""
    parser = argparse.ArgumentParser(description="print a multiplexer recording")
    parser.add_argument("paths", nargs="+", metavar="path")
    parser.add_argument("--type", help="only routed messages of this type, by name or number")
    parser.add_argument("--peer", type=int, help="only records involving this instance id")
    parser.add_argument("--no-rules-check", action="store_true", help="accept a recording made with other rules")
    args = parser.parse_args(argv)
    constants = multiplexer_constants
    wanted_type = None
    if args.type is not None:
        wanted_type = int(args.type) if args.type.isdigit() else getattr(constants.types, args.type)
    try:
        for record in read_many(args.paths, check_rules=not args.no_rules_check):
            if wanted_type is not None and not (record.HasField("routed") and record.routed.type == wanted_type):
                continue
            if args.peer is not None and not involves_peer(record, args.peer):
                continue
            print(describe(record))
    except TruncatedRecording as error:
        # Printed up to there, it must not pass for the whole recording.
        print("recording: %s; printed up to it" % error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
