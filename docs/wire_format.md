# The wire format

A connection is one TCP stream in each direction, carrying frames. Anything
that speaks this format is a peer; the libraries are a convenience, not a
requirement. [multiplexer/testing/raw_peer.py](../multiplexer/testing/raw_peer.py) is a
complete peer in eighty lines of Python and
[tests/scenarios/raw_protocol/raw_protocol.py](../tests/scenarios/raw_protocol/raw_protocol.py) uses it
to perform everything on this page.

## A frame

| Bytes | Content |
|---|---|
| 0 to 3 | length of the body, unsigned 32-bit, little-endian |
| 4 to 7 | CRC-32 of the body, unsigned 32-bit, little-endian; the usual polynomial, `zlib.crc32` in Python |
| 8 onwards | the body: a serialized `MultiplexerMessage` |

A body of length 0, a length over 128 MiB, or a CRC that does not match
closes the connection. So does a body that is not a parseable
`MultiplexerMessage`.

## MultiplexerMessage

The envelope, defined in
[Multiplexer.proto](../multiplexer/Multiplexer.proto). Fields with a role in
the protocol:

| Field | Number | Role |
|---|---|---|
| `id` | 1 | required in practice: a receiver drops a message without one, and uses it to drop a copy it has already seen. A client that sends a request again, to the backend its search found, gives that attempt a new id and accepts a reply to either |
| `sender` | 2 | the sender's instance id; the multiplexer sends delivery errors to it; named `from` up to 2.3.1, the same number |
| `to` | 3 | an instance id; when set the multiplexer delivers there and consults no rule |
| `type` | 4 | the message type; decides routing and, below 100, protocol meaning |
| `message` | 5 | the payload, opaque bytes |
| `references` | 7 | the id of the message this one answers |
| `workflow` | 8 | opaque bytes for tracing, copied from request to reply by the libraries |
| `report_delivery_error` | 21 | for a `to`-addressed message: report when the target is gone |
| `override_rrules` | 20 | routing rules carried by the message, replacing the file's |
| `timestamp`, `compression`, `logging_method` | 6, 24, 23 | carried but not acted on by the multiplexer, except that `logging_method` gates its own log |

## The handshake

The peer's first frame must be a message of type `CONNECTION_WELCOME` (2)
whose payload is a `WelcomeMessage` with the peer's `type` and its `id`, the
instance id it will use as `sender`. Any other first message closes the
connection, as does a peer type absent from the rules file or one of the
reserved 99, the two controllers apart (`RULES_CONTROLLER` always,
`RECORDING_CONTROLLER` when remote recording is on). The multiplexer registers the connection, then answers with its own
`CONNECTION_WELCOME`: type `MULTIPLEXER` (1) and its instance id. A second
welcome on the same connection closes it.

There is no deadline for the welcome: a connection that never sends one
stays open, whoever the peer, for as long as its host answers TCP keepalive
(see [Heartbeats](#heartbeats)), since a synchronous client may send its
welcome long after it connected, its reconnect going on only inside its
next call. One whose host or network is gone is closed after 90 s.

The `id` is the peer's own choice. Two connections announcing the same id
from the same host replace each other, the newer one winning, and from
different hosts the second is refused; whatever the hosts, the second
replaces a first whose socket is gone, and is refused when its own socket is
gone, reset before its welcome was read. [Connecting to a multiplexer](handshake.md)
draws the exchange.

## Heartbeats

`HEARTBIT` (4) with an empty payload, sent every 3 s on an otherwise idle
connection by both sides. The multiplexer expects some frame from a
non-passive peer within 30 s, then grants 60 s more before closing the
connection. Any frame counts; a peer that keeps sending real messages need
not send heartbeats. To a passive peer the multiplexer sends at most one
heartbeat per frame received, 3 s after its last write, and expects none;
an idle passive connection owed none costs it no wakeup. Below the protocol, the
multiplexer turns TCP keepalive on for every connection it accepts: the
kernel probes a connection silent for 30 s every 10 s and closes it after
60 s without an answer, so that a passive peer, or one that has not sent
its welcome, whose host or network is gone, is closed as an active one
is. The peer's kernel answers the probes; it needs to do nothing.

## Protocol messages

Types 1 to 99 are the protocol's. The libraries handle them themselves,
whichever class a peer is built on, with one gap: a peer on `SyncClient` does
not answer a `PING`.

| Type | Payload | Who sends it, and what the receiver does |
|---|---|---|
| `PING` (1) | any | a peer that gets a `PING` without `references` answers (every class but `SyncClient`) with a `PING` carrying the same payload, `references` set to the request's id, or, when that echo would be over `MAX_MESSAGE_SIZE`, with a `BACKEND_ERROR` saying so; a `PING` that references something is an answer and is not answered again |
| `CONNECTION_WELCOME` (2) | `WelcomeMessage` | the handshake; the optional `routing` (a `Routing`: `any`, `all`, `last_resort`) says which rule-routed paths reach the peer, applied before anything is routed to it |
| `BACKEND_FOR_PACKET_SEARCH` (3) | `BackendForPacketSearch { packet_type }` | a client asking who handles `packet_type`; the multiplexer forwards it to every peer named by the first rule of that type whose routing takes requests (`any`), or to the last resorts when none does; each backend answers with a `PING` referencing the search's id, addressed to the client and carrying the search's payload back, or, when that echo would be over `MAX_MESSAGE_SIZE`, with a `BACKEND_ERROR` saying so |
| `HEARTBIT` (4) | empty | keep-alive, ignored |
| `DELIVERY_ERROR` (5) | `DeliveryError` | the multiplexer, to a message's `sender`, when nobody received it: `packet_id` names the message; `failed_type` lists the peer types with no receiver, or `failed_to` the missing instance id, or `is_known_type` false for an unknown type; `original_message` is included only if the rule asked for it, and left out, `original_message_omitted` set, when the report would be over `MAX_MESSAGE_SIZE`. `references` is the failed message's id |
| `RECORDING_CONTROL` (6) | `RecordingControl` | a peer, without `to`, asking the multiplexer it is connected to for START, STOP, STATUS, TAP or UNTAP of its recording ([operations](operations.md#recording-on-demand-over-the-protocol)); START refused unless the multiplexer allows file sessions (`--recording-dir`), TAP unless it allows taps (`--allow-tap`); STATUS and STOP carried out on any, a STOP closing a `--record` session too |
| `RECORDING_STATUS` (7) | `RecordingStatus` | the multiplexer's answer, `references` the request's id, `error` set when it was refused |
| `RECORDING_RECORD` (8) | `Record` | the multiplexer, to every peer that tapped in, one per record, `multiplexer_id` set |
| `RULES_CONTROL` (9) | `RulesControl` | a peer, without `to`, asking the multiplexer it is connected to for RELOAD of its rules file, or its STATUS ([operations](operations.md#changing-the-rules)) |
| `RULES_STATUS` (10) | `RulesStatus` | the multiplexer's answer, `references` the request's id: the fingerprint, path and counts of the rules in use, `error` set when a reload was refused |
| `PEER_CONTROL` (11) | `PeerControl { routing }` | a peer, without `to`, telling the multiplexer it is connected to which rule-routed paths reach it from now on, replacing what its welcome said ([how a backend leaves](leaving.md#what-a-draining-backend-still-takes)) |
| `PEER_STATUS` (12) | `PeerStatus` | the multiplexer's answer, `references` the request's id, once the routing is in effect: `routing` as applied, `error` set when the request was ignored; queued after everything routed to the peer before the change |

A `PING`, a `RECORDING_RECORD` or a status sent without `to`, which nobody
could receive, is answered with a `DELIVERY_ERROR` and recorded as a type
of the rules file with no rule is; so is a message of type 0 or 13 to 99,
numbers the protocol keeps and does not use, as a type with no entry, with
`is_known_type` false. A `DELIVERY_ERROR` without `to` is dropped, never
answered with another.

## What the multiplexer does with a frame

In this order, the first that applies wins:

1. `to` set: deliver to that connection, or report `failed_to`.
2. `override_rrules` present: apply those rules.
3. Type 99 or below: the protocol handling above.
4. The rules file's entries for the type: for each rule, `ANY` picks the
   next peer of the type in round robin whose queue has room, `ALL` queues
   the frame on every peer of the type. No receiver for a rule that has
   `report_delivery_error` produces a `DELIVERY_ERROR`.
5. No entry for the type: drop, and report `is_known_type` false. An entry
   with no rule and no `to`: drop, and report `is_known_type` true.

The multiplexer forwards the frame it received, bytes unchanged; it does not
re-serialize, so anything in the body survives, including fields it does not
know.
