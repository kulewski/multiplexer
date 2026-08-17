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
| `id` | 1 | required in practice: a receiver drops a message without one, and uses it to drop a copy it has already seen. A client that sends a request again, after a timeout, a search or a lost connection, gives the new attempt a new id and accepts replies to any of them |
| `from` | 2 | the sender's instance id; the multiplexer sends delivery errors to it |
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
instance id it will use as `from`. Any other first message closes the
connection, as does a peer type that is 99 or below or absent from the rules
file. The multiplexer registers the connection, then answers with its own
`CONNECTION_WELCOME`: type `MULTIPLEXER` (1) and its instance id. A second
welcome on the same connection closes it.

The `id` is the peer's own choice. Two connections announcing the same id
from the same host replace each other, the newer one winning; from different
hosts the second is refused. [Connecting to a multiplexer](handshake.md)
draws the exchange.

## Heartbeats

`HEARTBIT` (4) with an empty payload, sent every 3 s on an otherwise idle
connection by both sides. The multiplexer expects some frame from a
non-passive peer within 30 s, then grants 60 s more before closing the
connection. Any frame counts; a peer that keeps sending real messages need
not send heartbeats. To a passive peer the multiplexer sends at most one
heartbeat per frame received and expects none.

## Protocol messages

Types 1 to 99 are the protocol's. A backend library answers them itself.

| Type | Payload | Who sends it, and what the receiver does |
|---|---|---|
| `PING` (1) | any | a peer that gets a `PING` without `references` answers with a `PING` carrying the same payload, `references` set to the request's id; a `PING` that references something is an answer and is not answered again |
| `CONNECTION_WELCOME` (2) | `WelcomeMessage` | the handshake |
| `BACKEND_FOR_PACKET_SEARCH` (3) | `BackendForPacketSearch { packet_type }` | a client asking who handles `packet_type`; the multiplexer forwards it to every peer named by the first rule of that type; each backend answers with a `PING` referencing the search's id, addressed to the client |
| `HEARTBIT` (4) | empty | keep-alive, ignored |
| `DELIVERY_ERROR` (5) | `DeliveryError` | the multiplexer, to a message's `from`, when nobody received it: `packet_id` names the message; `failed_type` lists the peer types with no receiver, or `failed_to` the missing instance id, or `is_known_type` false for an unknown type; `original_message` is included only if the rule asked for it. `references` is the failed message's id |

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
