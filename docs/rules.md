# The rules file

The rules file names every peer type and every message type, and says where
each message type goes. One file serves a whole deployment: every multiplexer
reads it at start, and every build of a peer generates its constants from it.

It is a protocol buffer in text format, message `MultiplexerRules` in
[Multiplexer.proto](../multiplexer/Multiplexer.proto). `#` starts a comment.
The file in this repository, [multiplexer.rules](../multiplexer.rules), is an
example to start from; [examples/echo/echo.rules](../examples/echo/echo.rules)
extends it with two peer types and two message types.

## A peer type

```
peer {
    type: 300
    name: "ECHO_BACKEND"
    comment: "answers ECHO_REQUEST"
    queue_size: 1024
    is_passive: false
}
```

- `type`: the number a peer sends in its welcome message. Unique among peer
  types. 1 to 99 are reserved; use 100 or more.
- `name`: the constant generated for it, so `peers.ECHO_BACKEND` in Python and
  `multiplexer::peers::ECHO_BACKEND` in C++. Unique among peer types. Rules
  refer to peer types by this name.
- `comment`: optional, for the reader.
- `queue_size`: how many messages the multiplexer will hold for one
  connection of this type before dropping new ones. Default 1024.
- `is_passive`: true for clients, which run the library's loop only inside
  calls. The multiplexer then does not expect heartbeats from them and does
  not drop them for silence. Default false, which is right for backends.

## A message type

```
type {
    type: 300
    name: "ECHO_REQUEST"
    comment: "payload is the text to upper-case"
    to {
        peer: "ECHO_BACKEND"
        whom: ANY
    }
}
```

- `type`: the number in every message of this type. Unique among message
  types. 1 to 99 are reserved; use 100 or more.
- `name`: the constant, `types.ECHO_REQUEST` in Python and
  `multiplexer::types::ECHO_REQUEST` in C++. Unique among message types.
- `to`: zero or more routing rules, each applied to every message of the type.
  A message type with no rule, such as a reply, is only ever addressed
  directly through the `to` field of a message; one sent without `to` is
  dropped and reported to the sender with a `DELIVERY_ERROR`, like a type
  that has no entry at all.

A routing rule has these fields:

- `peer`: the name of the receiving peer type. The special name `ALL_TYPES`
  means every peer type that has a connection.
- `whom`: `ANY` delivers to one connected peer of the type, round robin,
  skipping peers whose queue is full. `ALL` delivers to every connected peer
  of the type. Default `ANY`.
- `report_delivery_error`: when no peer received the message, send the sender
  a `DELIVERY_ERROR`. Default true. Requests need it; the client's `query()`
  starts its search on that report instead of waiting out its timeout.
- `include_original_packet_in_report`: put the whole undelivered message
  inside the `DELIVERY_ERROR`. Default false.
- `delivery_error_is_error`: only changes the level at which the multiplexer
  logs an undelivered message, error or warning. Default true.

The client's search for a backend (`BACKEND_FOR_PACKET_SEARCH`) uses the first
rule of the request type, with `whom` forced to `ALL`, so put the rule that
names the backends first.

## What the reserved ranges hold

Peer types 1 to 99 and message types 1 to 99 belong to the protocol. A peer
that announces a type in that range is refused; a backend treats a message
type in that range as internal and never passes it to `handle_message`.

| Peer type | Value | Meaning |
|---|---|---|
| `MULTIPLEXER` | 1 | what a multiplexer announces in its welcome |
| `ALL_TYPES` | 2 | in a rule: every peer type |
| `RECORDING_CONTROLLER` | 3 | a peer that drives recording; accepted, as passive, only by a multiplexer started with `--recording-dir` or `--allow-tap`; defined in `Recording.proto`, not in the rules file |
| `MAX_MULTIPLEXER_SPECIAL_PEER_TYPE` | 99 | end of the reserved range |

| Message type | Value | Meaning |
|---|---|---|
| `PING` | 1 | answer to a search; also an echo request between peers |
| `CONNECTION_WELCOME` | 2 | the handshake |
| `BACKEND_FOR_PACKET_SEARCH` | 3 | a client looking for a backend |
| `HEARTBIT` | 4 | keeps a connection alive |
| `DELIVERY_ERROR` | 5 | nobody received a message |
| `RECORDING_CONTROL` | 6 | a peer asks a multiplexer to start, stop or report its recording, or to tap in |
| `RECORDING_STATUS` | 7 | the multiplexer's answer |
| `RECORDING_RECORD` | 8 | one record streamed to a peer that tapped in |
| `MAX_MULTIPLEXER_META_PACKET` | 99 | end of the reserved range |

The three recording types are defined in `Recording.proto` and handled by
the multiplexer whatever the rules file says; the shipped rules files name
them so that dumps and logs show names.

The libraries also use two ordinary types by name, so keep them in every rules
file: `REQUEST_RECEIVED`, which a backend may send with `notify_start()` to say
it is working on a request, and `BACKEND_ERROR`, which the Python backend
sends when `handle_message` raised. `PICKLE_RESPONSE` is needed only by the
Python `MultiplexerServer` that exchanges pickles. `LOG_STREAMER`,
`LOG_RECEIVER_EXAMPLE` and `LOGS_STREAM` are needed only by the two log
commands of `mxcontrol`. Everything else in the example file, the
`PYTHON_TEST_*` and `*_COLLECTOR` entries, is there as illustration and can
go.

## Checks

At build time `generate_constants` refuses a file where a name or a number
repeats. At start the multiplexer refuses a rule whose `peer` names no peer
type, and stops. A message whose type has no entry is dropped at run time and
reported as a delivery error with `is_known_type` false.

## Pointing a build at your file

The build reads the file named by the flag `//:multiplexer_rules`, which
defaults to the example in this repository:

```
bazel build --@mx//:multiplexer_rules=//your/pkg:deployment.rules //...
```

Put it in your `.bazelrc` so nobody forgets, as
[examples/echo/.bazelrc](../examples/echo/.bazelrc) does. Inside this
repository the flag is spelled `--//:multiplexer_rules=`. Changing the file
regenerates the constants on the next build; a running multiplexer keeps the
rules it started with, so restart it.

## Messages that carry their own rules

A message can bypass the file: a set `to` field delivers to that instance id
and nothing else, and `override_rrules` on a message holds routing rules, by
peer type number, that replace the file's rules for that message. The
libraries have no call for the second one, but a message built by hand can
carry it.
