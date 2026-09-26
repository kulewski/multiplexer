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
- `is_passive`: true for peer types built on the synchronous `Client`,
  which runs the library's loop only inside calls. The multiplexer then
  does not expect heartbeats from them and does not drop them for
  silence. Default false, which is right for both backend classes,
  `ThreadedClient` and `AsyncClient`, all of which run the loop all the
  time; a deployment built on those never needs the mark.

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
  of the type. Default `ANY`. The sender is not left out: a peer that sends
  a message routed to its own type gets a copy of it with `ALL`, and may
  get it with `ANY` when its turn comes.
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
that announces a type in that range is refused, the two controllers below
apart; the backend classes treat a message type in that range as internal
and never pass it to `handle_message`.

| Peer type | Value | Meaning |
|---|---|---|
| `MULTIPLEXER` | 1 | what a multiplexer announces in its welcome |
| `ALL_TYPES` | 2 | in a rule: every peer type |
| `RECORDING_CONTROLLER` | 3 | a peer that drives recording; accepted, as passive, only by a multiplexer started with `--recording-dir` or `--allow-tap`; defined in `Recording.proto`, not in the rules file |
| `RULES_CONTROLLER` | 4 | what `mxcontrol rules` connects as; accepted, as passive, by every multiplexer; defined in `Multiplexer.proto`, not in the rules file |
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
| `RULES_CONTROL` | 9 | a peer asks a multiplexer to read its rules file again, or which rules it has in use |
| `RULES_STATUS` | 10 | the multiplexer's answer |
| `PEER_CONTROL` | 11 | a peer tells a multiplexer which rule-routed paths reach it from now on, what a draining backend uses |
| `PEER_STATUS` | 12 | the multiplexer's answer, once in effect |
| `MAX_MULTIPLEXER_META_PACKET` | 99 | end of the reserved range |

The three recording types, the two rules types and the two peer types are
defined in `Recording.proto` and `Multiplexer.proto` and handled by the
multiplexer whatever the rules file says; the shipped rules files name
them so that dumps and logs show names.

The libraries also use two ordinary types by name, so keep them in every rules
file: `REQUEST_RECEIVED`, which a backend built on `BaseMultiplexerServer`
may send with `notify_start()` to say it is working on a request, and
`BACKEND_ERROR`, which the Python backend classes send when `handle_message`
raised. A Bazel build takes their numbers from your file. A program installed from a release does not: the wheel and the
Debian package carry the example file's constants, so there
`REQUEST_RECEIVED` must stay 113 and `BACKEND_ERROR` 114, and no type of
yours may take either number; a reply of yours numbered 114 would be raised
as `BackendError`.

Three more are needed by a Bazel build pointed at your file, not only by the
programs that use them: `mxcontrol` is compiled with every subcommand, so
`@mx//mxcontrol` needs `LOG_STREAMER`, `LOG_RECEIVER_EXAMPLE` and
`LOGS_STREAM`, which its two log commands name, and the Python backend
modules read `PICKLE_RESPONSE` when they are imported, so every Python
program that imports them needs it, not only the `MultiplexerServer` that
exchanges pickles. Everything else in the example file, the
`PYTHON_TEST_*` and `*_COLLECTOR` entries, is there as illustration and can
go.

## Checks

At build time `generate_constants`, and `mxcontrol generate_constants` for
a build outside Bazel, refuses a file where a name or a number repeats. At
start the multiplexer refuses a rule whose `peer` names no peer type, a
file that does not parse, one without a peer type, or one that repeats a
number or a peer name, and stops; a running multiplexer given such a file
keeps the rules it has and says so ([changing the
rules](operations.md#changing-the-rules)). A message whose type has no
entry is dropped at run time and reported as a delivery error with
`is_known_type` false.

## Pointing a build at your file

The build reads the file named by the flag `//:multiplexer_rules`, which
defaults to the example in this repository:

```
bazel build --@mx//:multiplexer_rules=//your/pkg:deployment.rules //...
```

Put it in your `.bazelrc` so nobody forgets, as
[examples/echo/.bazelrc](../examples/echo/.bazelrc) does. Inside this
repository the flag is spelled `--//:multiplexer_rules=`. Changing the file
regenerates the constants on the next build; a running multiplexer reads
the file again on its own and puts the change in use, see [changing the
rules](operations.md#changing-the-rules).

## Messages that carry their own rules

A message can bypass the file: a set `to` field delivers to that instance id
and nothing else, and `override_rrules` on a message holds routing rules, by
peer type number, that replace the file's rules for that message. The
libraries have no call for the second one, but a message built by hand can
carry it.
