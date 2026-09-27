# mxcontrol

`mxcontrol` runs the multiplexer and a few tools next to it. Every way of
getting the multiplexer brings it: `pip install mx-multiplexer` puts the
`mxcontrol` command on the environment's PATH, `python -m
multiplexer.mxcontrol` being the same without PATH; every release has a
static one for any x86_64 Linux and an image that runs it; the Debian
packages install it in `/usr/bin`; and a build makes it, `bazel build
//mxcontrol`, `bazel build @mx//mxcontrol` from a workspace that consumes
the repository, or `make`, into `build/bin/mxcontrol`
([packaging](packaging.md)). It is a small tool with subcommands:

```
mxcontrol <general options> <command> <command options>
```

`mxcontrol help` lists the commands; `mxcontrol help <command>` prints that
command's options. Every command exits with 0 on success.

## General options

| Option | Effect |
|---|---|
| `--help` | print help for the command that follows |
| `--logging-file PATH` | write the log as a binary stream of `LogEntry` records to this file |
| `--logging-fd N` | write the same stream to an already open file descriptor instead |
| `--verbosity NAME` | the most verbose `DEBUG` entries still emitted: `ZEROVERBOSITY` (none), `LOWVERBOSITY`, `MEDIUMVERBOSITY` (default), `HIGHVERBOSITY` or `CHATTERBOX`; the other levels are always emitted. The environment variable `MX_LOG_VERBOSITY` ([operations](operations.md#logs)) replaces the default; an explicit flag wins over it |

The log always goes to stderr as text as well, one entry per line: level,
timestamp, pid, context, workflow id, text, and the source location.

## run_multiplexer

Runs one multiplexer.

```
mxcontrol run_multiplexer [--rules FILE] [--rules-check-interval S] [--address HOST:PORT]
                          [--port-file PATH] [--peers-file PATH] [--record PATH]
                          [--record-payload-bytes N] [--recording-dir DIR] [--allow-tap]
```

| Option | Default | Effect |
|---|---|---|
| `--rules FILE` | `multiplexer.rules` in the current directory | the [rules file](rules.md), read at start and again whenever it changes |
| `--rules-check-interval S` | 2 | seconds between reads of the rules file; a changed file is put in use without a restart once two reads in a row saw the same new bytes, see [changing the rules](operations.md#changing-the-rules); 0 never reads it again (`SIGHUP` and `mxcontrol rules reload` still do) |
| `--address HOST:PORT`, `-M`, or the first positional argument | `0.0.0.0:1980` | the address to listen on; `HOST` alone keeps port 1980 |
| `--port-file PATH` | none | after binding, write `host:port` to this file, atomically |
| `--peers-file PATH` | none | keep this file listing every connected peer, one `<instance id> <type name> <type>` per line, rewritten atomically on every change |
| `--record PATH` | none | write every routed message and every peer arrival and departure to this file; see [operations](operations.md#recording) |
| `--record-payload-bytes N` | 0 (whole payloads) | keep only the first N bytes of each recorded payload |
| `--recording-dir DIR` | off | let peers start and stop recording sessions over the protocol, written under `DIR`; see [recording on demand](operations.md#recording-on-demand-over-the-protocol) |
| `--allow-tap` | off | let peers receive every record over their connection |
| `--drain-seconds S` | 5 | on `SIGTERM` or `SIGINT`, how long to go on routing and sending what is queued before each connection closes; 0 stops at once |

`--address` takes an IP address, not a host name. With port 0 the system picks
a free port; together with `--port-file` that lets a test or a supervisor
learn where the multiplexer listens without guessing, which is how the
integration tests start theirs.

The multiplexer runs until it gets `SIGINT` or `SIGTERM`. Then it closes
the listening socket, and every connection that has not introduced itself
yet. Every other connection goes on as before, read, routed and routed to,
until what is queued for it is written; then it closes the polite way:
the multiplexer's end of the stream follows everything written, and what
the peer still sends is read and dropped until the peer's own end, 1 s at
most. A peer that does not read by `--drain-seconds` loses what is still
queued for it, and its connection is closed. Once every connection has
ended, the process exits with 0, within `--drain-seconds` and a second; a
second `SIGINT` or `SIGTERM` stops it at once, as does `--drain-seconds 0`.
Its last line says how long the stop took and what it dropped, if
anything. `SIGHUP` makes it read the rules file again now, and it no longer
exits on one. An exception thrown while handling one connection is logged
and the process keeps serving. So does a log reader that goes away, the
other end of `--logging-fd` or of stderr: the multiplexer ignores
`SIGPIPE`, drops the binary stream with a `WARNING`, and serves on.

Each peer that connects is logged at `INFO` with its instance id and peer
type, and again when it leaves. A message nobody could receive is
logged at `ERROR`, or `WARNING` if the rule says so, and a message a full
queue drops at `WARNING`: the first of a kind at once, the rest as one
line a second with a count ([logs](operations.md#logs)).

## generate_constants

Writes the peer and message types of a rules file as constants, for a
program built outside Bazel: the Python module (classes `peers` and
`types`), its stub for type checkers, and the C++ header (namespaces
`multiplexer::peers` and `multiplexer::types`), whichever are asked for.
The files are the ones a Bazel build generates from the same rules file,
byte for byte; the header's include guard comes from the rules file's
fingerprint, so the same rules give the same header in every build.

```
mxcontrol generate_constants RULES [--python FILE] [--pyi FILE] [--cxx FILE]
```

| Option | Effect |
|---|---|
| `RULES` | the [rules file](rules.md) |
| `--python FILE` | write the Python module, `multiplexer_constants.py` by convention, importable as `multiplexer_constants` from wherever it is put |
| `--pyi FILE` | write the module's type stub, next to the module |
| `--cxx FILE` | write the C++ header, as `multiplexer/multiplexer.constants.h` on the include path before the installed package's copy |

A rules file where a name or a number repeats is refused, as at build time.
A program installed from a release, with `pip install mx-multiplexer` or
the Debian package, runs this once per rules file and again when the file
changes; a Bazel build does it on its own.

## generate_rules

Writes the system rules, the peer and message types the multiplexer, the
libraries and `mxcontrol` use themselves, as a new rules file to add a
deployment's own types to ([the rules file](rules.md)). The text is
compiled into `mxcontrol`, so every one writes the rules it was built with,
the same in every artifact of a release.

```
mxcontrol generate_rules FILE
```

FILE must not exist: a rules file is edited after it is written, so an
existing one is never replaced, and the command exits 1 instead. With
FILE `-` the rules go to stdout, which is how the image writes them:
`docker run --rm ghcr.io/kulewski/multiplexer:<version> generate_rules - >
your.rules`.

## dump_recording

Prints recordings, one line per record; several files, the sessions of
several multiplexers or the output of a tap, are merged by time, each line
marked with the multiplexer that wrote it.

```
mxcontrol dump_recording FILE... [--rules FILE] [--type N] [--peer ID]
```

| Option | Effect |
|---|---|
| `--rules FILE` | the rules file the multiplexer ran with, for peer and message type names instead of numbers |
| `--type N` | only routed messages of this type |
| `--peer ID` | only records involving this instance id, as sender, recipient or the peer arriving or leaving |

A recording of any length is read whole. A file that ends in a record cut
short, or holds one it cannot read, is printed up to that record, which is
said on stderr, and the command exits with 1 once the other files are done.

The Python reader, `multiplexer.recording`, prints the same with names from
the generated constants: `bazel run @mx//multiplexer:dump_recording -- FILE...`,
several files merged by time.

## recording

Starts, stops, queries or taps the recording of running multiplexers over
the protocol; they must have been started with `--recording-dir` or
`--allow-tap` ([operations](operations.md#recording-on-demand-over-the-protocol)).

```
mxcontrol recording start|stop|status|tap -M HOST:PORT [-M ...] [options]
```

| Option | Effect |
|---|---|
| `-M`, `--multiplexer HOST:PORT` | a multiplexer to reach; repeatable; a host name resolves to every address it has, one connection each |
| `--type N` | the peer type to connect as; default the reserved `RECORDING_CONTROLLER`, which needs no rules entry |
| `--label NAME` | `start`: the session's name in the file name; letters, digits, `-`, `_`; default `session` |
| `--payload-bytes N` | `start`, `tap`: keep only the first N bytes of each payload; 0 keeps all |
| `--max-bytes N` | `start`: close the session at this size; default 1 GiB, 0 for no cap |
| `--max-seconds N` | `start`: close the session after this long |
| `--stay` | `start`: keep running, start the session again on any multiplexer that comes back without one, and stop every session on SIGINT or SIGTERM |
| `--out FILE` | `tap`: append the records to this file instead of stdout |
| `--timeout S` | seconds to wait for connections and answers; default 5 |

One line per multiplexer comes back, `multiplexer <id>: recording <path>
(<records> records, <bytes> bytes, label <label>)`, or `not recording`,
with the last session and why it ended, or `error: <why>`; `status` adds
the taps. The exit code is 0 when every `-M` reached a multiplexer and
every multiplexer answered without an error. `tap` writes the records as a stream in the recording's own format,
readable by `dump_recording`, and its status lines to stderr; SIGINT
untaps and exits.

```
mxcontrol recording start -M mx-0.mx.svc:1980 -M mx-1.mx.svc:1980 --label checkout-bug
mxcontrol recording status -M mx.svc:1980
mxcontrol recording tap -M mx.svc:1980 --payload-bytes 200 --out session.rec
mxcontrol recording stop -M mx.svc:1980
```

## rules

Makes running multiplexers read their rules file again, or asks which
rules each has in use, over the protocol ([changing the
rules](operations.md#changing-the-rules)).

```
mxcontrol rules reload|status -M HOST:PORT [-M ...] [--timeout S]
```

| Option | Effect |
|---|---|
| `-M`, `--multiplexer HOST:PORT` | a multiplexer to reach; repeatable; a host name resolves to every address it has, one connection each |
| `--timeout S` | seconds to wait for connections and answers; default 5 |

It connects as the reserved `RULES_CONTROLLER` type, which every
multiplexer accepts. One line per multiplexer comes back: `multiplexer
<id>: rules <fingerprint> (<n> message types, <m> peer types) from <path>,
loaded <time>`, prefixed for `reload` with `reloaded; ` when the file
differed and is in use now or `unchanged; ` when it is what was in use
already, or `error: <why>; keeps rules ...` when the file could not be put
in use. `status` adds `; the file on disk is not in use: <why>` while that
is so. The fingerprint is the CRC-32 the generated constants carry as
`RULES_FINGERPRINT`, so the lines show whether every replica runs the file
the peers were built from. The exit code is 0 when every `-M` reached a
multiplexer and every multiplexer answered without an error.

```
mxcontrol rules status -M mx-0.mx.svc:1980 -M mx-1.mx.svc:1980
mxcontrol rules reload -M mx.svc:1980
```

## streamlogs

```
mxcontrol streamlogs [--multiplexer [HOST]:PORT ...] [--chunksize N] [--timeout SECONDS]
```

Reads a binary log stream, as written by `--logging-file`, from stdin, and
sends it as `LOGS_STREAM` messages through every listed multiplexer, `N`
entries per message, 32 by default and 0 for everything in one message. It
connects as the peer type `LOG_STREAMER`. `--multiplexer` may be repeated;
the host defaults to `127.0.0.1`. It exits with 0 at the end of stdin,
however long the stream; one that breaks off, an entry cut short or
garbled, is sent up to there, logged as an error, and the command exits
with 1.

It never waits for a multiplexer, so that the program whose log it reads,
which blocks writing into the pipe once nobody reads it, never waits
either. Each message goes to every connection through a `ThreadedClient`,
whose io thread holds a copy a multiplexer does not take, one that stopped
reading or is restarting, for `--timeout` seconds, 10 by default, and then
drops it: such a multiplexer costs log entries, counted in a warning at
the end, and meanwhile memory, up to what is logged in that time. It does
not wait to connect either: what it reads in the first moment, before a
multiplexer's handshake is done, goes through those connected already, or
waits for the first.

A chunk no log receiver takes, none being connected, the multiplexers drop
and answer with a `DELIVERY_ERROR`, and `streamlogs` says so in a
`WARNING`, `no log receiver took the log stream`, the first at once and
then a count about once a second, so that a log that goes nowhere does not
go unnoticed.

## receivelogs

```
mxcontrol receivelogs [--multiplexer [HOST]:PORT ...]
```

Connects as the peer type `LOG_RECEIVER_EXAMPLE`, a backend, and prints every
message it receives on stdout in protocol buffer text format. With the example
rules file it receives what `streamlogs` sends. It is the smallest complete
backend in the repository and a handy way to watch a message type: give its
peer type a rule and point `receivelogs` at the multiplexer.

## help

```
mxcontrol help [command]
```
