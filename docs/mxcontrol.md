# mxcontrol

`mxcontrol` is the one binary this repository installs: it runs the
multiplexer and a few tools next to it. Build it with `bazel build
//mxcontrol`, or `bazel build @mx//mxcontrol` from a workspace that consumes
the repository. It is a small tool with subcommands:

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
| `--verbosity NAME` | the most verbose `DEBUG` entries still emitted: `ZEROVERBOSITY` (none), `LOWVERBOSITY`, `MEDIUMVERBOSITY` (default), `HIGHVERBOSITY` or `CHATTERBOX`; the other levels are always emitted |

The log always goes to stderr as text as well, one entry per line: level,
timestamp, pid, context, workflow id, text, and the source location.

## run_multiplexer

Runs one multiplexer.

```
mxcontrol run_multiplexer [--rules FILE] [--address HOST:PORT] [--port-file PATH]
                          [--peers-file PATH] [--record PATH] [--record-payload-bytes N]
```

| Option | Default | Effect |
|---|---|---|
| `--rules FILE` | `multiplexer.rules` in the current directory | the [rules file](rules.md) to read once at start |
| `--address HOST:PORT`, `-M`, or the first positional argument | `0.0.0.0:1980` | the address to listen on; `HOST` alone keeps port 1980 |
| `--port-file PATH` | none | after binding, write `host:port` to this file, atomically |
| `--peers-file PATH` | none | keep this file listing every connected peer, one `<instance id> <type name> <type>` per line, rewritten atomically on every change |
| `--record PATH` | none | write every routed message and every peer arrival and departure to this file; see [operations](operations.md#recording) |
| `--record-payload-bytes N` | 0 (whole payloads) | keep only the first N bytes of each recorded payload |

`--address` takes an IP address, not a host name. With port 0 the system picks
a free port; together with `--port-file` that lets a test or a supervisor
learn where the multiplexer listens without guessing, which is how the
integration tests start theirs.

The multiplexer runs until it gets `SIGINT` or `SIGTERM`, then closes the
listening socket and every connection and exits with 0. An exception thrown
while handling one connection is logged and the process keeps serving.

Each peer that connects is logged at `INFO` with its instance id and peer
type, and again when it leaves. A message nobody could receive is
logged at `ERROR`, or `WARNING` if the rule says so, and a full queue is
logged at `WARNING` for every message dropped.

## dump_recording

Prints a recording made with `run_multiplexer --record`, one line per
record.

```
mxcontrol dump_recording FILE [--rules FILE] [--type N] [--peer ID]
```

| Option | Effect |
|---|---|
| `--rules FILE` | the rules file the multiplexer ran with, for peer and message type names instead of numbers |
| `--type N` | only routed messages of this type |
| `--peer ID` | only records involving this instance id, as sender, recipient or the peer arriving or leaving |

The Python reader, `multiplexer.recording`, prints the same with names from
the generated constants: `bazel run @mx//multiplexer:dump_recording -- FILE`.

## streamlogs

```
mxcontrol streamlogs [--multiplexer [HOST]:PORT ...] [--chunksize N]
```

Reads a binary log stream, as written by `--logging-file`, from stdin, and
sends it as `LOGS_STREAM` messages through every listed multiplexer, `N`
entries per message, 32 by default and 0 for everything in one message. It
connects as the peer type `LOG_STREAMER`. `--multiplexer` may be repeated;
the host defaults to `127.0.0.1`.

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
