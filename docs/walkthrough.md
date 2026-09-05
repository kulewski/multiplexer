# Walkthrough: the echo example

This page runs one multiplexer, one backend and one client on your machine
and shows what each of them prints. Everything comes from
[examples/echo](../examples/echo), a separate Bazel workspace that consumes
the multiplexer the way your own project would.

You need Bazel 6.2 (through Bazelisk), a C++17 compiler,
`protobuf-compiler`, `libprotobuf-dev`, Python 3.10 or newer with
`python3-dev` and `python3-protobuf`; [building.md](building.md) has the
exact packages. Boost and pybind11 are fetched by Bazel on the first build,
which takes a few minutes; every later build is quick.

Open three terminals in `examples/echo`.

## Terminal 1: the multiplexer

```
bazel run @mx//mxcontrol -- run_multiplexer --address 127.0.0.1:1980 --rules $PWD/echo.rules
```

The multiplexer reads the rules file and starts listening. Its log goes to
stderr, one line per entry:

```
[INFO]  ts=1788564736  pid=2005459  ctx=host.mxcontrol.run_multiplexer.ConnectionsManager  flw=""  txt="created new ConnectionsManager with id 5397548489707036824"  from=external/mx/multiplexer/connections_manager.h:48
[DEBUG]  ts=1788564736  pid=2005459  ctx=host.mxcontrol.run_multiplexer.config  flw=""  txt="reading configuration file"  from=external/mx/multiplexer/config.h:64
[INFO]  ts=1788564736  pid=2005459  ctx=host.mxcontrol.run_multiplexer  flw=""  txt="starting MX server on 127.0.0.1:1980"  from=external/mx/mxcontrol/start_multiplexer_server.cc:64
```

The big number is the multiplexer's instance id. Leave it running.

## Terminal 2: a backend

```
bazel run //:backend_py
```

The backend connects, completes the handshake and prints `ready`. Terminal 1
shows the other side of that handshake, with the backend's own instance id and
its peer type from the rules file:

```
[INFO]  ...  txt="registered connection id=7947811977285246526 type=300 ('ECHO_BACKEND')"  ...
```

`bazel run //:backend_cc` starts the same backend written in C++; either one
will do, and you can run both.

## Terminal 3: a client

```
bazel run //:client_py -- 127.0.0.1:1980 "hello multiplexer"
```

The client connects, sends one `ECHO_REQUEST`, waits for the reply and prints
it. The lines in brackets are its library's log on stderr; the answer is the
last line:

```
[INFO]  ...  txt="connecting to 127.0.0.1:1980"  ...
[INFO]  ...  txt="registered connection id=5397548489707036824 type=1 ('MULTIPLEXER')"  ...
HELLO MULTIPLEXER
```

The `registered connection` line names the multiplexer's instance id, the same
number terminal 1 printed at start. `bazel run //:client_cc -- 127.0.0.1:1980
"hello multiplexer"` does the same from C++.

## What just happened

1. The client's `query()` wrapped the text in a message of type `ECHO_REQUEST`
   and sent it to the multiplexer.
2. The rules file says `ECHO_REQUEST` goes to one `ECHO_BACKEND`, so the
   multiplexer forwarded it to the backend.
3. The backend's `handle_message` upper-cased the payload and called
   `send_message`, which addressed the reply to the client's instance id and
   made it reference the request's id.
4. The multiplexer delivered the reply straight to the client, and `query()`
   returned it because the id matched.

[How a query is answered](query.md) draws these steps; [routing](routing.md)
explains the rule.

## Things to try

- **Two backends.** Start `backend_cc` in a fourth terminal and run the client
  a few times. Terminal 1 logs a second `ECHO_BACKEND`; requests alternate
  between the two, round robin. Stop one of them and the client keeps working.
- **No backend.** Stop both backends and run the client. It fails at once with
  `OperationFailed`: the multiplexer reported that nobody could take the
  request, the client searched for a backend, and nobody answered. Nothing
  waits for a timeout.
- **No multiplexer.** Stop the multiplexer and run the client. The connect
  attempt does not fail on its own; the query keeps trying to reconnect for
  its timeout, 10 s, and then fails with `NotConnected`. Start the
  multiplexer again within those seconds and the query goes through. A real
  client lists several multiplexers, so one that is down costs nothing.
- **A rule of your own.** Add a peer type and a message type to `echo.rules`,
  as described in [the rules file](rules.md), rebuild, and the generated
  constants carry your names in both languages.

## Where to go next

- [The rules file](rules.md): every field, what the reserved ranges are for.
- [Using the Python library](api_python.md) and
  [Using the C++ library](api_cpp.md): the calls the example used and the
  rest of them.
- [Operations](operations.md): several multiplexers, restarts, logs.
