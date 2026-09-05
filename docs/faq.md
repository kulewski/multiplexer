# Questions people ask

**Why must a client be passive?**
The synchronous `Client` runs its event loop only inside calls, so between
calls it can neither send heartbeats nor notice a closed socket. A
multiplexer that expected heartbeats from it would drop it after 90 s of
not being called. Marking the peer type `is_passive` tells the multiplexer
to expect nothing. `ThreadedClient` has no such limit: its io thread runs
all the time, so its peer type can be an ordinary, active one, and it
reconnects on its own. Use `is_passive` for peers built on `Client` only.

**Why do the multiplexers not talk to each other?**
Because then nothing has to agree. Each one routes on its own from the same
rules file, so losing one loses nothing but its connections, and adding one
is starting a process. The price is paid by the peers: every peer connects
to every multiplexer, and an event that must reach everyone either trusts
one multiplexer or is sent through all of them and deduplicated by id.

**Why is there no authentication, encryption or access control?**
It is designed for a trusted internal network, where the listening port is
reachable only from hosts you control. Any peer that can connect is trusted
to be what it declares. Restrict reachability at the network layer, or
tunnel the connection over TLS when the network is shared.

**Why does a query search for a backend instead of just failing?**
Because the common failure is one backend dying with a request in its hands.
The multiplexer has already delivered the request, so it cannot know it was
lost; the client can, by its timeout. Rather than fail the call, the client
asks every multiplexer who else handles the type and repeats the request to
the first backend that answers. The caller sees a slow call instead of an
error, which is the right outcome for a pool of interchangeable workers.

**Can a backend receive the same request twice?**
Yes. The original backend may have handled it and died before replying, or
the connection may have died with the request on the wire; the client sends
it again either way. Each attempt is a new message with a new `id`, so the
backend cannot recognise the repeat by id. Put a key of your own in the
payload when a request must not be applied twice.

**Why is a message's payload opaque bytes?**
So that the multiplexer never has to be rebuilt for your types. It routes on
the type number alone. Most peers put protocol buffers in the payload, and
the rules file is a protocol buffer in text format, but that is a habit, not
a requirement.

**Why is `from` called `from_` in Python?**
`from` is a keyword. The generated protocol buffer class has the field under
its real name, reachable with `getattr`; the library adds `from_` as a
property so that code reads normally.

**What is a good message size?**
Anything up to 128 MiB is accepted, and a 1 MiB message is exercised by the
tests. The multiplexer copies nothing, but it holds every message queued for
a slow receiver in memory, so keep large messages off `whom: ALL` types and
give their receivers a small `queue_size`.

**Can a backend send requests of its own?**
It can, but not well. Its connection is a `Client` (`self.conn` in Python,
`conn` in C++, and the second C++ constructor lets you supply your own), so
`query()` works from inside `handle_message`. While that nested query waits
for its reply, though, every other message that arrives on the backend's
connections is logged and dropped rather than kept for later, so concurrent
requests to that backend are lost. Sending an event from a backend is fine.
For a service that both serves and asks, run the asking side as a separate
client process, or make the backend's requests events answered by events.

**Why does the library not handle SIGTERM?**
Because in a process that mixes Python and C++ a signal handler is not a
reliable way to learn anything. A Python handler runs only when the main
thread next executes Python code; while the thread is inside the C++ client
or a long handler, the signal waits, and on an idle backend in a receive
with no timeout it waits forever. And there is exactly one handler per
signal per process: a C++ library in the same binary that installs its own
replaces Python's without a trace. The loop therefore polls, and a backend
notices a request to leave from `periodic_task()`, by a file a preStop hook
wrote or a flag another thread set, within one poll. A pure C++ backend may
still set a `sig_atomic_t` from a handler of its own and read it there.

**Can a backend be threaded too?**
Not yet. A backend built on `ThreadedClient` would run its handlers on a
worker pool while the io thread keeps the heartbeats going, so a slow
handler would no longer look like a dead peer to the multiplexer, and one
backend could serve several requests at once. The pieces exist: the io
thread, the receive queue, `send()` from any thread. What is missing is the
base class and the reply defaults, and a decision on ordering, since
requests handled in parallel are no longer answered in order. It is the
natural next step after the threaded client.

**Why Bazel?**
The rules file has to produce the same constants in C++ and Python in one
step, the multiplexer and both libraries share one C++ core, and a consuming
project needs all of that without installing anything. Bazel gives that with
a two-line `WORKSPACE` addition; see [examples/README.md](../examples/README.md).

**What does "mx" stand for?**
Multiplexer. It is the short name in code, targets and commands; the prose
here says multiplexer.
