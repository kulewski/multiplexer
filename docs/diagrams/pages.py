"""The step-by-step pictures: one Page per docs page, each a fixed set of
columns, boxes and arrows plus the steps that light them up. generate.py
turns these into Markdown with Mermaid; edit here to change a picture.

Layout rule (dagre): a right-column node with arrows to and from two
left-column nodes breaks the columns; draw one return path per node.
"""

from model import Column, Edge, Page, Section, Step

QUERY = Page(
    file="query.md",
    title="How a query is answered",
    intro="""
A query is a request that expects exactly one answer: `Client.query()` in
Python, `Client::query()` in C++. Almost always it is one message out and one
message back. If the backend that took the request fails, the client finds
another one and asks again. The three pictures below show the normal case, the
recovery, and what happens when no backend of the right type exists at all.

The pictures use the healthy deployment: the client is connected to both
multiplexers, and so is every backend. Every arrow is drawn in every frame; the
red ones are the current step, and grey arrows labelled "connected" are links
that carry nothing in that picture.
""",
    sections=[
        Section(
            "The fast path",
            """
Two multiplexers, two backends of the right type, everyone connected to
everyone. This is what nearly every query looks like.
""",
            columns=[
                Column("Clients", [("Q", "client")]),
                Column("Multiplexers", [("M1", "multiplexer 1"), ("M2", "multiplexer 2")]),
                Column("Backends", [("B1", "backend 1"), ("B2", "backend 2")]),
            ],
            edges=[
                Edge("Q", "M1", "request"),  # 0
                Edge("Q", "M2", "connected"),  # 1
                Edge("M1", "B1", "request"),  # 2
                Edge("M1", "B2", "connected"),  # 3
                Edge("M2", "B1", "connected"),  # 4
                Edge("M2", "B2", "connected"),  # 5
                Edge("B1", "M1", "response, to = client"),  # 6
                Edge("M1", "Q", "response"),  # 7
            ],
            steps=[
                Step(
                    "The client sends the request through one connection",
                    "The client is connected to both multiplexers and picks one of them "
                    "round robin, multiplexer 1 this time. It sends the request and waits "
                    "for a message that references the request id, ignoring "
                    "`REQUEST_RECEIVED` acknowledgements.",
                    [0],
                    ["Q"],
                ),
                Step(
                    "The multiplexer picks a backend",
                    "The rules file maps the request's type to the backend peer type with "
                    "`whom: ANY`. Both backends are connected to multiplexer 1, so it hands "
                    "the request to one of them round robin: backend 1 now, backend 2 for "
                    "the next request of this type.",
                    [2],
                    ["M1"],
                ),
                Step(
                    "The backend answers",
                    "The reply sets `to` to the client's instance id and `references` to "
                    "the request id, and goes back over the same connection it came in on. "
                    "A message with `to` set is delivered to that peer without consulting "
                    "the rules.",
                    [6, 7],
                    ["B1"],
                ),
            ],
        ),
        Section(
            "When the backend that took the request fails",
            """
Three backends this time, all connected to both multiplexers. Backend 1 dies
after receiving the request. Each step has its own full `timeout`, so a
recovery like this one costs about one timeout on top of the normal round trip.
""",
            columns=[
                Column("Clients", [("Q", "client")]),
                Column("Multiplexers", [("M1", "multiplexer 1"), ("M2", "multiplexer 2")]),
                Column("Backends", [("B1", "backend 1"), ("B2", "backend 2"), ("B3", "backend 3")]),
            ],
            edges=[
                Edge("Q", "M1", "request"),  # 0
                Edge("M1", "B1", "request"),  # 1
                Edge("Q", "M1", "search"),  # 2
                Edge("Q", "M2", "search"),  # 3
                Edge("M1", "B2", "search"),  # 4
                Edge("M2", "B3", "search"),  # 5
                Edge("B2", "M1", "PING"),  # 6
                Edge("M1", "Q", "PING, first"),  # 7
                Edge("B3", "M2", "PING"),  # 8
                Edge("M2", "Q", "PING, ignored"),  # 9
                Edge("Q", "M1", "request to backend 2"),  # 10
                Edge("M1", "B2", "request"),  # 11
                Edge("B2", "M1", "response"),  # 12
                Edge("M1", "Q", "response"),  # 13
                Edge("M2", "B2", "connected"),  # 14
            ],
            steps=[
                Step(
                    "The client sends the request",
                    "As in the fast path: one connection, multiplexer 1 here.",
                    [0],
                    ["Q"],
                ),
                Step(
                    "Multiplexer 1 hands it to backend 1",
                    "Round robin picked backend 1. The request is delivered; nothing has " "gone wrong yet.",
                    [1],
                    ["M1"],
                ),
                Step(
                    "Backend 1 fails before answering",
                    "It crashes, hangs, or is killed. If the process is gone, both "
                    "multiplexers see the closed socket and forget backend 1 at once, so no "
                    "further request will be routed to it. The client, however, is still "
                    "waiting for a reply that will never come, and gives up on this attempt "
                    "when its `timeout` runs out.",
                    [],
                    ["B1"],
                ),
                Step(
                    "The client searches every connection",
                    "It sends `BACKEND_FOR_PACKET_SEARCH`, carrying the request's type, "
                    "through all of its connections at once, and waits for the first `PING` "
                    "back.",
                    [2, 3],
                    ["Q"],
                ),
                Step(
                    "Each multiplexer asks a live backend, and each answers",
                    "A multiplexer routes the search with the request type's own rule, so "
                    "with `whom: ANY` each forwards it to one live backend of the type, "
                    "round robin. Multiplexer 1 picks backend 2 and multiplexer 2 picks "
                    "backend 3; each answers with a `PING` that references the search. The "
                    "client keeps the first `PING` to arrive, backend 2's via multiplexer 1 "
                    "here, and ignores the later one. It now knows backend 2's instance id "
                    "and a connection that leads to it.",
                    [4, 5, 6, 7, 8, 9],
                    ["B2", "B3"],
                ),
                Step(
                    "The client repeats the request directly",
                    "The same request goes out again as a new message, with a new `id`, "
                    "`to` set to backend 2's id, and through the connection the first `PING` "
                    "arrived on. Direct addressing bypasses the rules, so this cannot land on "
                    "some other backend. The client accepts a reply to either id, so a late "
                    "reply from backend 1 would still count.",
                    [10, 11],
                    ["Q"],
                ),
                Step(
                    "Backend 2 answers",
                    "The reply references the repeated request; the client accepts a reply "
                    "to either the first or the repeated one, in case the original backend "
                    "turns out to have answered late after all.",
                    [12, 13],
                    ["B2"],
                ),
            ],
        ),
        Section(
            "When no backend of that type exists",
            """
The only backend anywhere is of another type. The query cannot succeed; this
picture shows how quickly the client finds that out. A type that has no
routing rule at all is quicker still: the multiplexer answers the request
itself with `DELIVERY_ERROR` marked `is_known_type`, and the client raises
`OperationFailed` without searching.
""",
            columns=[
                Column("Clients", [("Q", "client")]),
                Column("Multiplexers", [("M1", "multiplexer 1"), ("M2", "multiplexer 2")]),
                Column("Backends", [("B0", "backend, other type")]),
            ],
            edges=[
                Edge("Q", "M1", "request"),  # 0
                Edge("M1", "Q", "DELIVERY_ERROR"),  # 1
                Edge("Q", "M1", "search"),  # 2
                Edge("Q", "M2", "search"),  # 3
                Edge("M1", "Q", "DELIVERY_ERROR"),  # 4
                Edge("M2", "Q", "DELIVERY_ERROR"),  # 5
                Edge("M1", "B0", "connected"),  # 6
                Edge("M2", "B0", "connected"),  # 7
            ],
            steps=[
                Step(
                    "The client sends the request",
                    "One connection, as always.",
                    [0],
                    ["Q"],
                ),
                Step(
                    "The multiplexer has nobody to give it to",
                    "No connected peer has the type the rule names, so multiplexer 1 "
                    "answers at once with `DELIVERY_ERROR`. Requests ask for that report; "
                    "without it the client would have had to wait out its timeout.",
                    [1],
                    ["M1"],
                ),
                Step(
                    "The client searches anyway",
                    "The search goes out on every connection, in case some other "
                    "multiplexer has a backend of the type.",
                    [2, 3],
                    ["Q"],
                ),
                Step(
                    "Every multiplexer says no",
                    "Each answers the search with `DELIVERY_ERROR`. Once every connection "
                    "has failed, the query fails: `OperationFailed` in Python, right away "
                    "rather than after a timeout.",
                    [4, 5],
                    ["Q"],
                ),
            ],
        ),
        Section(
            "An addressed query",
            """
A request with `to` set, the instance id of one peer, is an addressed query:
only that peer ever gets it, never another instance of its type. It has the
same three stages, with the middle one locating that instance instead of
searching for any backend, and one `timeout` covering all three. The
picture shows the moment that needs the middle stage: multiplexer 1 has
just restarted, the client is back on it, and the addressee is not yet.
Two instances of the type exist; the request is for instance 2.
""",
            columns=[
                Column("Clients", [("Q", "client")]),
                Column("Multiplexers", [("M1", "multiplexer 1"), ("M2", "multiplexer 2")]),
                Column("Backends", [("B1", "instance 1"), ("B2", "instance 2")]),
            ],
            edges=[
                Edge("Q", "M1", "request, to: instance 2"),  # 0
                Edge("M1", "Q", "DELIVERY_ERROR, failed_to"),  # 1
                Edge("Q", "M1", "probe, to: instance 2"),  # 2
                Edge("Q", "M2", "probe, to: instance 2"),  # 3
                Edge("M1", "Q", "DELIVERY_ERROR"),  # 4
                Edge("M2", "B2", "probe"),  # 5
                Edge("B2", "M2", "PING"),  # 6
                Edge("M2", "Q", "PING"),  # 7
                Edge("Q", "M2", "request again, to: instance 2"),  # 8
                Edge("M2", "B2", "request"),  # 9
                Edge("B2", "M2", "reply"),  # 10
                Edge("M2", "Q", "reply"),  # 11
                Edge("M1", "B1", "connected"),  # 12
                Edge("M2", "B1", "connected"),  # 13
            ],
            steps=[
                Step(
                    "The client sends the request through one connection",
                    "With `to` set and a delivery error requested, through the lane's "
                    "connection when the call gave a lane, else any live one; here "
                    "multiplexer 1. A reply would end the query here, as it does whenever "
                    "the addressee is behind the multiplexer chosen.",
                    [0],
                    ["Q"],
                ),
                Step(
                    "The multiplexer does not have the instance",
                    "Instance 2 has not reconnected to multiplexer 1 yet, so it answers at "
                    "once with `DELIVERY_ERROR` naming the instance in `failed_to`. Instance 1, "
                    "of the same type, is right there and does not get the request: an "
                    "addressed query never takes a detour.",
                    [1],
                    ["M1"],
                ),
                Step(
                    "The client probes for the instance on every connection",
                    "The probe is a `BACKEND_FOR_PACKET_SEARCH` addressed to the instance, "
                    "which a backend declines while it drains, or with `probe=PING` a `PING`, "
                    "which a peer answers as long as it lives, for a request that must land "
                    "even then. Delivery errors are requested, so a multiplexer without the "
                    "instance says so. The connection dying under the first stage leads here "
                    "too. A request that simply gets no answer within the timeout does not: "
                    "a silent addressee is one the multiplexer still has, and a probe would "
                    "find the same one.",
                    [2, 3],
                    ["Q"],
                ),
                Step(
                    "One multiplexer says no, the other delivers",
                    "Multiplexer 1 answers with `DELIVERY_ERROR`; multiplexer 2 hands the probe "
                    "to instance 2. Every connection failing would end the query with "
                    "`OperationFailed`, at once: the instance is gone.",
                    [4, 5],
                    ["M1", "M2"],
                ),
                Step(
                    "The instance answers with a PING",
                    "Through the multiplexer that has it, which the client now knows.",
                    [6, 7],
                    ["B2"],
                ),
                Step(
                    "The client repeats the request through that connection",
                    "A fresh id, the same `to`, through the connection the `PING` came on. A "
                    "lane given to the call adopts that connection, so what the caller sends "
                    "next follows the query. A reply to the first attempt arriving late is "
                    "accepted too, so a slow instance may see the request twice.",
                    [8, 9],
                    ["Q"],
                ),
                Step(
                    "Instance 2 answers",
                    "The reply ends the query. Nothing within the timeout is `OperationTimedOut`; "
                    "a `DELIVERY_ERROR` for the repeated request, the instance leaving between "
                    "the probe and the request, is `OperationFailed`.",
                    [10, 11],
                    ["B2"],
                ),
            ],
        ),
    ],
    outro="""
Where this lives: `Client::_query` and `Client::_query_addressed` in
`multiplexer/client.cc`, `Client.query` in `multiplexer/mxclient.py`, and
the state machine of `multiplexer/threaded_client.cc` for the threaded and
asyncio clients; the search is routed in `Server::_handle_meta_message` in
`multiplexer/server.h`. Scenarios `query_one.py`, `round_robin.py`,
`backend_dies.py`, `two_mx_backends_on_each.py` and `unrouted_type.py`
under `tests/scenarios/` exercise the typed pictures;
`multiplexer/testing/lanes_test.py` and `multiplexer/threaded_lanes_test.py`
the addressed one.
""",
)

EVENTS = Page(
    file="events.md",
    title="Sending an event",
    intro="""
An event is a message nobody answers: `send_message()` in Python, `schedule_one()`
or `schedule_all()` in C++. The client chooses how many of its multiplexer
connections carry the message; each multiplexer then delivers it to the
backends its rules name. Nothing comes back unless the client sets
`report_delivery_error` and delivery fails.

The pictures use the healthy deployment: one client and two backends, all of
them connected to both multiplexers. The rule for the event type says
`whom: ALL`, so every backend of that type gets every event.
""",
    sections=[
        Section(
            "Through one connection",
            """
The default, `multiplexer=ONE` in Python and `schedule_one()` in C++, and the
normal way to send an event. One connection is chosen round robin among the
live ones.
""",
            columns=[
                Column("Clients", [("S", "client")]),
                Column("Multiplexers", [("M1", "multiplexer 1"), ("M2", "multiplexer 2")]),
                Column("Backends", [("L1", "backend 1"), ("L2", "backend 2")]),
            ],
            edges=[
                Edge("S", "M1", "event"),  # 0
                Edge("S", "M2", "connected"),  # 1
                Edge("M1", "L1", "event"),  # 2
                Edge("M1", "L2", "event"),  # 3
                Edge("M2", "L1", "connected"),  # 4
                Edge("M2", "L2", "connected"),  # 5
            ],
            steps=[
                Step(
                    "The client picks one connection",
                    "The message is queued on one connection, multiplexer 1 this time, and "
                    "the call returns. With `flush=True` it waits until the bytes are handed "
                    "to the socket; there is no acknowledgement either way.",
                    [0],
                    ["S"],
                ),
                Step(
                    "That multiplexer delivers to every backend of the type",
                    "Multiplexer 1 applies the rule: `whom: ALL`, so every backend of the type "
                    "connected to it gets a copy. Since every backend is connected to every "
                    "multiplexer, one connection is enough to reach them all.",
                    [2, 3],
                    ["M1"],
                ),
            ],
        ),
        Section(
            "Through a lane: a stream in order",
            """
Round robin spreads consecutive events over the connections, and order holds
per connection only, so a stream of events to one receiver can arrive out of
order. A lane, `multiplexer=client.lane()` in Python and a `Lane` in C++,
keeps a stream on one connection, a soft and late pin: the first event
through it pins it to the connection the library chose, and every later
one follows. When that connection dies, the lane lets go and takes
another, and the stream goes on from there, with a gap or a reorder at the
failover and no other; a pinned lane, `lane(pinned=True)`, is the hard pin
and refuses instead, raising `NotConnected`, for a stream that must not be
split. A query through a lane leaves it on the connection
the reply came through, so the events after a request follow the request.
""",
            columns=[
                Column("Clients", [("S", "client")]),
                Column("Multiplexers", [("M1", "multiplexer 1"), ("M2", "multiplexer 2")]),
                Column("Backends", [("L1", "backend 1")]),
            ],
            edges=[
                Edge("S", "M1", "event 1"),  # 0
                Edge("S", "M1", "event 2"),  # 1
                Edge("M1", "L1", "events 1, 2"),  # 2
                Edge("S", "M2", "event 3"),  # 3
                Edge("M2", "L1", "event 3"),  # 4
            ],
            steps=[
                Step(
                    "The lane takes the first connection used",
                    "Event 1 goes through multiplexer 1, chosen round robin; the lane now "
                    "holds that connection. Event 2 follows it, where round robin would have "
                    "sent it through multiplexer 2.",
                    [0, 1, 2],
                    ["S"],
                ),
                Step(
                    "Multiplexer 1 dies; the lane moves",
                    "Event 3 finds the lane's connection gone, goes through multiplexer 2, and "
                    "the lane holds that connection from now on. The backend saw events 1 and 2 "
                    "in order, then event 3; a pinned lane would have raised `NotConnected` "
                    "here instead.",
                    [3, 4],
                    ["S", "M2"],
                ),
            ],
        ),
        Section(
            "Through every connection",
            """
`multiplexer=ALL` in Python, also available as `Client.event()`, and
`schedule_all()` in C++. The same message goes out on each connection. Use it
when the event must get through even if a multiplexer is unreachable from some
backends; it costs one copy per multiplexer on every link.
""",
            columns=[
                Column("Clients", [("S", "client")]),
                Column("Multiplexers", [("M1", "multiplexer 1"), ("M2", "multiplexer 2")]),
                Column("Backends", [("L1", "backend 1"), ("L2", "backend 2")]),
            ],
            edges=[
                Edge("S", "M1", "event"),  # 0
                Edge("S", "M2", "event"),  # 1
                Edge("M1", "L1", "event"),  # 2
                Edge("M1", "L2", "event"),  # 3
                Edge("M2", "L1", "event, dropped as duplicate"),  # 4
                Edge("M2", "L2", "event, dropped as duplicate"),  # 5
            ],
            steps=[
                Step(
                    "One copy per connection",
                    "The client queues the message on every live connection. The call "
                    "returns how many connections took it; `flush=True` is not supported in "
                    "this mode.",
                    [0, 1],
                    ["S"],
                ),
                Step(
                    "Each multiplexer delivers, and duplicates are dropped",
                    "Both multiplexers apply the rule, so each backend is sent two copies. "
                    "The receiving library remembers the ids of the last 2048 messages it "
                    "saw and silently drops the second copy, so the backend's code sees the "
                    "event once.",
                    [2, 3, 4, 5],
                    ["M1", "M2"],
                ),
            ],
        ),
    ],
    outro="""
With a `whom: ANY` rule each multiplexer would pick one backend instead of all
of them; see [routing](routing.md). Where this lives: `Client.send_message` and
`Client.event` in `multiplexer/mxclient.py`, `schedule_one` and `schedule_all`
in `multiplexer/client.h`, duplicate suppression in
`BasicClient::handle_message` in `multiplexer/basic_client.cc`. Scenarios
`event_all_backends.py` and `any_vs_all.py` under `tests/scenarios/` cover
the fan-out.
""",
)

HANDSHAKE = Page(
    file="handshake.md",
    title="Connecting to a multiplexer",
    intro="""
A peer opens a TCP connection and both sides exchange one `CONNECTION_WELCOME`
message before anything else. The peer speaks first; the multiplexer registers
it and only then answers. After that, heartbeats keep the connection alive.

There are two kinds of peer. A backend hands control to the library, which
runs its loop all the time, so its heartbeats flow on their own; so does a
`ThreadedClient`, whose io thread does the same. A synchronous `Client` only
runs the loop inside calls, so its peer type is declared `is_passive` in the
rules file and the multiplexer neither expects heartbeats from it nor drops it
for silence.
""",
    sections=[
        Section(
            "Handshake and heartbeats",
            "",
            columns=[
                Column("Peer", [("P", "peer")]),
                Column("Multiplexer", [("M", "multiplexer")]),
            ],
            edges=[
                Edge("P", "M", "TCP connect"),  # 0
                Edge("P", "M", "CONNECTION_WELCOME type, id"),  # 1
                Edge("M", "P", "CONNECTION_WELCOME MULTIPLEXER, id"),  # 2
                Edge("P", "M", "HEARTBIT every 3 s"),  # 3
                Edge("M", "P", "HEARTBIT"),  # 4
                Edge("M", "P", "close after 30 s + 60 s of silence"),  # 5
            ],
            steps=[
                Step(
                    "Connect",
                    "The peer opens the socket. Nothing it sends before its welcome is routed; "
                    "a message before the handshake gets the connection closed.",
                    [0],
                    ["P"],
                ),
                Step(
                    "The peer introduces itself",
                    "A `WelcomeMessage` carries the peer's type, which must exist in the "
                    "multiplexer's rules file, and its instance id, a random 64-bit number the "
                    "peer chose. The multiplexer registers the connection under both.",
                    [1],
                    ["P"],
                ),
                Step(
                    "The multiplexer answers",
                    "Only now does the multiplexer send its own welcome, with type "
                    "`MULTIPLEXER` and its instance id. From here on messages flow in both "
                    "directions.",
                    [2],
                    ["M"],
                ),
                Step(
                    "Heartbeats, backends",
                    "Each side sends `HEARTBIT` every 3 s and expects some message within "
                    "30 s, then grants 60 s more. Any message resets that clock. A backend "
                    "sits in the loop, so this just works.",
                    [3, 4],
                    [],
                ),
                Step(
                    "Heartbeats, clients",
                    "A client sends no heartbeats between calls. The multiplexer does "
                    "not require any from it and sends it at most one heartbeat per message "
                    "received, so an idle client is neither dropped nor flooded.",
                    [4],
                    ["M"],
                ),
                Step(
                    "Silence from a backend",
                    "If a backend stops talking, the multiplexer closes the connection "
                    "after the two intervals. The backend's library reconnects 3 s after "
                    "noticing.",
                    [5],
                    ["M"],
                ),
            ],
            outro="",
        ),
        Section(
            "Reconnecting after a multiplexer restart",
            """
Every peer's library remembers the address it was told to connect to and
reconnects on its own when the connection goes away. A backend, which runs
the loop all the time, does this within a few seconds. A client does it the
next time it calls the library. The picture has one backend, one client and
one multiplexer that is restarted.
""",
            columns=[
                Column("Clients", [("C", "client")]),
                Column("Multiplexer", [("M", "multiplexer")]),
                Column("Backends", [("B", "backend")]),
            ],
            edges=[
                Edge("C", "M", "connected"),  # 0
                Edge("M", "B", "connected"),  # 1
                Edge("M", "B", "connection closed"),  # 2
                Edge("B", "M", "TCP connect after 3 s, then welcome"),  # 3
                Edge("C", "M", "request finds the connection dead"),  # 4
                Edge("C", "M", "TCP connect after 3 s, then welcome"),  # 5
                Edge("C", "M", "request"),  # 6
            ],
            steps=[
                Step(
                    "The multiplexer goes down",
                    "Both peers lose their connection. The backend's loop notices at once, "
                    "because its read fails. The client notices nothing yet: it is not in "
                    "the loop.",
                    [2],
                    ["M"],
                ),
                Step(
                    "The backend reconnects",
                    "The backend's library waits 3 s, connects to the same address again "
                    "and repeats the handshake. If the multiplexer is still down it tries "
                    "again every 3 s. Once it is back, the backend is registered as if "
                    "nothing had happened, under the same instance id.",
                    [3],
                    ["B"],
                ),
                Step(
                    "The client's next call finds the connection dead",
                    "The client's library learns about the closed connection only when it "
                    "next runs the loop, inside a call. The request it just wrote is lost "
                    "with the connection; the call does not fail, it keeps running the "
                    "loop.",
                    [4],
                    ["C"],
                ),
                Step(
                    "The same call reconnects and sends again",
                    "The reconnect timer fires 3 s later, inside the call, the handshake "
                    "runs, and the request goes out again with a fresh id. The caller "
                    "sees a slow call, not an error, as long as the multiplexer is back "
                    "within the call's timeout. With connections to several multiplexers "
                    "the request goes through another one at once instead.",
                    [5, 6],
                    ["C"],
                ),
            ],
            outro="""
Where this lives: `multiplexer/io/connection.h` for the welcome exchange and
the heartbeat timers, `multiplexer/connections_manager.h` for registration,
`multiplexer/basic_client.cc` for the reconnect timer, `multiplexer/defaults.h`
for the intervals. The `raw_protocol` scenario in `tests/scenarios/` performs
the handshake byte by byte; the `mx_restarts` and `mx_restarts_under_idle_client`
scenarios record what a backend and a client see across a restart.
""",
        ),
    ],
)

ROUTING = Page(
    file="routing.md",
    title="How the multiplexer routes a message",
    intro="""
Every message carries a type. The rules file maps each message type to one
peer type and says whether one peer of that type gets the message (`whom: ANY`,
round robin) or all of them (`whom: ALL`). A message may instead name a peer
directly by instance id, which wins over the rules. A message nobody can
receive is answered with `DELIVERY_ERROR` if the client asked for one.

Peer types are how you group backends. The picture has one client, one
multiplexer and two backend types, A and B, with two backends each. Type A
receives an event type under a `whom: ALL` rule; type B receives a request
type under a `whom: ANY` rule.
""",
    sections=[
        Section(
            "Rules, direct addressing, delivery errors",
            "",
            columns=[
                Column("Clients", [("S", "client")]),
                Column("Multiplexer", [("M", "multiplexer")]),
                Column(
                    "Backends",
                    [
                        ("A1", "type A, backend 1"),
                        ("A2", "type A, backend 2"),
                        ("B1", "type B, backend 1"),
                        ("B2", "type B, backend 2"),
                    ],
                ),
            ],
            edges=[
                Edge("S", "M", "event; rule: type A, whom ALL"),  # 0
                Edge("M", "A1", "event"),  # 1
                Edge("M", "A2", "event"),  # 2
                Edge("S", "M", "request; rule: type B, whom ANY"),  # 3
                Edge("M", "B1", "request"),  # 4
                Edge("M", "B2", "request, next time"),  # 5
                Edge("S", "M", "to = id of type A, backend 2"),  # 6
                Edge("M", "A2", "direct"),  # 7
                Edge("S", "M", "type with no rule"),  # 8
                Edge("M", "S", "DELIVERY_ERROR"),  # 9
            ],
            steps=[
                Step(
                    "whom: ALL",
                    "The rule for this message type names peer type A with `whom: ALL`, "
                    "so every connected backend of type A gets a copy. This is how events "
                    "are usually routed.",
                    [0, 1, 2],
                    ["S"],
                ),
                Step(
                    "whom: ANY",
                    "The rule names peer type B with `whom: ANY`. One connected backend of "
                    "type B gets the message, chosen round robin. This is how requests are "
                    "usually routed: the backends of a type are interchangeable workers.",
                    [3, 4],
                    ["B1"],
                ),
                Step(
                    "The next request, same rule",
                    "Round robin moves on to the other backend of type B. A backend that "
                    "has dropped off is skipped.",
                    [3, 5],
                    ["B2"],
                ),
                Step(
                    "A direct address",
                    "When `to` is set, the message goes to that instance id and the rules "
                    "are not consulted. This is how a backend replies to the client that "
                    "asked, and how a client repeats a request to the backend a search "
                    "found.",
                    [6, 7],
                    ["A2"],
                ),
                Step(
                    "Nobody can receive it",
                    "No rule for the type, or a rule whose peer type has no connected "
                    "backend: the message is dropped, and the client gets `DELIVERY_ERROR` "
                    "if it set `report_delivery_error`. A query's search relies on this.",
                    [8, 9],
                    ["M"],
                ),
            ],
        ),
    ],
    outro="""
Where this lives: `Server::handle_message` in `multiplexer/server.h`; the rules
file format is described in the root README. The `any_vs_all`,
`direct_addressing` and `unrouted_type` scenarios under `tests/scenarios/`
cover each case.
""",
)

DIAGRAMS = [QUERY, EVENTS, HANDSHAKE, ROUTING]
