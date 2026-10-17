# Keys: routing within a peer type

The rules route by message type to a peer type. Sometimes the backends of
one type are not interchangeable for every message:

- a tenant's requests should go to the backends serving that tenant;
- an inference request should go to the workers that have its model loaded;
- a group's events should go only to the processes with members in it.

A key lets a message say what it is about, and a key filter lets each peer
say which keys of a message type it takes. Everything else about routing
stays as [routing](routing.md) and [the rules file](rules.md) describe it.
[examples/models](../examples/models/README.md) is a complete program on
keys: workers that each hold some of the models, loading and unloading
them while they serve.

## A key and a key filter

**The key.** A message's `key` (field 9 of the envelope) is a string of at
most `MAX_KEY_SIZE` bytes, 256: text, in UTF-8, as protobuf's `string`
fields are, and what the filters' regular expressions match. Python's and
Rust's keys are text by their type; a C++ program sets bytes, and one that
sets bytes that are not UTF-8, a binary digest say, sends a frame that a
Rust peer refuses, closing the connection it came on, and that a debug
build of a C++ peer logs protobuf's error for. Such a key goes as hex or
base64.

- Python: `query(..., key=...)`, and `key=` among the fields of
  `send_message()` and `new_message()`.
- C++: the key set on a whole `MultiplexerMessage` given to a send or a
  query. The payload forms of `query()` take none.
- Rust: `.key(key)` on a query or a send, or `Message::key`, an
  `Option<String>`, of a whole message.

**The key filter.** A peer's filter for a message type is a list of
patterns, RE2 regular expressions, each matched against the whole key:
`tenant\.acme\..*` takes `tenant.acme.orders`, but not
`tenant.acmeco.orders`. Patterns use RE2's syntax, which has no
back-references and no lookaround.

**Four rules hold throughout:**

- A peer with no filter for a type takes every key of it.
- A filter with no pattern takes no key.
- A message without a key ignores every filter.
- A filter belongs to its message type: a peer that filters `USER_REQUEST`
  takes every key of the other types.

## Where a keyed message goes

A key filter is a filter on what its peer receives. A peer **takes** a key
of a type when it has no filter for the type, or when its filter takes the
key. Routing passes over the peers that do not take a message's key and
treats the rest as it always does:

- **`whom: ANY`.** One of them, round robin: the one that has waited
  longest for a message of its type, a peer that has just registered
  first, so that the peers taking a key take its messages in turn, as the
  peers of a type with no filters take every message in turn, and one
  that another key's messages keep busy lets the others take its turn;
  full queues skipped, a last resort among them only when none of the
  others can take it.
- **`whom: ALL`.** Every one of them.
- **`to`.** An addressed message ignores filters, as it ignores rules.
- **The search.** The search a query falls back to
  (`BACKEND_FOR_PACKET_SEARCH`) carries the request's key and goes to the
  same peers. So the backend it finds, which then gets the request
  addressed to it, took the key when it answered: a failover never moves a
  request to a backend whose filters had dropped its key before the search.
  A backend that answered the search and changed its filters right after
  can still get that addressed request, after its acknowledgement, since
  `to` ignores filters.

**A filter keeps keys away from its own peer only.** A peer with no filter
for the type takes every key, those a dedicated pool serves included. To
keep a tenant on its own pool, the general pool declares a filter too.
RE2 has no lookahead, so "every tenant but acme" is awkward as a pattern;
keys made for it are easy:

- the senders key a tenant with a pool of its own `dedicated.<tenant>.…`,
  and every other tenant `shared.<tenant>.…`;
- acme's pool takes `dedicated\.acme\..*`;
- the general pool takes `shared\..*`.

**When nobody takes the key.**

- The sender gets the usual `DELIVERY_ERROR`, when the rule reports one.
- The multiplexer logs "no key filter of a peer of type … takes the key of
  a message of type …", its first line of a kind naming the key, then a
  count a second as for every other drop ([operations](operations.md#logs)).
- A recording says `KEY_NOT_TAKEN`, and each record of a keyed message
  carries the key.

**A key over `MAX_KEY_SIZE`.** The libraries refuse it where the message is
built: `ValueError` in Python, `std::length_error` in C++, `TooLarge` in
Rust. A multiplexer
that gets one drops the message and tells the sender.

## Declaring the filters

`set_key_filters({message type: [patterns]})` is on every class:
`SyncClient`, `ThreadedClient`, `AsyncClient` and both server classes, in
every language, in Rust with a `KeyFilters`, a `BTreeMap` of message type
to patterns. It replaces the filters set before. Set them before the
peer connects, so that its welcome carries them and nothing reaches it
without them:

```python
backend = TenantBackend(addresses, peers.USER_SERVICE)
backend.set_key_filters({types.USER_REQUEST: [r"tenant\.acme\..*", r"tenant\.globex\..*"]})
backend.serve_forever()
```

```cpp
backend.set_key_filters({{multiplexer::types::USER_REQUEST, {R"(tenant\.acme\..*)"}}});
backend.serve_forever();
```

`ThreadedClient` and `AsyncClient` connect in their constructors, so they
take the filters there too, as `key_filters=`; in Rust every builder takes
them, `key_filters(filters)`, so that the first welcome carries them.

**Changing them while running.** A later `set_key_filters()` goes to every
multiplexer as `PEER_CONTROL` and leaves the routing as it is. Each
multiplexer answers once the change is in effect, after everything it
routed under the old filters. Once every one has, and everything they
routed before has been handed to the peer, the change is acknowledged:
`routing_acknowledged()` is true, and

- **`on_acknowledged`**, given to `set_key_filters()` (or to
  `set_routing()`; in Rust `set_key_filters_then()` and
  `set_routing_then()`), is called once, where each class calls back:
  - `SyncClient`: inside a `receive_message()`, the one after the last
    message routed before was received, or the one waiting when the
    acknowledgement came;
  - `ThreadedClient`: on its io thread, after `on_message` had everything
    routed before;
  - `AsyncClient`: on its event loop, after the deliveries before;
  - the server classes: on the loop's thread, between two messages, once
    every request routed before has been handled, by the workers too on
    `BaseThreadedMultiplexerServer`.
- **The server classes' loops wake**, so that `periodic_task()` and a
  drain see the acknowledgement at once rather than at their next poll.

That ordering makes a handover safe:

- **Unloading a model:** remove its pattern, and free the model in
  `on_acknowledged`: the requests routed by the rules before have been
  served by then, and no more come so. One can still come addressed: the
  third stage of a query whose search the worker answered just before the
  change. Its handler answers it, with an error when the model is gone,
  rather than assume the model, so that the caller gets the error at once;
  or the worker waits a client's round trip after `on_acknowledged` before
  it frees the model.
  A worker unloading its last model keeps the type with an empty list:
  dropping the type from its filters would take every key.
  [examples/models](../examples/models/walkthrough.md) does it.
- **Moving a tenant between pools:** the new owner adds the key and waits
  for its acknowledgement; then the old owner removes it and waits for
  its own. Both may serve the tenant in between, and the old owner may
  still get an addressed request after its acknowledgement, as above.

**The drain keeps them.** A drain's routing leaves the filters as they are,
and a change of filters leaves a drain as it is
([how a backend leaves](leaving.md)).

## Refusals and limits

**Refused by the library**, which raises `ValueError` in Python,
`std::invalid_argument` in C++ and returns `InvalidArgument` in Rust:

- a filter for a protocol message type, 99 and below;
- a pattern longer than `MAX_KEY_PATTERN_SIZE` bytes, 1024.

Python also refuses one string where the list of patterns goes
(`TypeError`), rather than read it as one-letter patterns.

**Refused by the multiplexer**, which compiles every pattern:

- a pattern that does not compile;
- a second filter for one type;
- in a `PEER_CONTROL`, filters whose answer, `PEER_STATUS`, which repeats
  them, would be over `MAX_MESSAGE_SIZE`: a request within a few dozen
  bytes of the limit.

There is no limit on the number of patterns or keys but the frame that
carries them, `MAX_MESSAGE_SIZE`; what they cost is below.

A welcome carrying such a filter is refused: the multiplexer closes the
connection and logs why, as for a peer type the rules file does not name.
A `PEER_CONTROL` carrying one is answered with the error, and the filters
and routing stay as they were. The library logs that answer as an error,
and `routing_acknowledged()` stays false until a change succeeds.

## What it costs

- **A message without a key:** nothing.
- **A keyed message of a type no peer filters:** one hash lookup.
- **A keyed message of a filtered type**, measured on one core:
  - one hash lookup among the type's exact keys, its patterns that are
    plain text, `model\.alpha` say, letters, digits and escaped
    punctuation: 20 ns, 36 ns with 100,000 of them;
  - one match of the key against all of its other patterns at once, an
    RE2::Set: 60 ns, 75 ns with 400 patterns, 100 ns with 10,000;
  - a write for each peer that holds a key or a pattern that matched;
  - for `whom: ANY`, about 6 ns for each peer of the type that does not
    take the key and stands ahead of the one that does, in the order of
    the turns: 6 µs a message for a key a thousand others pass over. A
    type whose keys are each taken by few of many peers pays for the
    peers it passes over; one whose keys most of its peers take pays next
    to nothing.
- **Peers declaring the same pattern share it:** a pool of a hundred
  backends with one filter costs what one backend does.
- **A pattern new to its type** is compiled once, about 5 µs, and the set
  over the type's other patterns is built again at the next keyed message:
  0.25 ms for 64 patterns, 0.9 ms for 400 and 28 ms for 10,000, during
  which the multiplexer routes nothing else. Exact keys need neither:
  100,000 are declared in 15 ms. A filter that changes often, or lists
  many keys, is best made of them.
- **The set's memory** is sized from what its patterns compile to, 128
  bytes for each instruction of their programs, 8 MiB at least and 1 GiB
  at most: a thousand long patterns with repetitions, which RE2 unrolls
  into some 260 instructions each, take about 32 MiB at most, and only as
  matching needs it. A set that cannot be built all the same is
  said in a `WARNING`, and its type's keyed messages are matched against
  its patterns one by one, with the same answer, about 18 µs a message
  for a thousand of them.

Only the multiplexer links RE2. The Bazel build fetches it; the build
without Bazel needs `libre2-dev` ([building](building.md)).

## Where it is shown

- **[tests/scenarios/key_routing](../tests/scenarios/key_routing/README.md):**
  pools by key through every client mode and both server classes; the
  search carrying the key from `SyncClient` and `ThreadedClient`; events by
  key. In Python, C++ and Rust.
- **[multiplexer/key_routing_test.py](../multiplexer/key_routing_test.py):**
  the protocol itself:
  - whom `whom: ANY` picks from and whom `whom: ALL` reaches;
  - a peer without a filter, and an empty filter;
  - thousands of exact keys and hundreds of patterns of one type;
  - the search;
  - `PEER_CONTROL` and its ordering;
  - each refusal;
  - the size limit;
  - `to`.
- **[multiplexer/key_filters_test.cc](../multiplexer/key_filters_test.cc):**
  the multiplexer's index of patterns, its exact keys checked against RE2.
- **[multiplexer/acknowledged_test.py](../multiplexer/acknowledged_test.py)
  and [its C++ twin](../multiplexer/backend/acknowledged_test.cc):**
  `on_acknowledged` in every class, after what was routed before, and the
  server classes' loops woken by the acknowledgement.
- **[examples/models](../examples/models/README.md):** workers that each
  hold some of the models, a model loaded and one unloaded while its
  requests keep coming, a worker with an empty filter and one with none;
  its test makes the race of a request routed under the old filter happen
  every time.
