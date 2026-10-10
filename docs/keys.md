# Keys: routing within a peer type

The rules route by message type to a peer type. Sometimes the backends of
one type are not interchangeable for every message:

- a tenant's requests should go to the backends serving that tenant;
- an inference request should go to the workers that have its model loaded;
- a group's events should go only to the processes with members in it.

A key lets a message say what it is about, and a key filter lets each peer
say which keys of a message type it takes. Everything else about routing
stays as [routing](routing.md) and [the rules file](rules.md) describe it.

## A key and a key filter

**The key.** A message's `key` (field 9 of the envelope) is a string of at
most `MAX_KEY_SIZE` bytes, 256.

- Python: `query(..., key=...)`, and `key=` among the fields of
  `send_message()` and `new_message()`.
- C++: the key set on a whole `MultiplexerMessage` given to a send or a
  query. The payload forms of `query()` take none.

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

- **`whom: ANY`.** One of them, round robin, skipping full queues; a last
  resort among them only when none of the others can take it.
- **`whom: ALL`.** Every one of them.
- **`to`.** An addressed message ignores filters, as it ignores rules.
- **The search.** The search a query falls back to
  (`BACKEND_FOR_PACKET_SEARCH`) carries the request's key and goes to the
  same peers. So the backend it finds, which then gets the request
  addressed to it, takes the key: a failover never moves a request to a
  backend that does not take its key.

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
built: `ValueError` in Python, `std::length_error` in C++. A multiplexer
that gets one drops the message and tells the sender.

## Declaring the filters

`set_key_filters({message type: [patterns]})` is on every class:
`SyncClient`, `ThreadedClient`, `AsyncClient` and both server classes, in
both languages. It replaces the filters set before. Set them before the
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
take the filters there too, as `key_filters=`.

**Changing them while running.** A later `set_key_filters()` goes to every
multiplexer as `PEER_CONTROL` and leaves the routing as it is. Each
multiplexer answers once the change is in effect, after everything it
routed under the old filters. Once every one has, and everything they
routed before has been handed to the peer, the change is acknowledged:
`routing_acknowledged()` is true, and

- **`on_acknowledged`**, given to `set_key_filters()` (or to
  `set_routing()`), is called once, where each class calls back:
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
  `on_acknowledged`: the requests routed before have been served by then,
  and no more come. A worker unloading its last model keeps the type with
  an empty list: dropping the type from its filters would take every key.
- **Moving a tenant between pools:** the new owner adds the key and waits
  for its acknowledgement; then the old owner removes it and waits for
  its own. Both may serve the tenant in between.

**The drain keeps them.** A drain's routing leaves the filters as they are,
and a change of filters leaves a drain as it is
([how a backend leaves](leaving.md)).

## Refusals and limits

**Refused by the library**, which raises `ValueError` in Python and
`std::invalid_argument` in C++:

- a filter for a protocol message type, 99 and below;
- a pattern longer than `MAX_KEY_PATTERN_SIZE` bytes, 1024.

Python also refuses one string where the list of patterns goes
(`TypeError`), rather than read it as one-letter patterns.

**Refused by the multiplexer**, which compiles every pattern:

- a pattern that does not compile;
- a second filter for one type.

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
  - one comparison per peer, about 1 ns.
- **Peers declaring the same pattern share it:** a pool of a hundred
  backends with one filter costs what one backend does.
- **A pattern new to its type** is compiled once, about 5 µs, and the set
  over the type's other patterns is built again at the next keyed message:
  0.25 ms for 64 patterns, 0.9 ms for 400 and 28 ms for 10,000, during
  which the multiplexer routes nothing else. Exact keys need neither:
  100,000 are declared in 15 ms. A filter that changes often, or lists
  many keys, is best made of them.

Only the multiplexer links RE2. The Bazel build fetches it; the build
without Bazel needs `libre2-dev` ([building](building.md)).

## Where it is shown

- **[tests/scenarios/key_routing](../tests/scenarios/key_routing/README.md):**
  pools by key through every client mode and both server classes; the
  search carrying the key from `SyncClient` and `ThreadedClient`; events by
  key. In both languages.
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
