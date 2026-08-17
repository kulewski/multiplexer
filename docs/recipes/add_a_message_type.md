# Recipe: add a peer type or a message type

For a deployment that has its own rules file, all of this happens in that
file and in the peers; nothing in this repository changes.

1. **Edit the rules file.** Add a `peer { type: N name: "NAME" }` block for a
   new kind of program, with `is_passive: true` if it is a client, and a
   `type { type: N name: "NAME" to { peer: "..." whom: ANY|ALL } }` block
   for a new message type. Numbers must be 100 or more and unique within
   their kind; names must be unique. [The rules file](../rules.md) has every
   field. A reply type needs no `to` rule.
2. **Build.** The constants regenerate: `peers.NAME` and `types.NAME` in
   Python, `multiplexer::peers::NAME` and `multiplexer::types::NAME` in C++.
   A duplicate name or number fails the build here.
3. **Restart the multiplexers** with the new file; they read it once at
   start.
4. **Use it.** A backend of the new peer type subclasses
   `BaseMultiplexerServer` with `type=peers.NAME`; a client sends
   `type=types.NAME`. [examples/echo](../../examples/echo) is a complete
   pair.

In this repository the example file is [multiplexer.rules](../../multiplexer.rules)
and the integration tests use [tests/testing.rules](../../tests/testing.rules),
which adds `TEST_*` entries at 201 and up. A test that needs a new type adds
it there, in the `TEST_` range, and refers to it as `C.types.TEST_NAME`
through the generated `tests/testing_constants.py`.
