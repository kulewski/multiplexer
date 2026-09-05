# Recipe: add an mxcontrol subcommand

`mxcontrol` is a set of subcommands registered at link time;
[mxcontrol.md](../mxcontrol.md) describes the existing ones.

1. **Write a `Task` subclass** in `mxcontrol/<name>.cc`: override `run()`,
   `short_description()` (shown by `mxcontrol help`), and
   `_initialize_options_description()` to declare options with
   `boost::program_options`. Call `_add_multiplexer_client_options()` there
   if the command is itself a peer, then `_multiplexer_client(peer_type)`
   in `run()` returns a `Client` connected to every `--multiplexer` given.
   `receive_logs.cc` is the smallest complete example, `stream_logs.cc` the
   next.
2. **Register it** at file scope with
   `REGISTER_MXCONTROL_SUBCOMMAND(name, mxcontrol::ClassName);`. The name is
   the word on the command line.
3. **Add a `cc_library`** for the file in `mxcontrol/BUILD` with
   `alwayslink = 1`, like the others, and list it in the deps of the
   `mxcontrol` binary. The static registration only happens if the object is
   linked, which `alwayslink` guarantees.
4. **Check** `bazel run //mxcontrol -- help` lists it and
   `bazel run //mxcontrol -- help name` prints its options, then add it to
   [mxcontrol.md](../mxcontrol.md).

The test roles in `tests/roles/cc/mxtestroles.cc` are subcommands of a
different binary built the same way; a tool that belongs to the tests goes
there rather than into `mxcontrol`.
