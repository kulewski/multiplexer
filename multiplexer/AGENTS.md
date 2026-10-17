# Working in multiplexer/

The broker (`server.h`), the C++ client (`basic_client.h`, `client.h`,
`threaded_client.h`), the backend base class (`backend/`), the Python
package (`clients.py`, `servers.py`, `mxclient.py`, `threaded_client.py`,
`_native.cc`) and what they share (`io/`, `config.h`,
`defaults.h`, `Multiplexer.proto`). Every file opens with a comment saying
what it is and where the tricky parts are; `docs/code_map.md` collects them.

- The per-message path is `Server::_handle_message` in `server.h` and
  `Connection` in `io/connection.h`. Nothing new goes there without a reason
  that survives "what does this cost per message"; frames are forwarded as
  received, never re-serialized. The several-loop paths branch off it on
  `Server::loops_` and `Connection::shared_`, set before start and never
  changed: one loop pays a predicted branch for them, nothing more.
- Input from the network is checked with `if` and answered by closing the
  connection. `Assert` is for our own invariants only.
- The query algorithm exists four times, `Client::_query` in
  `client_query.cc`, `ThreadedClient` in `threaded_client_query.cc`,
  `Client.query` in `mxclient.py` and `core/query.rs` in
  `rust/mx-multiplexer/src`, and the backend loop three,
  `backend/base_multiplexer_server.cc`, `servers.py` and the Rust crate's
  `servers/`; the outbox (`outbox.cc` and its parts) and the connection
  (`io/connection.h`) exist again in the Rust core, `core/outbox/` and
  `core/connection.rs`. A change to one is a change to all, and a scenario
  under `tests/scenarios/` that runs in every language is how it is checked.
- Constants come from the rules file at build time (`generate_constants.cc`).
  A new protocol message type goes into `multiplexer.rules`, the reserved
  range below 100, and into `docs/wire_format.md`.
- `Connection`, `ConnectionsManager` and everything on them belong to the
  thread that runs their `io_service`, declared with `MX_DCHECK_RUN_ON`.
  Code that runs on another thread reaches them only through
  `io_service::post`. Keep it that way and the clang analysis keeps proving it.
  The multiplexer on several event loops (`run_multiplexer --threads`,
  `server_loops.h`) keeps this, declared too: its routing state is
  `owner_thread_`'s, which `Server::RoutingScope` holds there by taking the
  routing lock, and a connection's outgoing side is a `SharedOutgoing`
  (`io/shared_outgoing.h`) under a lock of its own; the rest of a
  connection, its reading and its timers, stays its own loop's, reached by
  a post (`server_shared.cc`). A new entry point into the routing takes a
  `RoutingScope`, never `MX_DCHECK_RUN_ON`, and the multiplexer's tests on
  several loops run under ThreadSanitizer (AGENTS.md's commands).
- Anything a peer can observe is documented under `docs/`; update the page
  in the same change.
