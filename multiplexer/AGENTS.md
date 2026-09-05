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
  received, never re-serialized.
- Input from the network is checked with `if` and answered by closing the
  connection. `Assert` is for our own invariants only.
- The query algorithm exists three times, `Client::_query` in `client.cc`,
  `ThreadedClient` in `threaded_client.cc` and `Client.query` in
  `mxclient.py`, and so does the backend loop,
  `backend/base_multiplexer_server.cc` and `servers.py`. A change to one is
  a change to both, and a scenario under `tests/scenarios/` that runs in both
  languages is how it is checked.
- Constants come from the rules file at build time (`generate_constants.cc`).
  A new protocol message type goes into `multiplexer.rules`, the reserved
  range below 100, and into `docs/wire_format.md`.
- `Connection`, `ConnectionsManager` and everything on them belong to the
  thread that runs their `io_service`, declared with `MX_DCHECK_RUN_ON`.
  Code that runs on another thread reaches them only through
  `io_service::post`. Keep it that way and the clang analysis keeps proving it.
- Anything a peer can observe is documented under `docs/`; update the page
  in the same change.
