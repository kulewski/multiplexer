# A C++-only consumer

A workspace that uses the multiplexer from C++ alone, as `@mx`, the way a
program of yours that has no Python would: `mx_dependencies(python = False)`
declares no Python repository, and no `mx_setup()` is called, since it only
sets up Python. `client.cc` and `backend.cc` only have to compile and link
against `@mx//multiplexer:client` and `@mx//multiplexer/backend:base_multiplexer_server`.

`check.sh` builds it with `(cd tests/cc_only_consumer && bazel build //...)`;
the repository's own build leaves it out (`.bazelignore`), as it does the
example workspaces.
