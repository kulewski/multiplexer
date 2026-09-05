# Working in tests/

Integration tests: real multiplexers and real peer processes, driven from
Python scenarios. The harness is `multiplexer/testing`, public and
importable from other workspaces; `harness/` here only adds the constants
of `testing.rules`. `README.md` here is the reference for the harness, the
roles, their options and their events; `docs/recipes/add_a_scenario.md` is
the step-by-step. What changes in `multiplexer/testing` changes for every
workspace that tests with it: keep names and signatures.

- One scenario per folder under `scenarios/`, named for what it shows: the
  test (`<name>/<name>.py`, with a one-line docstring) and a README with a
  Mermaid sequence diagram and the checks. `scenarios/README.md` is generated
  from the docstrings by `format.sh`. One `mx_integration_test` per
  configuration in `scenarios/BUILD`.
- Roles exist in both languages with identical options and events. A new
  option or event is added to `roles/py/<role>.py`, to the matching class in
  `roles/cc/mxtestroles.cc`, and to the table in `README.md`, in one change.
- Client roles are passive unless spawned with `threaded=True`, which uses
  `ThreadedClient` and may use the active `TEST_ACTIVE_CLIENT` type. Do not
  write a scenario that needs a synchronous client running the loop between
  calls.
- Scenarios that wait out heartbeat intervals carry `tags = ["slow"]`, so
  `bazel test --test_tag_filters=-slow //...` stays a few seconds.
- Test-only peer and message types live in `testing.rules`, numbers 201 and
  up, names `TEST_*`.
