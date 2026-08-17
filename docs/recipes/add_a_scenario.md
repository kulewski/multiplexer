# Recipe: add an integration test scenario

A scenario is one folder under `tests/scenarios/` holding the test, a
Python file of the same name that starts real multiplexers and role
processes and asserts on what they report, and a README that draws what
happens. [tests/scenarios/README.md](../../tests/scenarios/README.md) is the
generated index.
[tests/README.md](../../tests/README.md) documents the harness, the four
roles, their options and their events.

1. **Write the folder.** Copy the closest existing scenario folder;
   `query_one` is the simplest. Keep the README's shape: what happens as a
   Mermaid sequence diagram, what is checked, how to run it. It opens a `Cluster(n)`, spawns roles with `spawn(role,
   cfg.lang(role), mx=cluster.addresses, type=C.peers.TEST_..., ...)`, waits
   with `role.wait_for("connected", ...)`, and asserts on `role.events_of(...)`.
   Start with a one-line docstring saying what the scenario shows; the code
   map picks it up.
2. **Register it** in `tests/scenarios/BUILD` with `mx_integration_test`. The
   `roles` dict says who plays each role, `"py"`, `"cc"` or the label of a
   binary of your own; a list comprehension over `LANGS` runs the scenario
   in every combination. Use `mx = 2` for two
   multiplexers, `params` for anything the scenario reads with
   `cfg.param()`, and `tags = ["slow"]` if it waits on heartbeat intervals.
3. **Run it**: `bazel test //tests/scenarios:name_py_py`, then the fast
   suite, `bazel test --test_tag_filters=-slow //...`.
4. **When it fails**, every process's stderr and events are under the
   test's `test.outputs/outputs.zip`, named after the role.

If the roles cannot express the behaviour, extend them: the option and the
event have to be added to both `tests/roles/py/<role>.py` and the matching
class in `tests/roles/cc/mxtestroles.cc`, with the same names, and to the
table in `tests/README.md`. A protocol-level scenario that needs bytes on
the wire uses `multiplexer.testing.raw_peer` instead of a role.
