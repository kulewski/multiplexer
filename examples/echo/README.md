# echo

A backend that upper-cases whatever it is asked, and a client that asks, each
in Python and in C++; and `workers.py`, several threads sharing one
`ThreadedClient` and getting their replies through their own queues. `echo.rules` adds the two peer types and two message types
the example uses; `.bazelrc` points the build at it, so the generated constants
carry `peers.ECHO_BACKEND`, `peers.ECHO_CLIENT`, `types.ECHO_REQUEST` and
`types.ECHO_RESPONSE`.

```
bazel test //...                    # the end-to-end test, and harness_test.py, below
bazel run @mx//mxcontrol -- run_multiplexer --address 127.0.0.1:1980 --rules $PWD/echo.rules
bazel run //:backend_py         # or //:backend_cc, in another terminal
bazel run //:client_py -- 127.0.0.1:1980 "hello multiplexer"     # or //:client_cc
bazel run //:workers -- 127.0.0.1:1980 3 4                         # 3 threads, 4 jobs each
```

`harness_test.py` tests the same exchange without the binaries, with the
multiplexer's own test infrastructure: `mx_integration_test` in `BUILD`
(from `@mx//multiplexer/testing:defs.bzl`) starts the test with this
workspace's rules file, and the test opens a `Cluster` of real multiplexers,
scripts a `FakePeer` as the backend and asks it with a `TestClient`. Its
other tests are the shapes a program's tests need most: a multiplexer
restart under a live client, a raising handler seen as `BackendError`, and
a recording read back. `default_rules_test.py` is the same from a plain
`py_test`: `Cluster()` finds this workspace's rules through the flag. See
[docs/api_python.md](../../docs/api_python.md#testing).
