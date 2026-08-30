# aio

An asyncio program on the multiplexer: `gateway.py` is a TCP server that
turns every line a client sends into a request awaited on the event loop,
answers with the reply, and writes what the chat server broadcasts to every
client it holds; `backend.py` is that chat server. `chat.rules` adds the two
peer types and three message types; `.bazelrc` points the build at it.

```
bazel test //...                                                       # the end-to-end test
bazel run @mx//mxcontrol -- run_multiplexer --address 127.0.0.1:1980 --rules $PWD/chat.rules
bazel run //:backend -- 127.0.0.1:1980                                 # in another terminal
bazel run //:gateway -- 127.0.0.1:1980 8765                            # and another
nc 127.0.0.1 8765                                                      # type lines; "shout hi" reaches everyone
```

`gateway_test.py` runs the gateway on the test's own loop against a real
multiplexer from `multiplexer.testing`, with the chat server on a
`BackendThread`, and checks a line and a shout through two TCP clients.
The Django Channels version of the gateway is in
[docs/recipes/async_web_server.md](../../docs/recipes/async_web_server.md).
