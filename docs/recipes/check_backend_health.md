# Recipe: check a backend's health

A backend can stop serving while its process runs on: a handler waits
for a database that never answers, two threads deadlock, a call into a C
library never returns. Kubernetes restarts a container whose liveness
probe fails, so the probe has to see the loop that serves, not the
process around it. This page ties the probe to the serve loop of
`BaseMultiplexerServer`, in Python and in C++, and says what to watch on
the threaded server classes instead.
[examples/health](../../examples/health) is all of it in Python: a
worker, its health endpoint, a Kubernetes manifest, and a test that
holds a handler and watches the endpoint go unhealthy and come back.

## Why the usual checks miss a stuck loop

**The multiplexers see the connection.** A multiplexer routes a request
by the rules to a registered connection of the request's peer type,
round robin, passing over one whose routing turned the path off or
whose queue on that multiplexer is full: `queue_size` messages, 1024 by
default ([the rules file](../rules.md#a-peer-type)), after what the
sockets hold. Nothing asks whether the program behind the connection
still reads. A `BaseMultiplexerServer` runs the library's loop,
heartbeats included, on its one thread, between handlers and inside the
library's own calls, so a handler stuck in code of its own stops
everything the backend sends. Every
request routed to it meanwhile waits unanswered: its caller waits out a
stage's timeout, then searches, and repeats the request to a backend
whose loop answers the search ([how a query is answered](../query.md)).
A multiplexer drops a peer of a type that is not `is_passive` 90 s
after its last frame, and the backend is then out of the pool until its
handler returns, if it ever does: its loop is what would reconnect, and
the process lives on. A type marked `is_passive`, which the server
classes do not need, is never dropped for silence, and keeps getting
requests until its queue is full.
[The health example's steps](../../examples/health/walkthrough.md#the-steps)
show the requests that wait out their timeout, the drop at 90 s, and
the reconnect once the handler returned.

**A health endpoint on a thread of its own answers for the process.**
The thread runs whatever the loop does: a handler blocked on I/O, on a
lock or in `time.sleep()` releases the GIL, and one that spins in Python
code gives it up every 5 ms (`sys.getswitchinterval()`), so the endpoint
answers 200 while nothing is served. Only a handler stuck in C code that
holds the GIL stops the endpoint too, and the probe then fails on its
timeout.

**A TCP probe sees the kernel.** A `tcpSocket` probe passes once the
kernel completes the connection, which it does for a listening port
without the program, even one stopped with `SIGSTOP`, until the listen
backlog is full. A backend has no port of the library's to probe
anyway: it connects out to the multiplexers.

## The check

1. **Write down the time in `periodic_task()`.** `serve_forever()` calls
   it after every turn of its loop: after each message, the protocol's
   own included, and after each wait of `poll` seconds that found none.
   A loop that stops stops calling it.

   ```python
   import time

   class Backend(BaseMultiplexerServer):
       def __init__(self, *args, **kwargs):
           super().__init__(*args, **kwargs)
           self.loop_alive_at = time.monotonic()  # read by the health check on its own thread

       def periodic_task(self):
           super().periodic_task()
           self.loop_alive_at = time.monotonic()
   ```

   `time.monotonic()` does not jump with the wall clock, and the
   attribute is one float, replaced whole, which another thread may read
   at any time. `poll` must set a deadline: with a negative or infinite
   `poll` the loop waits for a message before it calls `periodic_task()`
   ([timeouts](../api_python.md#timeouts)), and an idle backend would
   look stuck.
2. **Answer the probe from it**, on the health endpoint's thread:

   ```python
   # Unhealthy once the loop has not come round for STALE_SECONDS,
   # longer than the slowest legitimate handler plus `poll`.
   if time.monotonic() - backend.loop_alive_at > STALE_SECONDS:
       return 503
   ```

   The example's `HealthEndpoint` is that on the standard library's
   `http.server`: `GET /healthz` on a port of its own, each request on a
   thread of its own, so that a connection that never sends its request
   holds up no probe.
3. **Choose `STALE_SECONDS`.** On a healthy backend two calls of
   `periodic_task()` are at most one wait of `poll` seconds, one
   message's handling and one `periodic_task()` apart, so `STALE_SECONDS`
   is longer than those three at their slowest, with room: a probe that
   fails a backend that was only slow restarts it, and its callers lose
   the requests it held. It stays below 90 s, since a
   `BaseMultiplexerServer` handler that runs that long has the backend
   dropped anyway ([which class](../README.md#which-class-to-build-on)).
   The example takes 30 s, with a `poll` of 1 s and handlers that take
   seconds at most. Between a loop that stops and its container's
   restart pass `STALE_SECONDS`, then up to `failureThreshold` ×
   `periodSeconds` of failed probes, then the kill's grace period: in the
   example 30 s, up to 30 s more, and 5 s.
4. **Probe it, beside the drain.** From the example's
   [deployment.yaml](../../examples/health/deployment.yaml), the
   workers' Deployment beside the multiplexers' StatefulSet of
   [operations](../operations.md#on-kubernetes):

   ```yaml
   livenessProbe:
     httpGet: {path: /healthz, port: health}
     periodSeconds: 10
     failureThreshold: 3
     terminationGracePeriodSeconds: 5
   lifecycle:
     preStop:
       exec:
         command: [sh, -c, "touch /tmp/leave; while [ -e /tmp/leave ]; do sleep 0.5; done"]
   ```

   Three failures in a row, ten seconds apart, and the kubelet kills the
   container and starts it again. The preStop hook is the drain
   [operations](../operations.md#restarting-backends) and [how a backend
   leaves](../leaving.md) describe: the file asks the backend to leave,
   `periodic_task()` sees it and calls `start_draining()`, and the
   example's backend removes the file once `serve_forever()` has
   returned, its connections closed. The hook waits for that, since the
   kubelet sends `SIGTERM` as soon as the hook returns and neither the
   library nor the backend handles it, so the signal's default would
   cut the drain short (unless the backend is the container's first
   process, to which the kernel delivers no `SIGTERM` it has no handler
   for). The pod's `terminationGracePeriodSeconds`, 30 s in the example,
   is longer than `drain_seconds`, 20 s, and the close after it. The
   kubelet runs the preStop hook before a kill the probe asks for too,
   and a stuck loop never reads the file, so the probe's own
   `terminationGracePeriodSeconds` gives that kill 5 s rather than the
   pod's 30.
5. **Find the handler that hangs.** `serve_forever(stall_seconds=...)`
   arms `faulthandler.dump_traceback_later` around each message's
   handling and the `periodic_task()` after it, never the poll's wait:
   one that runs longer has every thread's stack written once to
   `stall_file`, stderr by default
   ([BaseMultiplexerServer](../api_python.md#basemultiplexerserver)).
   With `stall_seconds` below `STALE_SECONDS`, the log of a container
   the probe restarted ends with the stack of the handler that hung,
   which `kubectl logs --previous` shows; the example's step 3 has one.
   The watchdog is faulthandler's, one per process. The C++
   `serve_forever()` has no such option.

## In C++

The virtual `periodic_task()` runs after every turn of `serve_forever()`
as in Python ([the C++ API](../api_cpp.md#basemultiplexerserver)); the
time goes into an atomic, since the health check reads it on another
thread.

```cpp
#include <atomic>
#include <chrono>

#include "multiplexer/backend/base_multiplexer_server.h"

class Backend : public multiplexer::backend::BaseMultiplexerServer {
 public:
  explicit Backend(const multiplexer::backend::MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::WORKER) {}

  // Whether the loop has not come round for longer than `limit`; for the
  // health check, on a thread of its own.
  bool stale(std::chrono::steady_clock::duration limit) const {
    const std::chrono::steady_clock::time_point alive_at{std::chrono::steady_clock::duration(loop_alive_at_.load())};
    return std::chrono::steady_clock::now() - alive_at > limit;
  }

 protected:
  void handle_message(multiplexer::MultiplexerMessage& mxmsg) override;

  void periodic_task() override {
    BaseMultiplexerServer::periodic_task();
    loop_alive_at_ = std::chrono::steady_clock::now().time_since_epoch().count();
  }

 private:
  // steady_clock ticks at the loop's last turn: written by the loop, read by the health check.
  std::atomic<std::chrono::steady_clock::rep> loop_alive_at_{std::chrono::steady_clock::now().time_since_epoch().count()};
};
```

## The threaded server classes

`BaseThreadedMultiplexerServer`'s `serve_forever()` thread only waits
and calls `periodic_task()` every `poll` seconds, whatever the workers
do, and its io thread heartbeats and answers searches and `PING`s on
its own. A time written down in `periodic_task()` says that the polling
thread runs, not that a request is being served. A worker stuck in a
handler holds its request while the other workers serve on; with every
worker stuck, requests wait in the queue, `queue_size` of them, and
those beyond it are dropped with a warning and no answer, while the
multiplexers route the backend its share all the time, its connections
heartbeating.

What the class offers: `pending` (`pending()` in C++), the requests
waiting for a worker plus those being handled, and `dropped`
(`dropped()`), those a full queue dropped or leaving refused. Neither
tells a stuck worker from a busy one: `pending` is above zero whenever
there is work, and `dropped` grows only once the queue is full.
`decline_searches_when_full=True` keeps a search from finding a backend
whose every worker is busy, but not the rules: requests still come. So
a check of the workers keeps its own record of when each handler began:

```python
class Backend(BaseThreadedMultiplexerServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lock = threading.Lock()
        self.started: dict[int, float] = {}  # worker thread: when its handler began

    def handle_message(self, request):
        worker = threading.get_ident()
        with self.lock:
            self.started[worker] = time.monotonic()
        try:
            ...  # the work, and request.reply()
        finally:
            with self.lock:
                del self.started[worker]

    def longest_running(self) -> float:
        """Seconds the oldest handler under way has run, 0 when none runs; for the health check."""
        with self.lock:
            oldest = min(self.started.values(), default=None)
        return 0.0 if oldest is None else time.monotonic() - oldest
```

The health check answers 503 once `longest_running()` passes the
slowest legitimate handler with room; `poll` plays no part, since the
workers do not poll. A handler that hands its request to a thread of
its own and returns leaves this record, and `pending` too, and that work
is the program's to watch. In C++ the same takes a mutex around a map
from `std::thread::id` to the time.

## What it does not catch

- **A loop that only slows down.** One that still comes round within
  `STALE_SECONDS` passes, however slowly it serves. Its requests wait in
  its client's incoming queue, 1024 at most, beyond which the client
  drops what arrives (`incoming_queue_full`), and their callers wait out
  their timeouts. That is a question of capacity, and more backends,
  not of a restart.
- **A loop that comes round and serves badly**, every handler raising
  say: each request gets its `BACKEND_ERROR`, and `periodic_task()`
  runs on.
- **A backend that reaches no multiplexer.** Its loop goes on coming
  round, reconnecting every 3 s. `connections_count()`, read in
  `periodic_task()`, since the client belongs to the loop's thread, says
  how many multiplexers it reaches.
- **The time before the restart.** Until the probe has failed enough
  times, the multiplexers route the stuck backend its share, and each
  such request costs its caller a timeout and a search; the requests it
  held when it was killed cost the same ([killed](../leaving.md#killed)).
