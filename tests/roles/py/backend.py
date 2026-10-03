"""Backend role: a backend serving request types until stopped.

--serves REQUEST=RESPONSE maps each request type to the reply type;
--behaviour says what to do with the payload (upper, echo, drop, raise,
sleep:MS); --crash-after N exits with status 3 after N requests. The role
stops on SIGTERM or when its --drain-file appears; with --drain-seconds it
drains first (tells the multiplexers to route it nothing new, keeps serving
what arrives) for at most that long, and with --drain-min-handled N it
refuses to leave before N requests were served. --drain-routing names the
Routing flags kept on while draining, a comma-separated subset of any, all
and last_resort; none by default. --exit-on-exception makes a handler
exception end the process with status 4. --threaded serves the same on
BaseThreadedMultiplexerServer, the handler on a worker thread, with the
same options and events.
"""

import os
import sys
import time

from tests.roles.py import common
from tests.roles.py.common import drain_routing, emit, servers, threaded_server


class Leaving:
    """What both backends do about leaving: notice the request, drain or
    stop, report the multiplexers' acknowledgement of the drain routing
    once, and refuse to leave before --drain-min-handled requests. Mixed
    into the two classes, which supply `draining`, `start_draining()`,
    `acknowledged()`, `handled` and `working`."""

    # Only what the server classes do not define: `draining`, `working`
    # and `start_draining()` are theirs, and this comes first in the
    # method order so that its periodic_task() is the one called.
    drain_seconds = 0.0
    drain_file: str | None = None
    drain_min_handled = 0
    exit_on_exception = False
    handled = 0
    acked = False
    drain_started = 0.0

    def acknowledged(self) -> bool:
        """Whether every multiplexer has the drain routing in effect."""
        return self.conn.routing_acknowledged()

    def drained(self):
        """The drain is over and at least --drain-min-handled requests were served."""
        return super().drained() and self.handled >= self.drain_min_handled


class ThreadedBackend(Leaving, threaded_server.BaseThreadedMultiplexerServer):
    """The same backend on the threaded class: the handler runs on a worker
    thread and answers through the request."""

    def __init__(
        self,
        addresses,
        type,
        serves,
        behaviour,
        crash_after,
        memory_every=0,
        drain_seconds=0.0,
        drain_file=None,
        drain_min_handled=0,
        exit_on_exception=False,
        drain_routing=None,
    ):
        super().__init__(addresses, type=type, drain_routing=drain_routing)
        self.serves = serves
        self.behaviour = behaviour
        self.crash_after = crash_after
        self.memory_every = memory_every
        self.drain_seconds = drain_seconds
        self.drain_file = drain_file
        self.drain_min_handled = drain_min_handled
        self.exit_on_exception = exit_on_exception
        self.handled = 0
        self.acked = False

    def handle_message(self, request):
        """Report the request, then answer, drop, raise or crash as configured; on a worker."""
        mxmsg = request.mxmsg
        self.handled += 1
        if self.memory_every and self.handled % self.memory_every == 0:
            common.memory_event(self.handled)
        emit("request", type=mxmsg.type, id=mxmsg.id, sender=mxmsg.sender, size=len(mxmsg.message))
        response_type = self.serves.get(mxmsg.type)
        if response_type is None:
            emit("unexpected", type=mxmsg.type, id=mxmsg.id)
            request.no_response()
            return
        if self.behaviour == "drop":
            request.no_response()
        elif self.behaviour == "raise":
            raise RuntimeError("handler failed on purpose")
        else:
            if self.behaviour.startswith("sleep:"):
                time.sleep(int(self.behaviour.split(":", 1)[1]) / 1000.0)
            payload = mxmsg.message.upper() if self.behaviour == "upper" else mxmsg.message
            request.reply(payload, type=response_type)
        if self.crash_after and self.handled >= self.crash_after:
            emit("crash", handled=self.handled)
            os._exit(3)

    def acknowledged(self) -> bool:
        return self.client.routing_acknowledged()

    def drained(self):
        """The drain is over and at least --drain-min-handled requests were served."""
        return super().drained() and self.handled >= self.drain_min_handled


class Backend(Leaving, servers.BaseMultiplexerServer):
    """The backend; see the module docstring for what the options do."""

    def __init__(
        self,
        addresses,
        type,
        serves,
        behaviour,
        crash_after,
        memory_every=0,
        drain_seconds=0.0,
        drain_file=None,
        drain_min_handled=0,
        exit_on_exception=False,
        drain_routing=None,
    ):
        super().__init__(addresses, type=type, drain_routing=drain_routing)
        self.serves = serves
        self.behaviour = behaviour
        self.crash_after = crash_after
        self.memory_every = memory_every
        self.drain_seconds = drain_seconds
        self.drain_file = drain_file
        self.drain_min_handled = drain_min_handled
        self.exit_on_exception = exit_on_exception
        self.handled = 0
        self.acked = False

    def handle_message(self, mxmsg):
        """Report the request, then answer, drop, raise or crash as configured."""
        self.handled += 1
        if self.memory_every and self.handled % self.memory_every == 0:
            common.memory_event(self.handled)
        emit("request", type=mxmsg.type, id=mxmsg.id, sender=mxmsg.sender, size=len(mxmsg.message))
        response_type = self.serves.get(mxmsg.type)
        if response_type is None:
            emit("unexpected", type=mxmsg.type, id=mxmsg.id)
            self.no_response()
            return
        if self.behaviour == "drop":
            self.no_response()
        elif self.behaviour == "raise":
            raise RuntimeError("handler failed on purpose")
        else:
            if self.behaviour.startswith("sleep:"):
                time.sleep(int(self.behaviour.split(":", 1)[1]) / 1000.0)
            payload = mxmsg.message.upper() if self.behaviour == "upper" else mxmsg.message
            self.send_message(message=payload, type=response_type)
        if self.crash_after and self.handled >= self.crash_after:
            emit("crash", handled=self.handled)
            os._exit(3)

    def periodic_task(self):
        """Notice a request to leave, from SIGTERM or the drain file: stop at
        once, or start draining when a drain was configured; report the
        multiplexers' acknowledgement of the drain routing once."""
        if self.draining:
            if not self.acked and self.conn.routing_acknowledged():
                self.acked = True
                emit("acked", ms=(time.monotonic() - self._draining_since) * 1000)
            return
        if common.STOP.is_set() or (self.drain_file and os.path.exists(self.drain_file)):
            if not self.drain_seconds and not self.drain_min_handled:
                self.working = False
                return
            self.start_draining()  # the multiplexers route it nothing new; keep serving what arrives
            emit("draining", drain_seconds=self.drain_seconds)

    def drained(self):
        """The drain period is over and at least --drain-min-handled requests were served."""
        return super().drained() and self.handled >= self.drain_min_handled

    def on_handler_exception(self, exc):
        """Keep serving, unless --exit-on-exception."""
        return not self.exit_on_exception


def main() -> None:
    """Parse the options and serve until asked to leave."""
    p = common.parser("multiplexer backend role")
    p.add_argument("--serves", action="append", help="REQUEST_TYPE=RESPONSE_TYPE, repeatable")
    p.add_argument("--behaviour", default="upper", help="upper | echo | drop | raise | sleep:MS")
    p.add_argument("--crash-after", type=int, default=0, help="exit(3) after N handled requests")
    p.add_argument("--memory-every", type=int, default=0, help="emit a memory event every N requests")
    p.add_argument(
        "--drain-seconds",
        type=float,
        default=0.0,
        help="when asked to leave, drain: the multiplexers route it nothing new, it serves what arrives, at most this long",
    )
    p.add_argument("--drain-file", default=None, help="a file whose appearance asks the backend to leave")
    p.add_argument("--drain-min-handled", type=int, default=0, help="do not leave before N requests were served")
    p.add_argument("--drain-routing", default="", help="Routing flags kept on while draining: any,all,last_resort")
    p.add_argument("--exit-on-exception", action="store_true", help="a handler exception ends the process (4)")
    p.add_argument("--threaded", action="store_true", help="serve on BaseThreadedMultiplexerServer instead")
    args = p.parse_args()
    common.stop_on_sigterm()

    common.start_memory_tracing(args.memory_every)
    backend = (ThreadedBackend if args.threaded else Backend)(
        common.endpoints(args),
        args.type,
        common.kv_ints(args.serves),
        args.behaviour,
        args.crash_after,
        args.memory_every,
        args.drain_seconds,
        args.drain_file,
        args.drain_min_handled,
        args.exit_on_exception,
        drain_routing(args.drain_routing),
    )
    backend.connect()  # the harness waits for this event's connection count before it sends
    client = backend.client if isinstance(backend, ThreadedBackend) else backend.conn
    emit("connected", instance_id=client.instance_id, connections=client.connections_count(), name=args.name)
    try:
        backend.serve_forever(poll=0.25, drain_seconds=args.drain_seconds)
    except Exception as exc:  # the handler's exception, let through by on_handler_exception
        emit("handler_exception", kind=common.error_name(exc), handled=backend.handled)
        sys.exit(4)
    emit("stopped", handled=backend.handled)


if __name__ == "__main__":
    main()
