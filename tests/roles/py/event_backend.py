"""Event backend role: a backend that receives events, reports them and never answers.

With --crash-at N it exits with status 3 at the N-th message, before
handling it, reporting `crash`: the messages of a confirmed batch it holds
go unconfirmed, as with a consumer that dies. With --drain-file it starts draining when the file appears, with the
--drain-routing flags kept on (a comma-separated subset of any, all and
last_resort; none by default), and keeps looping until --until, --for or
SIGTERM, reporting `draining` and then `acked` once every multiplexer has
the routing in effect.
"""

import os
import time

from tests.roles.py import common
from tests.roles.py.common import drain_routing, emit, servers


class EventBackend(servers.BaseMultiplexerServer):
    """Receives events, reports each one and never answers."""

    received = 0
    acked = False
    crash_at = 0

    def handle_message(self, mxmsg):
        """Report one received event."""
        if self.crash_at and self.received + 1 >= self.crash_at:
            emit("crash", received=self.received)
            os._exit(3)
        self.received += 1
        emit(
            "received",
            type=mxmsg.type,
            id=mxmsg.id,
            sender=mxmsg.sender,
            to=mxmsg.to,
            **common.payload_summary(mxmsg.message),
            **common.key_fields(mxmsg),
        )
        self.no_response()


def main() -> None:
    """Parse the options and receive until SIGTERM, --until N or --for S."""
    p = common.parser("multiplexer event_backend role")
    p.add_argument("--until", type=int, default=0, help="exit after N messages")
    p.add_argument("--for", dest="duration", type=float, default=0.0, help="exit after S seconds")
    p.add_argument("--crash-at", type=int, default=0, help="exit with 3 at the N-th message, before handling it")
    p.add_argument("--drain-file", default=None, help="a file whose appearance starts a drain")
    p.add_argument("--drain-routing", default="", help="Routing flags kept on while draining: any,all,last_resort")
    p.add_argument(
        "--key-filter", action="append", help="TYPE:PATTERN, repeatable: the key filters, set before connecting"
    )
    args = p.parse_args()
    common.stop_on_sigterm()

    event_backend = EventBackend(
        common.endpoints(args), type=args.type, drain_routing=drain_routing(args.drain_routing)
    )
    event_backend.crash_at = args.crash_at
    if args.key_filter:
        event_backend.set_key_filters(common.key_filters(args.key_filter))
    event_backend.connect()  # the loop below is its own; connect() is what serve_forever() would do first
    emit(
        "connected",
        instance_id=event_backend.conn.instance_id,
        connections=event_backend.conn.connections_count(),
        name=args.name,
    )
    deadline = time.time() + args.duration if args.duration else None
    while not common.STOP.is_set():
        if args.until and event_backend.received >= args.until:
            break
        if deadline and time.time() >= deadline:
            break
        if args.drain_file and not event_backend.draining and os.path.exists(args.drain_file):
            event_backend.start_draining()
            emit("draining", drain_seconds=0.0)
        if event_backend.draining and not event_backend.acked and event_backend.conn.routing_acknowledged():
            event_backend.acked = True
            emit("acked", ms=0.0)
        try:
            event_backend.loop_iter(timeout=0.25)
        except common.OperationTimedOut:
            continue
    emit("done", received=event_backend.received)
    event_backend.close()


if __name__ == "__main__":
    main()
