"""Event backend role: a backend that receives events, reports them and never answers."""

import time

from tests.roles.py import common
from tests.roles.py.common import emit, servers


class EventBackend(servers.BaseMultiplexerServer):
    """Receives events, reports each one and never answers."""

    received = 0

    def handle_message(self, mxmsg):
        """Report one received event."""
        self.received += 1
        emit(
            "received",
            type=mxmsg.type,
            id=mxmsg.id,
            from_=mxmsg.from_,
            to=mxmsg.to,
            **common.payload_summary(mxmsg.message),
        )
        self.no_response()


def main() -> None:
    """Parse the options and receive until SIGTERM, --until N or --for S."""
    p = common.parser("multiplexer event_backend role")
    p.add_argument("--until", type=int, default=0, help="exit after N messages")
    p.add_argument("--for", dest="duration", type=float, default=0.0, help="exit after S seconds")
    args = p.parse_args()
    common.stop_on_sigterm()

    event_backend = EventBackend(common.endpoints(args), type=args.type)
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
        try:
            event_backend.loop_iter(timeout=0.25)
        except common.OperationTimedOut:
            continue
    emit("done", received=event_backend.received)
    event_backend.close()


if __name__ == "__main__":
    main()
