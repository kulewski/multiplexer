"""Event client role: a client that sends events and never waits for answers."""

import time

from tests.roles.py import common
from tests.roles.py.common import clients, emit


def main() -> None:
    """Send the --send list in order, reporting each, then linger if asked."""
    p = common.parser("multiplexer event_client role")
    p.add_argument("--send", action="append", help="TYPE:payload, repeatable, sent in order")
    p.add_argument("--to", type=int, default=0, help="direct the message to this instance id")
    p.add_argument("--all", action="store_true", help="send through every connection")
    p.add_argument("--no-flush", action="store_true")
    p.add_argument("--interval", type=float, default=0.0, help="pause between sends")
    p.add_argument("--linger", type=float, default=0.0, help="stay connected this long after sending")
    args = p.parse_args()

    client = clients.Client(common.endpoints(args), type=args.type)
    emit("connected", instance_id=client.instance_id, connections=client.connections_count(), name=args.name)
    sent = 0
    for type_, payload in common.typed_payloads(args.send):
        fields = dict(type=type_)
        if args.to:
            fields["to"] = args.to
        mxmsg = client.new_message(message=payload, **fields)
        state = {}
        try:
            if args.no_flush:
                # Queue only, through the lower-level calls that return the
                # tracker, and report where the message stands right after.
                raw = mxmsg.SerializeToString()
                if args.all:
                    state["connections"] = client.schedule_all(raw)
                else:
                    tracker = client.schedule_one(raw)
                    if tracker:
                        state.update(in_queue=tracker.in_queue(), is_sent=tracker.is_sent(), is_lost=tracker.is_lost())
                    else:
                        state["is_lost"] = True
            else:
                multiplexer = clients.Client.ALL if args.all else clients.Client.ONE
                client.send_message(mxmsg, flush=True, multiplexer=multiplexer)
                state["is_sent"] = True  # a flushing send returns only once written
                if args.all:
                    state["connections"] = client.connections_count()
        except Exception as exc:
            emit("error", kind=common.error_name(exc), type=type_)
            continue
        sent += 1
        emit("sent", type=type_, id=mxmsg.id, **state)
        if args.interval:
            time.sleep(args.interval)
    if args.linger:
        time.sleep(args.linger)
    emit("done", sent=sent, connections=client.connections_count())
    client.shutdown()


if __name__ == "__main__":
    main()
