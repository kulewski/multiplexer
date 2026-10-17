"""A fulfillment worker: a backend on `BaseMultiplexerServer` that records
every order it gets in the ledger, the SQLite file every worker shares.

    python worker.py [ADDRESSES] --name NAME [--ledger FILE] [--crash-at N]

ADDRESSES is host:port of every multiplexer, comma-separated, default
127.0.0.1:1980. The shop sends its orders as confirmed sends: the library
confirms an order to the shop once handle_message() returned from it, the
order committed to the ledger by then, and FAILED when it raised, on an
order no worker can record, which the shop then gives up on rather than
send again. An order may come twice, its confirmation lost with a worker
that died: the ledger records it once and counts the repeat. With
--crash-at N the worker dies at the Nth order it gets, before recording
it, as a crash would end it: what it held goes again, to the workers left.
SIGTERM makes it leave: the multiplexers route it nothing new, it records
what was on its way, confirms it and exits."""

import argparse
import os
import signal
import sys

from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.mxclient import CLOSE_FLUSH_SECONDS
from multiplexer.servers import BaseMultiplexerServer

from ledger import Ledger
from multiplexer_constants import peers, types
from orders_pb2 import Order
from shop import parse_addresses


class Worker(BaseMultiplexerServer):
    """The ledger, and the handler that records each order in it; on the serve loop's one thread."""

    multiplexer_client_type = peers.FULFILLMENT_WORKER

    def __init__(self, addresses: list[tuple[str, int]], name: str, ledger: str, crash_at: int = 0):
        """The worker `name` for the multiplexers in `addresses`, recording
        in the ledger at the path `ledger`, dying at its `crash_at`th order
        when that is not 0; connect() or serve_forever() connects."""
        super().__init__(addresses)
        self.name = name
        self.ledger = Ledger(ledger)
        self.crash_at = crash_at
        self.taken = 0  # the orders handle_message() got
        self.recorded = 0
        self.repeats = 0
        self.leave = False  # set by SIGTERM; periodic_task() starts the drain
        self.ready_pending = False  # set by main(): "ready" is printed once a multiplexer welcomed the worker

    def handle_message(self, mxmsg: MultiplexerMessage) -> None:
        """An order recorded in the ledger, once, committed before this returns."""
        self.no_response()  # an event: no answer, not even the BACKEND_ERROR a raise would send the shop
        if mxmsg.type != types.ORDER_PLACED:
            return  # nothing else is routed here
        self.taken += 1
        if self.taken == self.crash_at:
            print(f"{self.name}: dying at order {self.taken}, before recording it", flush=True)
            os._exit(3)  # no cleanup, no confirmation: as a crash, or kill -9, ends it
        order = Order.FromString(mxmsg.message)
        if order.quantity < 1:
            raise ValueError(f"order {order.id} is for no {order.item}")  # FAILED: the shop gives up on it
        if self.ledger.record(order, self.name):
            self.recorded += 1
        else:
            self.repeats += 1

    def periodic_task(self) -> None:
        """After every message and every poll: the drain started once
        SIGTERM came, and "ready" said once a multiplexer welcomed the worker."""
        if self.leave:
            self.start_draining()
        self.say_ready_once_connected()

    def say_ready_once_connected(self) -> None:
        """Print the "ready" main() asked for once a multiplexer has
        welcomed the worker: whoever runs the steps, and the test, wait
        for the line and then place orders."""
        if self.ready_pending and self.conn.connections_count() > 0:
            print(f"ready: {self.name}", flush=True)
            self.ready_pending = False

    def close(self, timeout: float = CLOSE_FLUSH_SECONDS) -> None:
        """The connections closed, what the worker owes the shop confirmed
        first, then the ledger."""
        super().close(timeout)
        self.ledger.close()


def main() -> None:
    """Serve until SIGTERM, then leave."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "addresses", nargs="?", default="127.0.0.1:1980", help="host:port of every multiplexer, comma-separated"
    )
    parser.add_argument("--name", required=True, help="the worker's name, which the ledger records its orders under")
    parser.add_argument("--ledger", default="ledger.db", help="the ledger's file, which every worker shares")
    parser.add_argument("--crash-at", type=int, default=0, help="die at the Nth order, before recording it")
    args = parser.parse_args()
    worker = Worker(parse_addresses(args.addresses), args.name, args.ledger, args.crash_at)

    def leave(signum: int, frame: object) -> None:
        """SIGTERM: a flag the serve loop reads, between two messages."""
        worker.leave = True

    # The handler runs when the serve loop's wait comes back to Python, at
    # the latest once its poll is over.
    signal.signal(signal.SIGTERM, leave)
    worker.ready_pending = True
    worker.connect()  # waits for the handshakes, so that "ready" can come at once
    worker.say_ready_once_connected()
    if worker.ready_pending:
        print("no multiplexer reachable yet; ready once one is", file=sys.stderr, flush=True)
    worker.serve_forever(poll=0.5, drain_seconds=5)
    print(
        f"{worker.name} left after recording {worker.recorded:,} orders and skipping {worker.repeats:,} repeats",
        flush=True,
    )


if __name__ == "__main__":
    main()
