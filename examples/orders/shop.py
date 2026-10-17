"""Shop: what the shop's web processes place orders with. One ORDER_PLACED
event per order, sent as a confirmed send (docs/delivery.md): the library
keeps every order until a fulfillment worker's library says the worker
recorded it, sends it again until then, and tells the order's callback 1
once it was, 0 once it gave up on it.

    with Shop(addresses) as shop:
        shop.place(Order(id=new_order_id(), customer="alice", item="mug", quantity=2), callback)

One ThreadedClient connected to every multiplexer, its io thread sending
what place() hands it and hearing the confirmations; every method is safe
from any thread, which is what a web process's threads need. The orders
go in batches, an order waiting at most MAX_DELAY for others, or one by
one, each at once. What nothing confirmed for RETRY_AFTER goes again; an
order not confirmed within TIMEOUT of its batch's first order is given up
on, its callback hearing 0 and `on_drop` NOT_CONFIRMED, and so is one a
worker's handler raised on, FAILED. A flush, close() and the end of a
`with` block let every batch go at once and wait for the confirmations."""

import uuid
from typing import Callable

from multiplexer.endpoints import Endpoint, parse_endpoint
from multiplexer.mxclient import DropReason
from multiplexer.threaded_client import ThreadedClient

from multiplexer_constants import peers, types
from orders_pb2 import Order

MAX_DELAY = 0.005  # seconds an order waits for others to share its batch
RETRY_AFTER = 2.0  # seconds without a confirmation after which a batch's orders go again
TIMEOUT = 60.0  # seconds a batch has, from its first order, to be confirmed


def parse_addresses(text: str) -> list[Endpoint]:
    """ "host:port,host:port", an IPv6 address as [address]:port, as the list the library takes."""
    return [parse_endpoint(item) for item in text.split(",")]


def new_order_id() -> str:
    """A fresh order id, as the shop gives one to an order when it is placed."""
    return uuid.uuid4().hex


class Shop:
    """One ThreadedClient connected to every multiplexer, its orders confirmed sends."""

    def __init__(
        self,
        addresses: list[Endpoint],
        one_by_one: bool = False,
        confirm: bool = True,
        max_delay: float = MAX_DELAY,
        timeout: float = TIMEOUT,
        on_drop: Callable[[int, DropReason], None] | None = None,
    ):
        """Connect to every multiplexer, all at once, waiting ten seconds at
        most; one that is not reachable yet is tried again in the
        background. `one_by_one` sends every order at once, alone, rather
        than in batches; `confirm` False sends them as plain events, done
        once written. `on_drop(message_id, reason)` hears of every order
        given up on, on the io thread."""
        self._client = ThreadedClient([], type=peers.SHOP, on_drop=on_drop)
        self._client.set_delivery(
            types.ORDER_PLACED,
            batch=not one_by_one,
            max_delay=max_delay,
            confirm=confirm,
            retry_after=RETRY_AFTER,
            timeout=timeout,
        )
        self._client.connect_all(addresses)

    def place(self, order: Order, callback: Callable[[int], None] | None = None) -> int:
        """Send `order` and return its message id, which `on_drop` names;
        returns at once. `callback(1)` runs on the io thread once a worker
        recorded the order, `callback(0)` once the shop gave up on it."""
        return self._client.send_message(order, type=types.ORDER_PLACED, callback=callback)

    def flush(self, timeout: float = TIMEOUT) -> bool:
        """Let every batch go, and wait until every order placed before the
        call was confirmed or given up on, or `timeout` seconds: whether
        every one was confirmed."""
        return self._client.flush_all(timeout)

    @property
    def sent_again(self) -> int:
        """How many times the shop sent an order again, nothing having confirmed it."""
        return self._client.sent_again()

    def close(self) -> None:
        """Let the batches go and wait for their confirmations, a second at
        most, giving up on the rest, then disconnect; the shop is done."""
        self._client.shutdown()

    def __enter__(self) -> "Shop":
        """The shop, for a `with` block, at whose end it is closed."""
        return self

    def __exit__(self, *exc: object) -> None:
        """close(), however the block ended."""
        self.close()
