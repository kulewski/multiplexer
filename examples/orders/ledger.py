"""The ledger: every order the fulfillment workers recorded, a row each in
one SQLite file they share (sqlite3, from the standard library), under the
order's id, the primary key.

    ledger = Ledger("ledger.db")
    ledger.record(order, "w1")   # True: recorded; False: a repeat, skipped and counted

Recording is idempotent. A confirmed send arrives at least once
(docs/delivery.md, "At least once"): an order whose confirmation was lost
with a worker that died comes again, and finds its row there. INSERT OR
IGNORE then leaves the row as it was, and the repeat is only counted, in
the row's `repeats`. Each record() is one transaction, committed before it
returns, so before the worker's handler returns and the library confirms
the order to the shop. Several workers write the file at once: WAL lets
readers go on beside the one writer, and a writer waits for another, up
to `timeout`. A Ledger belongs to the thread that made it, as its sqlite3
connection does."""

import sqlite3

from orders_pb2 import Order

SCHEMA = """CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    customer TEXT NOT NULL,
    item TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    worker TEXT NOT NULL,
    repeats INTEGER NOT NULL DEFAULT 0
)"""


class Ledger:
    """One connection to the ledger's file, made with the file and its table when there are none."""

    def __init__(self, path: str, timeout: float = 30.0):
        """The ledger in the file at `path`, made when there is none; a
        write waits up to `timeout` seconds for another worker's."""
        self.db = sqlite3.connect(path, timeout=timeout)
        try:
            self.db.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            # Two workers switching a new file at once: one is told at once
            # that it is locked, and its second try waits for the other's switch.
            self.db.execute("PRAGMA journal_mode=WAL")
        # A commit is in the file once it returns, so a worker that dies
        # loses nothing it committed. NORMAL leaves the last commits to a
        # power cut; FULL would keep those too, at an fsync each.
        self.db.execute("PRAGMA synchronous=NORMAL")
        with self.db:
            self.db.execute(SCHEMA)

    def record(self, order: Order, worker: str) -> bool:
        """Record `order` as `worker`'s, committed when this returns: True
        when it is new, False when the ledger has it already, a repeat,
        which leaves its row as it was but for the count."""
        with self.db:  # one transaction: committed at the end of the block, rolled back if it raises
            inserted = self.db.execute(
                "INSERT OR IGNORE INTO orders (id, customer, item, quantity, worker) VALUES (?, ?, ?, ?, ?)",
                (order.id, order.customer, order.item, order.quantity, worker),
            )
            if inserted.rowcount == 1:
                return True
            self.db.execute("UPDATE orders SET repeats = repeats + 1 WHERE id = ?", (order.id,))
            return False

    def count(self) -> int:
        """How many orders the ledger holds."""
        return self.db.execute("SELECT COUNT(*) FROM orders").fetchone()[0]

    def by_worker(self) -> dict[str, int]:
        """How many orders each worker recorded first, by its name."""
        return dict(self.db.execute("SELECT worker, COUNT(*) FROM orders GROUP BY worker ORDER BY worker"))

    def repeats(self) -> dict[str, tuple[int, int]]:
        """The repeats skipped, by the worker that recorded the order first:
        how many of its orders came again, and how many times in all."""
        rows = self.db.execute(
            "SELECT worker, COUNT(*), SUM(repeats) FROM orders WHERE repeats > 0 GROUP BY worker ORDER BY worker"
        )
        return {worker: (orders, times) for worker, orders, times in rows}

    def close(self) -> None:
        """Close the connection; what was recorded is in the file."""
        self.db.close()
