"""worker threads share one ThreadedClient; each gets its replies through its own queue.

Every thread issues queries with query with a callback, keeps working meanwhile, and
takes the replies from its own queue, which the client's io thread fills.
No thread blocks on the network and none is involved in another's replies.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

WORKERS = 4
QUERIES = 10


class ThreadedWorkers(unittest.TestCase):
    """Checks that worker threads share one ThreadedClient; each gets its replies through its own queue."""

    def test_every_worker_gets_its_own_replies(self):
        cfg = harness.CONFIG
        with Cluster(cfg.mx) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
            )
            backend.wait_for("connected", connections=cfg.mx)
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_ACTIVE_CLIENT,
                workers=WORKERS,
                count=QUERIES,
                sleep_between=0.02,
                query=[(C.types.TEST_REQUEST_A, "w{worker} q{round}")],
            )
            self.assertEqual(0, client.wait())
            self.assertEqual([], client.events_of("error"))
            connected = client.events_of("connected")
            self.assertEqual(1, len(connected), "one client shared by every worker")
            self.assertEqual(WORKERS, connected[0]["workers"])
            for w in range(WORKERS):
                mine = client.events_of("response", worker=w)
                self.assertEqual(QUERIES, len(mine))
                self.assertEqual(sorted("W%d Q%d" % (w, r) for r in range(QUERIES)), sorted(r["payload"] for r in mine))
            dones = client.events_of("done")
            self.assertEqual(WORKERS, len(dones))
            self.assertEqual({connected[0]["instance_id"]}, {d["instance_id"] for d in dones})
            self.assertEqual(WORKERS * QUERIES, len(backend.events_of("request")))
            self.assertEqual(0, backend.stop())


if __name__ == "__main__":
    harness.main()
