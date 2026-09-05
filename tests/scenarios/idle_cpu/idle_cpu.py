"""an idle deployment uses no CPU: every wait loop sleeps, none spins.

A multiplexer, a backend, a synchronous client and a threaded client sit
idle for a while, heartbeats and all. The CPU time of every process over
that window, read from /proc, stays within a few hundredths of a second: a
loop that spins would burn a whole core into it. This is the check that a
wait somewhere did not turn into a busy loop.
"""

import time
import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

IDLE_SECONDS = 8.0
CPU_BUDGET_SECONDS = 0.15


class IdleCpu(unittest.TestCase):
    """Checks that idle processes use no CPU."""

    def test_idle_processes_do_not_spin(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
            )
            backend.wait_for("connected", connections=1)
            idle_before_query = IDLE_SECONDS + 4
            sync_client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                name="sync",
                type=C.peers.TEST_CLIENT,
                sleep_before=idle_before_query,
                query=[(C.types.TEST_REQUEST_A, "after idling")],
            )
            threaded_client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                name="threaded",
                type=C.peers.TEST_ACTIVE_CLIENT,
                threaded=True,
                sleep_before=idle_before_query,
                query=[(C.types.TEST_REQUEST_A, "after idling")],
            )
            sync_client.wait_for("connected")
            threaded_client.wait_for("connected")
            time.sleep(1)  # let the handshakes and the first heartbeats settle

            watched = {
                "multiplexer": cluster.mx[0].cpu_seconds,
                "backend": backend.cpu_seconds,
                "sync client": sync_client.cpu_seconds,
                "threaded client": threaded_client.cpu_seconds,
            }
            before = {name: read() for name, read in watched.items()}
            time.sleep(IDLE_SECONDS)
            used = {name: read() - before[name] for name, read in watched.items()}
            for name, seconds in used.items():
                self.assertLess(seconds, CPU_BUDGET_SECONDS, "%s used %.3f s of CPU while idle" % (name, seconds))

            self.assertEqual(0, sync_client.wait(timeout=30))
            self.assertEqual(0, threaded_client.wait(timeout=30))
            self.assertEqual(1, len(sync_client.events_of("response")), "the idle sync client still works")
            self.assertEqual(1, len(threaded_client.events_of("response")), "the idle threaded client still works")
            self.assertEqual(0, backend.stop())


if __name__ == "__main__":
    harness.main()
