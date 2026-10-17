"""an idle deployment uses no CPU: every wait loop sleeps, none spins.

A multiplexer, a backend, a synchronous client and a threaded client sit
idle for a while, heartbeats and all. The CPU time of every process over
that window, read from /proc, stays within a few hundredths of a second: a
loop that spins would burn a whole core into it. Its threads are woken a
few times a second, counted from /proc as well, far fewer than a loop
that polls on a short timer, which costs too little CPU to show, is woken
at every turn. This is the check that a wait somewhere did not turn into a
busy loop.
"""

import os
import time
import unittest

from tests import harness
from tests.harness import Cluster, constants as C, cpu_seconds, spawn

IDLE_SECONDS = 4.0  # the window: longer than the 3 s heartbeat interval, so that every heartbeat timer runs in it
CPU_BUDGET_SECONDS = 0.15
# Wakeups a process may have in a second of the window, all its threads
# together: the backends poll every 0.25 s, the heartbeats and the rules
# check come every few seconds, and ThreadSanitizer's own thread wakes ten
# times a second; a loop on a 5 ms timer is woken two hundred times.
WAKEUPS_PER_SECOND = 25
NEVER = 3600  # s the clients idle before their query: they are stopped long before


def voluntary_switches(pid: int) -> int:
    """How many times the process's threads went to sleep and were woken, in
    all, as the kernel counts it: a thread that polls is woken every
    interval, one that waits for events only when one comes."""
    total = 0
    for task in os.listdir("/proc/%d/task" % pid):
        with open("/proc/%d/task/%s/status" % (pid, task)) as status:
            for line in status:
                if line.startswith("voluntary_ctxt_switches:"):
                    total += int(line.split()[1])
    return total


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
            # Their query would come only after an hour: idle all along, they
            # are stopped once the window is over. idle_client and
            # threaded_idle show that a client's call after idling works.
            sync_client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                name="sync",
                type=C.peers.TEST_CLIENT,
                sleep_before=NEVER,
                query=[(C.types.TEST_REQUEST_A, "never sent")],
            )
            threaded_client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                name="threaded",
                type=C.peers.TEST_ACTIVE_CLIENT,
                threaded=True,
                sleep_before=NEVER,
                query=[(C.types.TEST_REQUEST_A, "never sent")],
            )
            sync_client.wait_for("connected", connections=1)
            threaded_client.wait_for("connected", connections=1)
            cluster.wait_for_peer(C.peers.TEST_CLIENT)
            cluster.wait_for_peer(C.peers.TEST_ACTIVE_CLIENT)

            watched = {
                "multiplexer": cluster.mx[0].proc.pid,
                "backend": backend.proc.pid,
                "sync client": sync_client.proc.pid,
                "threaded client": threaded_client.proc.pid,
            }
            before = {name: (cpu_seconds(pid), voluntary_switches(pid)) for name, pid in watched.items()}
            time.sleep(IDLE_SECONDS)  # the window measured, not a wait for anything
            after = {name: (cpu_seconds(pid), voluntary_switches(pid)) for name, pid in watched.items()}
            for name in watched:
                seconds = after[name][0] - before[name][0]
                woken = after[name][1] - before[name][1]
                self.assertLess(seconds, CPU_BUDGET_SECONDS, "%s used %.3f s of CPU while idle" % (name, seconds))
                self.assertLess(
                    woken,
                    WAKEUPS_PER_SECOND * IDLE_SECONDS,
                    "%s was woken %d times in %.1f s while idle" % (name, woken, IDLE_SECONDS),
                )

            for client in (sync_client, threaded_client):
                self.assertEqual([], client.events_of("response") + client.events_of("error"), "idle all along")
            self.assertEqual(0, backend.stop())


if __name__ == "__main__":
    harness.main()
