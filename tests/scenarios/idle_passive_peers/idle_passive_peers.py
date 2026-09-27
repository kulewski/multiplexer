"""idle passive peers cost the multiplexer no wakeups, and a frame one sends is owed a heartbeat.

A passive peer is sent one heartbeat per frame it sends, so that one that
reads only inside its calls never finds a pile of them. Twenty passive
peers connect 150 ms apart, so that their connections' timers fall
apart, and each is sent the one heartbeat its welcome is owed. Over the
next 3.5 s, longer than the 3 s heartbeat interval, the multiplexer's
threads are woken a handful of times at most, counted from /proc, where
every idle passive connection's timer woke them every interval to find
nothing owed. Then one peer sends a heartbeat and is sent one back. The
periodic rules check is off, so that only the connections could wake the
multiplexer.
"""

import os
import time
import unittest

from tests import harness
from tests.harness import Cluster, constants as C
from multiplexer.testing.raw_peer import RawPeer

PEERS = 20
SPREAD = 0.15  # s between connections: their timers 3 s apart in all, a wakeup each per interval
QUIET = 3.5  # s counted, longer than the 3 s heartbeat interval: every such timer fires meanwhile
HANDFUL = 5  # wakeups an idle multiplexer may have in that time; the timers made some twenty
BOUND = 30  # seconds a wait for a heartbeat may take: a failure detector only


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


class IdlePassivePeers(unittest.TestCase):
    """Idle passive peers wake the multiplexer for nothing; a frame from one is owed its heartbeat."""

    def test_idle_passive_peers_cost_no_wakeups(self):
        with Cluster(1, rules=harness.CONFIG.rules, rules_check_interval=0) as cluster:
            pid = cluster.mx[0].proc.pid
            idle = []
            for _ in range(PEERS):
                peer = RawPeer(cluster.endpoints[0], C.peers.TEST_CLIENT)
                peer.handshake()
                idle.append(peer)
                time.sleep(SPREAD)
            for peer in idle:
                peer.receive_type(C.types.HEARTBIT, timeout=BOUND)  # the one its welcome is owed
            before = voluntary_switches(pid)
            time.sleep(QUIET)
            woken = voluntary_switches(pid) - before
            self.assertLess(woken, HANDFUL, "the multiplexer woke for idle passive peers")

            idle[0].send(b"", C.types.HEARTBIT)
            idle[0].receive_type(C.types.HEARTBIT, timeout=BOUND)  # the one this frame is owed
            for peer in idle:
                peer.close()


if __name__ == "__main__":
    harness.main()
