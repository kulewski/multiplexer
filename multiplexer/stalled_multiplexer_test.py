"""A synchronous client keeps a connection that is fine through a stall of
its multiplexer. The client runs the loop only inside its calls: a call
during the stall runs out the first phase of the drop, 30 s of silence, and
arms the second, 60 s; the multiplexer comes back and sends, into a socket
the idle client does not read; and the next call, past the second phase,
has its expired wait and those frames come due in one pass of the loop.
The read re-armed the timer, which cannot take back a wait the pass
already collected, and the client shut the connection down, to open it
again 3 s later inside a later call. The client's type is passive, so the
multiplexer requires nothing of it and only the client's side can drop.
Timed by the drop's own two phases, each call well past one; about 110 s,
tagged slow.
"""

import time
import unittest

from multiplexer import _native
from multiplexer.clients import SyncClient
from multiplexer.multiplexer_constants import peers
from multiplexer.mxclient import OperationTimedOut
from multiplexer.testing import Cluster, runfile

RULES = runfile("tests/testing.rules")
FIRST_PHASE = _native.NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL  # seconds of silence before the drop's first phase
SECOND_PHASE = _native.NO_HEARTBIT_SO_REALLY_DROP_INTERVAL  # and from it to the second
MARGIN = 10  # seconds each call comes past the phase it is after: run out by then, however loaded the machine


def short_call(client: SyncClient) -> None:
    """A call that runs the loop briefly, firing what came due meanwhile."""
    try:
        client.receive_message(timeout=0.2)
    except OperationTimedOut:
        pass


def sleep_until(moment: float) -> None:
    """Until time.monotonic() reaches `moment`."""
    time.sleep(max(0.0, moment - time.monotonic()))


class StalledMultiplexerTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_call_after_a_stall_keeps_the_connection(self):
        with Cluster(1, rules=RULES) as cluster:
            multiplexer = cluster.mx[0]
            client = SyncClient(cluster.endpoints, type=peers.TEST_CLIENT)
            connected = time.monotonic()  # the multiplexer's welcome was the last frame read
            multiplexer.pause()  # from here it sends nothing, the heartbeat it owes the client's welcome included
            self.assertEqual(1, client.connections_count())

            sleep_until(connected + FIRST_PHASE + MARGIN)
            short_call(client)  # runs out the first phase, arming the second
            first_call = time.monotonic()
            multiplexer.resume()  # its heartbeats now wait in the client's socket

            sleep_until(first_call + SECOND_PHASE + MARGIN)
            short_call(client)  # the second phase's expired wait and the frames waiting, in one pass
            self.assertEqual(1, client.connections_count(), "the client dropped a connection that was fine")
            client.shutdown()


if __name__ == "__main__":
    unittest.main()
