"""A zero timeout reads what has already arrived: receive_message(0), and
loop_iter(timeout=0) and serve_forever(poll=0) through it, run the loop
once without waiting before they give up, where they gave up before
looking, and a message that came while no call ran the loop stayed in the
socket. Ordered, not timed: the message is in the client's socket, as
/proc/net/tcp shows, before the call.
"""

import unittest

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationTimedOut
from multiplexer.testing import Cluster, runfile, wait_until
from multiplexer.testing.raw_peer import RawPeer, frame

RULES = runfile("tests/testing.rules")


def waiting_bytes(remote_port: int, other_than: int) -> int:
    """The bytes waiting to be read on the established IPv4 socket
    connected to `remote_port` from a local port other than `other_than`,
    from /proc/net/tcp; 0 while there is none. A connection to the same
    port that ended earlier stays in the table a while, in TIME_WAIT and
    empty, and may come first: only an established one counts."""
    with open("/proc/net/tcp") as table:
        next(table)  # the header
        for line in table:
            fields = line.split()
            local, remote = int(fields[1].split(":")[1], 16), int(fields[2].split(":")[1], 16)
            if remote == remote_port and local != other_than and fields[3] == "01":  # ESTABLISHED
                return int(fields[4].split(":")[1], 16)  # tx_queue:rx_queue
    return 0


class ZeroTimeoutTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_zero_timeout_reads_what_has_arrived(self) -> None:
        with Cluster(1, rules=RULES) as cluster:
            port = cluster.endpoints[0][1]
            client = Client(cluster.endpoints, type=peers.PYTHON_TEST_SERVER)
            sender = RawPeer(cluster.endpoints[0], peers.WEBSITE)
            try:
                sender.handshake()
                with self.assertRaises(OperationTimedOut):
                    client.receive_message(timeout=0)  # nothing there yet
                message = sender.message(b"waiting", types.PYTHON_TEST_REQUEST, to=client.instance_id)
                sent = frame(message.SerializeToString())
                sender.send_raw(sent)
                mine = sender.sock.getsockname()[1]
                wait_until(lambda: waiting_bytes(port, mine) >= len(sent), 10, "the message in the client's socket")
                received, _ = client.receive_message(timeout=0)
                self.assertEqual(message.id, received.id)
            finally:
                sender.close()
                client.shutdown()


if __name__ == "__main__":
    unittest.main()
