"""The multiplexer's test infrastructure used from another workspace, on
this example's rules: the shapes a program's tests need most, in-process.

Run through mx_integration_test (see BUILD), which names the rules file:
the one @mx//:multiplexer_rules points at, echo.rules here, so the
multiplexer routes the types the generated constants carry.
"""

import unittest

from multiplexer import recording, testing
from multiplexer.Recording_pb2 import RoutedMessage
from multiplexer.clients import BackendError
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, FakePeer, TestClient


class HarnessTest(unittest.TestCase):
    """A FakePeer plays the echo backend; a TestClient asks it."""

    def test_fake_backend_answers_the_client(self):
        with (
            Cluster(1) as cluster,
            FakePeer(cluster, peers.ECHO_BACKEND) as backend,
            TestClient(cluster, peers.ECHO_CLIENT) as client,
        ):
            backend.on(types.ECHO_REQUEST, lambda mxmsg: mxmsg.message.upper(), types.ECHO_RESPONSE)
            reply = client.query(b"hello multiplexer", types.ECHO_REQUEST)
            self.assertEqual((types.ECHO_RESPONSE, b"HELLO MULTIPLEXER"), (reply.type, reply.message))
            self.assertEqual(b"hello multiplexer", backend.wait_for(types.ECHO_REQUEST)[0].message)
            self.assertIn("ECHO_BACKEND", [name for _, name, _ in cluster.mx[0].connected_peers()])

    def test_the_rules_file_is_this_workspace_s(self):
        """mx_integration_test passed echo.rules, the file the constants come from."""
        assert testing.CONFIG is not None
        self.assertTrue(testing.CONFIG.rules.endswith("echo.rules"), testing.CONFIG.rules)

    def test_a_multiplexer_restart_under_a_live_client(self):
        """The backend reconnects on its own and the client's next query
        goes through: what a rolling restart of the brokers looks like to
        a program. The client is a synchronous one, so it reconnects inside
        the call, within the library's reconnect delay."""
        with (
            Cluster(1) as cluster,
            FakePeer(cluster, peers.ECHO_BACKEND) as backend,
            TestClient(cluster, peers.ECHO_CLIENT) as client,
        ):
            backend.reply_with(types.ECHO_REQUEST, b"before", types.ECHO_RESPONSE)
            self.assertEqual(b"before", client.query(b"x", types.ECHO_REQUEST).message)
            cluster.mx[0].restart()
            cluster.wait_for_peer_gone(peers.ECHO_BACKEND, timeout=1)
            cluster.wait_for_peer(peers.ECHO_BACKEND)
            backend.reply_with(types.ECHO_REQUEST, b"after", types.ECHO_RESPONSE)
            self.assertEqual(b"after", client.query(b"x", types.ECHO_REQUEST, timeout=15).message)

    def test_a_raising_handler_is_a_backend_error_to_the_client(self):
        """A backend that raises answers BACKEND_ERROR, which the client
        library raises as BackendError; the fake fails the test at stop(),
        so it is stopped explicitly here to look at the exception."""
        with Cluster(1) as cluster, TestClient(cluster, peers.ECHO_CLIENT) as client:
            backend = FakePeer(cluster, peers.ECHO_BACKEND).start()
            backend.on(types.ECHO_REQUEST, lambda mxmsg: {}[mxmsg.message])
            with self.assertRaises(BackendError):
                client.query(b"missing", types.ECHO_REQUEST)
            with self.assertRaises(KeyError):
                backend.stop()

    def test_a_recording_read_back(self):
        """Cluster(record=True) records what was routed; once the
        multiplexer stopped, the file says who sent what to whom."""
        with Cluster(1, record=True) as cluster:
            with FakePeer(cluster, peers.ECHO_BACKEND) as backend, TestClient(cluster, peers.ECHO_CLIENT) as client:
                backend.reply_with(types.ECHO_REQUEST, b"pong", types.ECHO_RESPONSE)
                client.query(b"ping", types.ECHO_REQUEST)
                backend_id = backend.backend.conn.instance_id
                client_id = client.client.instance_id
            cluster.mx[0].stop()
            routed = [
                record.routed for record in recording.read(cluster.mx[0].record_file) if record.HasField("routed")
            ]
            request = next(r for r in routed if r.type == types.ECHO_REQUEST)
            self.assertEqual(
                (client_id, backend_id, RoutedMessage.DELIVERED),
                (getattr(request, "from"), request.recipient, request.disposition),
            )
            self.assertEqual(b"ping", request.payload)
            reply = next(r for r in routed if r.type == types.ECHO_RESPONSE)
            self.assertEqual(
                (backend_id, client_id, request.id), (getattr(reply, "from"), reply.recipient, reply.references)
            )


if __name__ == "__main__":
    testing.main()
