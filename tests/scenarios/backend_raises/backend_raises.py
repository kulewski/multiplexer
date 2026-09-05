"""a backend whose handler throws answers with BACKEND_ERROR, at once, and keeps serving.

A Python client raises BackendError; a C++ client gets the BACKEND_ERROR
message back from query(). Either way nobody waits for a timeout. A backend
whose on_handler_exception() returns False instead lets the exception out of
serve_forever(), after the error was still reported to the requester.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn


class BackendRaises(unittest.TestCase):
    """Checks that a backend whose handler throws answers with BACKEND_ERROR, at once, and keeps serving."""

    def test_error_is_reported_and_backend_survives(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="raise",
            )
            backend.wait_for("connected", connections=1)

            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                count=2,
                timeout=10,
                query=[(C.types.TEST_REQUEST_A, "boom")],
            )
            self.assertEqual(0, client.wait())

            outcomes = client.events_of("error") + client.events_of("response")
            self.assertEqual(2, len(outcomes), "both queries got an answer of some kind")
            for outcome in outcomes:
                if outcome["event"] == "error":
                    self.assertEqual("BackendError", outcome["kind"])
                else:
                    self.assertEqual(C.types.BACKEND_ERROR, outcome["type"])
                    self.assertIn("handler failed on purpose", outcome["payload"])
                self.assertLess(outcome["ms"], 2000, "reported at once, not by timeout")
            self.assertEqual(2, len(backend.events_of("request")), "the backend served both")
            self.assertEqual(0, backend.stop())

    def test_exception_can_end_the_backend(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="raise",
                exit_on_exception=True,
            )
            backend.wait_for("connected", connections=1)
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                count=1,
                timeout=10,
                query=[(C.types.TEST_REQUEST_A, "boom")],
            )
            self.assertEqual(0, client.wait())
            outcome = (client.events_of("error") + client.events_of("response"))[0]
            self.assertLess(outcome["ms"], 2000, "the requester was still told at once")
            self.assertEqual(4, backend.wait(timeout=15), "the exception left serve_forever and ended the process")
            self.assertEqual(1, len(backend.events_of("handler_exception")))


if __name__ == "__main__":
    harness.main()
