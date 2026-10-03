"""what a draining backend still takes is its choice: nothing new by default, events with `all` kept, everything as the last resort when it is alone.

Five runs. A lone backend drains with the default routing while staying
registered: the client's next requests fail at once with OperationFailed
rather than waiting for a timeout, although the backend is still there.
The same backend draining as the last resort keeps getting every request
until its period is up, since nobody else could take them. An event
backend draining with the default gets no more events while another one
is there, one draining with `all` kept still gets every one, and a lone
one draining as the last resort keeps getting them too. Behind two
multiplexers, a drain is not over until both have confirmed: with one
multiplexer frozen the backend keeps serving, and it leaves once the
multiplexer is back.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn, wait_for_total

LONG_TIMEOUT = 60  # seconds, a query's timeout where a check says it failed at once


def request_types(backend_events: list) -> list:
    """The request events among a role's events."""
    return [event for event in backend_events if event["event"] == "request"]


class DrainRouting(unittest.TestCase):
    """Strict by default, last resort on request, events kept on request, every multiplexer heard."""

    def backend(self, cluster: Cluster, name: str, **options):
        """A TEST_BACKEND_A serving TEST_REQUEST_A, connected."""
        role = spawn(
            "backend",
            harness.CONFIG.lang("backend"),
            mx=cluster.addresses,
            name=name,
            type=C.peers.TEST_BACKEND_A,
            serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
            **options,
        )
        role.wait_for("connected", connections=len(cluster.mx))
        return role

    def ask(self, cluster: Cluster, count: int, timeout: float = 5) -> tuple[list, list]:
        """`count` queries from a fresh client: (responses, errors)."""
        client = spawn(
            "client",
            harness.CONFIG.lang("client"),
            mx=cluster.addresses,
            type=C.peers.TEST_CLIENT,
            count=count,
            timeout=timeout,
            query=[(C.types.TEST_REQUEST_A, "q{round}")],
        )
        self.assertEqual(0, client.wait())
        return client.events_of("response"), client.events_of("error")

    def test_a_lone_backend_draining_fails_new_requests_at_once(self):
        with Cluster(1) as cluster:
            # A rule of its own keeps it from leaving, so the failures below
            # are the multiplexer's doing, not an empty peer list.
            backend = self.backend(cluster, "alone", drain_seconds=60, drain_min_handled=1000)
            instance_id = backend.events_of("connected")[0]["instance_id"]
            self.assertEqual((3, 0), tuple(map(len, self.ask(cluster, 3))))
            backend.request_drain()
            backend.wait_for("acked")
            # A query that waited for its timeout took all of it; one that
            # failed at once a small part of it, however loaded the machine.
            responses, errors = self.ask(cluster, 3, timeout=LONG_TIMEOUT)
            self.assertIn(instance_id, [peer_id for peer_id, _, _ in cluster.mx[0].connected_peers()], "still there")
            self.assertEqual([], responses, "nobody takes the request by the rules, and there is no last resort")
            self.assertEqual(["OperationFailed"] * 3, [error["kind"] for error in errors])
            self.assertLess(
                max(error["ms"] for error in errors), LONG_TIMEOUT * 1000 / 2, "at once, not after a timeout"
            )
            self.assertEqual(3, len(backend.events_of("request")), "it served only what came before")
            backend.kill()  # its rule never lets it leave

    def test_the_drain_waits_for_every_multiplexer(self):
        with Cluster(2) as cluster:
            backend = self.backend(cluster, "behind-two", drain_seconds=30)
            backend.wait_for("connected", connections=2)
            cluster.mx[1].pause()  # frozen: it will not confirm until resumed
            backend.request_drain()
            backend.wait_for("draining")
            with self.assertRaises(TimeoutError):
                backend.wait_for("acked", timeout=2)
            self.assertIsNone(backend.returncode, "one multiplexer has not confirmed: the drain goes on")
            cluster.mx[1].resume()
            backend.wait_for("acked", timeout=15)
            self.assertEqual(0, backend.wait(timeout=30), "confirmed by both, it leaves")

    def test_a_lone_backend_draining_as_the_last_resort_keeps_serving(self):
        with Cluster(1) as cluster:
            # Not before it served the five: a client that a loaded machine
            # starts late would otherwise find the period over.
            backend = self.backend(
                cluster, "last-resort", drain_seconds=3, drain_routing="last_resort", drain_min_handled=5
            )
            backend.request_drain()
            backend.wait_for("acked")
            responses, errors = self.ask(cluster, 5)
            self.assertEqual((5, 0), (len(responses), len(errors)), "served, since nobody else could take them")
            self.assertEqual(0, backend.wait(timeout=30), "and it leaves when its period is up")
            self.assertEqual(5, backend.events_of("stopped")[0]["handled"])

    def test_an_event_backend_draining_gets_no_more_events_unless_all_is_kept(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            # The two that get every event leave after the last, and are
            # waited for, not stopped: a SIGTERM can find a Python role
            # that is leaving on its own past its handler, and kill it. The
            # strict one, whose share ends when its drain takes effect, is
            # stopped after the checks.
            strict = spawn(
                "event_backend",
                cfg.lang("event_backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_BACKEND,
                name="strict",
                drain_file=True,
            )
            keeping = spawn(
                "event_backend",
                cfg.lang("event_backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_BACKEND,
                name="keeping",
                drain_file=True,
                drain_routing="all",
                until=200,
            )
            staying = spawn(
                "event_backend",
                cfg.lang("event_backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_BACKEND,
                name="staying",
                until=200,
            )
            for backend in (strict, keeping, staying):
                backend.wait_for("connected", connections=1)
            sender = spawn(
                "event_client",
                cfg.lang("event_client"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_CLIENT,
                send=[(C.types.TEST_EVENT, "e%d" % index) for index in range(200)],
                interval=0.02,
            )
            wait_for_total([strict], "received", 5)
            strict.request_drain()
            keeping.request_drain()
            acked_strict = strict.wait_for("acked")
            acked_keeping = keeping.wait_for("acked")
            self.assertEqual(0, sender.wait(timeout=60))
            self.assertEqual(200, len(staying.wait_for_count("received", 200, timeout=15)), "the staying one got all")
            self.assertEqual(
                200, len(keeping.wait_for_count("received", 200, timeout=15)), "so did the one keeping all"
            )
            after = strict.events[strict.events.index(acked_strict) + 1 :]
            self.assertEqual([], [event for event in after if event["event"] == "received"], "none after confirmation")
            self.assertLess(len(strict.events_of("received")), 200)
            self.assertIn(acked_keeping, keeping.events)
            self.assertEqual(0, strict.stop())
            for backend in (keeping, staying):
                self.assertEqual(0, backend.wait(), "it leaves on its own after the last event")

    def test_a_lone_event_backend_draining_as_the_last_resort_keeps_receiving(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            lone = spawn(
                "event_backend",
                cfg.lang("event_backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_BACKEND,
                name="lone",
                drain_file=True,
                drain_routing="last_resort",
                until=50,  # every one: it leaves after the last
            )
            lone.wait_for("connected", connections=1)
            lone.request_drain()
            lone.wait_for("acked")
            sender = spawn(
                "event_client",
                cfg.lang("event_client"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_CLIENT,
                send=[(C.types.TEST_EVENT, "e%d" % index) for index in range(50)],
            )
            self.assertEqual(0, sender.wait(timeout=30))
            self.assertEqual(50, len(lone.wait_for_count("received", 50, timeout=15)), "nobody else could take them")
            self.assertEqual(0, lone.wait(), "it leaves on its own after the last event")


if __name__ == "__main__":
    harness.main()
