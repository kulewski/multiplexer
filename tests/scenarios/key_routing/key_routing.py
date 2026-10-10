"""keys reach the backends whose filter takes them, through every client and server class.

Three runs, the roles of one language each. Pools: a backend whose filter
for TEST_REQUEST_A takes the tenant acme's keys and one whose filter takes
globex's, both with the filters set before they connect, on
BaseMultiplexerServer and then on BaseThreadedMultiplexerServer; a client
in every mode has its queries keyed for acme all answered by the acme
backend, those keyed for globex by the globex one, and those without a
key by both. The search: the only backend, a peer that speaks the
protocol by hand, takes a keyed query's request and reports it
undelivered, as a multiplexer would, so that the query searches at once;
the search carries the request's key, from SyncClient and ThreadedClient
both, and once the backend answers it and the request sent again, the
query has its reply. Events: event backends whose filters take one group
each, and one with no filter, get an event_client's keyed event when
their filter takes its key or they have none, and every one of them its
event without a key, which the second event_client sends once the keyed
one was routed, so that a keyed event routed to a backend whose filter
does not take it would have come before it.
"""

import unittest

from multiplexer.Multiplexer_pb2 import DeliveryError
from multiplexer.testing.raw_peer import RawPeer
from tests import harness
from tests.harness import Cluster, constants as C, spawn

REQUEST = C.types.TEST_REQUEST_A
ACME = r"tenant\.acme\..*"
GLOBEX = r"tenant\.globex\..*"
# Every mode the client roles have, with the peer type each runs as: a
# passive one for SyncClient, an active one for ThreadedClient.
CLIENT_MODES = {
    "sync": {"type": C.peers.TEST_CLIENT},
    "threaded": {"type": C.peers.TEST_ACTIVE_CLIENT, "threaded": True},
    "async": {"type": C.peers.TEST_ACTIVE_CLIENT, "threaded": True, "async": 4},
    "workers": {"type": C.peers.TEST_ACTIVE_CLIENT, "workers": 2},
}


class KeyRouting(unittest.TestCase):
    """Pools by key, the search's key, events by key."""

    def backend(self, cluster: Cluster, name: str, **options) -> int:
        """A TEST_BACKEND_A serving TEST_REQUEST_A, connected: its instance id."""
        role = spawn(
            "backend",
            harness.CONFIG.lang("backend"),
            mx=cluster.addresses,
            name=name,
            type=C.peers.TEST_BACKEND_A,
            serves={REQUEST: C.types.TEST_RESPONSE},
            **options,
        )
        return role.wait_for("connected", connections=len(cluster.mx))["instance_id"]

    def answered_by(self, cluster: Cluster, mode: str, key: str | None) -> set[int]:
        """Who answered a fresh client's queries in `mode`, keyed `key`,
        none for None; every query answered."""
        options = dict(CLIENT_MODES[mode])
        if key is not None:
            options["key"] = key
        client = spawn(
            "client",
            harness.CONFIG.lang("client"),
            mx=cluster.addresses,
            count=4,
            query=[(REQUEST, "q{round}")],
            **options,
        )
        self.assertEqual(0, client.wait())
        self.assertEqual([], client.events_of("error"), (mode, key))
        return {event["sender"] for event in client.events_of("response")}

    def test_pools_by_key(self):
        for threaded in (False, True):
            with self.subTest(threaded=threaded), Cluster(2) as cluster:
                acme = self.backend(cluster, "acme", key_filter=[(REQUEST, ACME)], threaded=threaded)
                globex = self.backend(cluster, "globex", key_filter=[(REQUEST, GLOBEX)], threaded=threaded)
                for mode in CLIENT_MODES:
                    self.assertEqual({acme}, self.answered_by(cluster, mode, "tenant.acme.orders"), mode)
                    self.assertEqual({globex}, self.answered_by(cluster, mode, "tenant.globex.orders"), mode)
                    self.assertEqual({acme, globex}, self.answered_by(cluster, mode, None), mode)

    def test_the_search_carries_the_key(self):
        with Cluster(1) as cluster:
            backend = RawPeer(cluster.endpoints[0], C.peers.TEST_BACKEND_A)
            self.addCleanup(backend.close)
            backend.handshake({REQUEST: [ACME]})
            for mode in ("sync", "threaded"):
                with self.subTest(mode=mode):
                    client = spawn(
                        "client",
                        harness.CONFIG.lang("client"),
                        mx=cluster.addresses,
                        key="tenant.acme.orders",
                        query=[(REQUEST, "q")],
                        **CLIENT_MODES[mode],
                    )
                    request = backend.receive_type(REQUEST)
                    self.assertEqual("tenant.acme.orders", request.key)
                    report = DeliveryError(packet_id=request.id, failed_type=[C.peers.TEST_BACKEND_A])
                    backend.send(
                        report.SerializeToString(), C.types.DELIVERY_ERROR, to=request.sender, references=request.id
                    )
                    search = backend.receive_type(C.types.BACKEND_FOR_PACKET_SEARCH)
                    self.assertEqual("tenant.acme.orders", search.key)
                    backend.send(search.message, C.types.PING, to=search.sender, references=search.id)
                    again = backend.receive_type(REQUEST)
                    self.assertEqual((backend.instance_id, "tenant.acme.orders"), (again.to, again.key))
                    backend.send(b"Q", C.types.TEST_RESPONSE, to=again.sender, references=again.id)
                    self.assertEqual(0, client.wait())
                    self.assertEqual([backend.instance_id], [event["sender"] for event in client.events_of("response")])

    def event_backend(self, cluster: Cluster, name: str, **options):
        """A TEST_EVENT_BACKEND, connected."""
        role = spawn(
            "event_backend",
            harness.CONFIG.lang("event_backend"),
            mx=cluster.addresses,
            name=name,
            type=C.peers.TEST_EVENT_BACKEND,
            **options,
        )
        role.wait_for("connected", connections=len(cluster.mx))
        return role

    def send_event(self, cluster: Cluster, payload: str, **options) -> None:
        """One TEST_EVENT from a fresh event_client, written."""
        sender = spawn(
            "event_client",
            harness.CONFIG.lang("event_client"),
            mx=cluster.addresses,
            type=C.peers.TEST_EVENT_CLIENT,
            send=[(C.types.TEST_EVENT, payload)],
            **options,
        )
        self.assertEqual(0, sender.wait())

    def test_events_by_key(self):
        with Cluster(1) as cluster:
            first = self.event_backend(cluster, "first", key_filter=[(C.types.TEST_EVENT, r"group\.1")])
            second = self.event_backend(cluster, "second", key_filter=[(C.types.TEST_EVENT, r"group\.2")])
            everyone = self.event_backend(cluster, "everyone")
            self.send_event(cluster, "keyed", key="group.1")
            first.wait_for("received", payload="keyed")
            everyone.wait_for("received", payload="keyed")
            self.send_event(cluster, "unkeyed")
            for role in (first, second, everyone):
                role.wait_for("received", payload="unkeyed")
            self.assertEqual(
                [("keyed", "group.1"), ("unkeyed", "")],
                [(event["payload"], event.get("key", "")) for event in first.events_of("received")],
            )
            self.assertEqual(["unkeyed"], [event["payload"] for event in second.events_of("received")])
            self.assertEqual(["keyed", "unkeyed"], [event["payload"] for event in everyone.events_of("received")])


if __name__ == "__main__":
    harness.main()
