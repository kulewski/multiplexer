"""Key filters on the wire, against a real multiplexer and peers that speak
the protocol by hand (RawPeer). A filter is a filter on what its peer
receives: whom: ANY and whom: ALL pass over a peer whose filter does not
take a message's key and treat the rest as always, round robin or every
one, a peer with no filter for the type taking every key and one with an
empty filter none; thousands of exact keys and hundreds of patterns of one
type route as one; a message without a key ignores every filter; the
search for a backend passes over the same peers as the request;
PEER_CONTROL replaces the filters and its PEER_STATUS comes after
everything routed under the old ones; a filter that does not compile
closes a welcome's connection and leaves PEER_CONTROL's filters as they
were; a key over MAX_KEY_SIZE is dropped and reported. Counted, not timed:
after what is being counted, the sender addresses a marker to each
receiver, and a receiver has everything routed to it before once its
marker arrives, since frames from one connection are routed in their order
and a connection's queue keeps it.
"""

import re
import unittest

from multiplexer._native import MAX_KEY_SIZE
from multiplexer.Multiplexer_pb2 import (
    PEER_CONTROL,
    PEER_STATUS,
    BackendForPacketSearch,
    DeliveryError,
    KeyFilter,
    MultiplexerMessage,
    PeerControl,
    PeerStatus,
    Routing,
)
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, runfile
from multiplexer.testing.raw_peer import RawPeer

RULES = runfile("tests/testing.rules")
REQUEST = types.TEST_REQUEST_A  # whom: ANY to TEST_BACKEND_A
EVENT = types.TEST_EVENT  # whom: ALL to TEST_EVENT_BACKEND
ACME = r"tenant\.acme\..*"
GLOBEX = r"tenant\.globex\..*"


def received(peer: RawPeer, sender: RawPeer, type_: int) -> list[MultiplexerMessage]:
    """What `sender` routed to `peer` so far, of `type_`: everything that
    arrives before a marker the sender addresses to the peer after it."""
    marker = sender.send(b"marker", type_, to=peer.instance_id)
    got = []
    while True:
        mxmsg = peer.receive_type(type_)
        if mxmsg.id == marker:
            return got
        got.append(mxmsg)


def keys_of(messages: list[MultiplexerMessage]) -> list[str]:
    """The keys of `messages`, "" for none, in their order."""
    return [mxmsg.key for mxmsg in messages]


def delivery_error(sender: RawPeer, message_id: int) -> DeliveryError:
    """The DELIVERY_ERROR the multiplexer sent `sender` about `message_id`."""
    while True:
        mxmsg = sender.receive_type(types.DELIVERY_ERROR)
        if mxmsg.references == message_id:
            report = DeliveryError()
            report.ParseFromString(mxmsg.message)
            return report


def peer_control(
    peer: RawPeer, filters: dict[int, list[str]], routing: Routing | None = None
) -> tuple[PeerStatus, list[MultiplexerMessage]]:
    """PEER_CONTROL from `peer` with these filters, its PEER_STATUS, and the
    messages that came before the status, heartbeats apart."""
    control = PeerControl(routing=routing or Routing())
    for type_, patterns in filters.items():
        control.key_filter.add(type=type_, pattern=patterns)
    asked = peer.send(control.SerializeToString(), PEER_CONTROL)
    before = []
    while True:
        mxmsg = peer.receive()
        if mxmsg.type == PEER_STATUS and mxmsg.references == asked:
            status = PeerStatus()
            status.ParseFromString(mxmsg.message)
            return status, before
        if mxmsg.type != types.HEARTBIT:
            before.append(mxmsg)


class KeyRoutingTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self) -> None:
        self.cluster = Cluster(1, rules=RULES)
        self.cluster.__enter__()
        self.addCleanup(self.cluster.__exit__, None, None, None)
        self.endpoint = self.cluster.endpoints[0]

    def peer(self, peer_type: int, filters: dict[int, list[str]] | None = None) -> RawPeer:
        """A peer of `peer_type`, its welcome carrying `filters`, handshake done."""
        peer = RawPeer(self.endpoint, peer_type)
        self.addCleanup(peer.close)
        peer.handshake(filters)
        return peer

    def test_any_round_robins_over_the_peers_that_take_the_key(self) -> None:
        acme = self.peer(peers.TEST_BACKEND_A, {REQUEST: [ACME]})
        anyone = self.peer(peers.TEST_BACKEND_A)
        globex = self.peer(peers.TEST_BACKEND_A, {REQUEST: [GLOBEX]})
        sender = self.peer(peers.TEST_CLIENT)
        for _ in range(4):
            sender.send(b"", REQUEST, key="tenant.acme.orders")  # acme's filter and no filter take it, in turn
        for _ in range(4):
            sender.send(b"", REQUEST, key="tenant.initech.orders")  # only no filter takes it
        for _ in range(3):
            sender.send(b"", REQUEST)  # no key: all three, in turn
        self.assertEqual(keys_of(received(acme, sender, REQUEST)), ["tenant.acme.orders"] * 2 + [""])
        self.assertEqual(
            keys_of(received(anyone, sender, REQUEST)),
            ["tenant.acme.orders"] * 2 + ["tenant.initech.orders"] * 4 + [""],
        )
        self.assertEqual(keys_of(received(globex, sender, REQUEST)), [""])

    def test_any_takes_turns_among_the_peers_that_take_each_message(self) -> None:
        """whom: ANY gives each message to the peer, of those that take it,
        that has waited longest for one of its type, a peer that has just
        registered first in line: turn by turn over every peer for messages
        without a key, and over a key's takers for keyed ones, wherever the
        peers that do not take it stand. Pinned message by message: what
        each peer gets, in order, is what that rule gives."""
        acme, globex = "tenant.acme.orders", "tenant.globex.orders"
        filters = {"a": [ACME], "b": None, "c": [GLOBEX], "d": [ACME]}
        backends = {name: self.peer(peers.TEST_BACKEND_A, {REQUEST: f} if f else None) for name, f in filters.items()}
        sender = self.peer(peers.TEST_CLIENT)
        keys = [""] * 4 + [acme] * 4 + [globex] * 2 + [acme] * 3 + [""] + [globex] + [""] * 3

        def takes(name: str, key: str) -> bool:
            patterns = filters[name]
            return not key or patterns is None or any(re.fullmatch(pattern, key) for pattern in patterns)

        waiting = list(reversed(filters))  # the longest waiting first: a peer that registers comes first in line
        expected: dict[str, list[bytes]] = {name: [] for name in filters}
        for index, key in enumerate(keys):
            payload = b"m%d" % index
            if key:
                sender.send(payload, REQUEST, key=key)
            else:
                sender.send(payload, REQUEST)
            chosen = next(name for name in waiting if takes(name, key))
            waiting.remove(chosen)
            waiting.append(chosen)
            expected[chosen].append(payload)
        got = {name: [mxmsg.message for mxmsg in received(peer, sender, REQUEST)] for name, peer in backends.items()}
        self.assertEqual(expected, got)

    def test_a_key_nobody_takes_is_reported_and_a_peer_without_a_filter_takes_every_key(self) -> None:
        acme = self.peer(peers.TEST_BACKEND_A, {REQUEST: [ACME]})
        sender = self.peer(peers.TEST_CLIENT)
        nobody = sender.send(b"", REQUEST, key="tenant.initech.orders")
        report = delivery_error(sender, nobody)
        self.assertEqual(list(report.failed_type), [peers.TEST_BACKEND_A])
        # A peer with no filter takes every key: acme's as well, which the
        # acme backend, drained here with its filter kept, takes no more.
        anyone = self.peer(peers.TEST_BACKEND_A)
        peer_control(acme, {REQUEST: [ACME]}, Routing(any=False))
        sender.send(b"", REQUEST, key="tenant.initech.orders")
        sender.send(b"", REQUEST, key="tenant.acme.orders")
        self.assertEqual(keys_of(received(anyone, sender, REQUEST)), ["tenant.initech.orders", "tenant.acme.orders"])
        self.assertEqual(keys_of(received(acme, sender, REQUEST)), [])

    def test_all_reaches_the_matching_filters_and_the_unfiltered(self) -> None:
        first = self.peer(peers.TEST_EVENT_BACKEND, {EVENT: [r"group\.1"]})
        second = self.peer(peers.TEST_EVENT_BACKEND, {EVENT: [r"group\.2"]})
        everyone = self.peer(peers.TEST_EVENT_BACKEND)
        sender = self.peer(peers.TEST_EVENT_CLIENT)
        sender.send(b"", EVENT, key="group.1")
        sender.send(b"", EVENT, key="group.3")
        sender.send(b"", EVENT)
        self.assertEqual(keys_of(received(first, sender, EVENT)), ["group.1", ""])
        self.assertEqual(keys_of(received(second, sender, EVENT)), [""])
        self.assertEqual(keys_of(received(everyone, sender, EVENT)), ["group.1", "group.3", ""])

    def test_a_filter_belongs_to_its_message_type(self) -> None:
        backend = self.peer(peers.TEST_EVENT_BACKEND, {types.TEST_EVENT_ANY: [r"group\.1"]})
        sender = self.peer(peers.TEST_EVENT_CLIENT)
        sender.send(b"", EVENT, key="group.2")  # another type: no filter, every key
        self.assertEqual(keys_of(received(backend, sender, EVENT)), ["group.2"])

    def test_an_empty_filter_takes_no_key_and_every_message_without_one(self) -> None:
        nothing = self.peer(peers.TEST_EVENT_BACKEND, {EVENT: []})
        everyone = self.peer(peers.TEST_EVENT_BACKEND)
        sender = self.peer(peers.TEST_EVENT_CLIENT)
        sender.send(b"", EVENT, key="group.1")
        sender.send(b"", EVENT)
        self.assertEqual(keys_of(received(nothing, sender, EVENT)), [""])
        self.assertEqual(keys_of(received(everyone, sender, EVENT)), ["group.1", ""])

    def test_the_search_follows_the_key(self) -> None:
        acme = self.peer(peers.TEST_BACKEND_A, {REQUEST: [ACME]})
        anyone = self.peer(peers.TEST_BACKEND_A)
        globex = self.peer(peers.TEST_BACKEND_A, {REQUEST: [GLOBEX]})
        sender = self.peer(peers.TEST_CLIENT)
        search = BackendForPacketSearch(packet_type=REQUEST).SerializeToString()
        sender.send(search, types.BACKEND_FOR_PACKET_SEARCH, key="tenant.acme.orders")
        sender.send(search, types.BACKEND_FOR_PACKET_SEARCH, key="tenant.initech.orders")
        sender.send(search, types.BACKEND_FOR_PACKET_SEARCH)
        found = {
            "acme": keys_of(received(acme, sender, types.BACKEND_FOR_PACKET_SEARCH)),
            "anyone": keys_of(received(anyone, sender, types.BACKEND_FOR_PACKET_SEARCH)),
            "globex": keys_of(received(globex, sender, types.BACKEND_FOR_PACKET_SEARCH)),
        }
        self.assertEqual(
            found,
            {
                "acme": ["tenant.acme.orders", ""],
                "anyone": ["tenant.acme.orders", "tenant.initech.orders", ""],
                "globex": [""],
            },
        )

    def test_peer_control_replaces_the_filters_after_what_came_before(self) -> None:
        backend = self.peer(peers.TEST_BACKEND_A, {REQUEST: [ACME]})
        other = self.peer(peers.TEST_BACKEND_A, {REQUEST: [GLOBEX]})
        sender = self.peer(peers.TEST_CLIENT)
        sender.send(b"", REQUEST, key="tenant.acme.orders")  # the backend's alone
        received(other, sender, REQUEST)  # the multiplexer has routed the request
        status, before = peer_control(backend, {REQUEST: [GLOBEX]})
        self.assertFalse(status.HasField("error"))
        self.assertEqual(list(status.key_filter), [KeyFilter(type=REQUEST, pattern=[GLOBEX])])
        self.assertEqual(keys_of(before), ["tenant.acme.orders"])  # routed under the old filter, before the answer
        nobody = sender.send(b"", REQUEST, key="tenant.acme.orders")
        delivery_error(sender, nobody)  # no filter takes acme any more
        sender.send(b"", REQUEST, key="tenant.globex.orders")
        sender.send(b"", REQUEST, key="tenant.globex.orders")  # both filters take it, in turn
        self.assertEqual(keys_of(received(backend, sender, REQUEST)), ["tenant.globex.orders"])
        self.assertEqual(keys_of(received(other, sender, REQUEST)), ["tenant.globex.orders"])

    def test_peer_control_keeps_the_filters_when_it_drains(self) -> None:
        backend = self.peer(peers.TEST_EVENT_BACKEND, {EVENT: [r"group\.1"]})
        sender = self.peer(peers.TEST_EVENT_CLIENT)
        status, _ = peer_control(backend, {EVENT: [r"group\.1"]}, Routing(any=False))
        self.assertEqual(list(status.key_filter), [KeyFilter(type=EVENT, pattern=[r"group\.1"])])
        sender.send(b"", EVENT, key="group.1")
        sender.send(b"", EVENT, key="group.2")
        self.assertEqual(keys_of(received(backend, sender, EVENT)), ["group.1"])

    def test_a_refused_peer_control_changes_nothing(self) -> None:
        backend = self.peer(peers.TEST_BACKEND_A, {REQUEST: [ACME]})
        sender = self.peer(peers.TEST_CLIENT)
        status, _ = peer_control(backend, {REQUEST: [r"tenant\.(acme"]}, Routing(any=False))
        self.assertIn("does not compile", status.error)
        self.assertEqual(list(status.key_filter), [KeyFilter(type=REQUEST, pattern=[ACME])])
        self.assertTrue(status.routing.any)
        status, _ = peer_control(backend, {types.PING: ["a"]})
        self.assertIn("a protocol type", status.error)
        # The filter in effect is the old one: it takes acme, and still
        # leaves globex to nobody, rather than the backend taking every key.
        sender.send(b"", REQUEST, key="tenant.acme.orders")
        globex = sender.send(b"", REQUEST, key="tenant.globex.orders")
        delivery_error(sender, globex)
        self.assertEqual(keys_of(received(backend, sender, REQUEST)), ["tenant.acme.orders"])

    def test_a_welcome_with_a_filter_that_does_not_compile_is_refused(self) -> None:
        refused = RawPeer(self.endpoint, peers.TEST_BACKEND_A)
        self.addCleanup(refused.close)
        welcome_filter = {REQUEST: [r"tenant\.(acme"]}
        with self.assertRaises(ConnectionError):
            refused.handshake(welcome_filter)
        accepted = self.peer(peers.TEST_BACKEND_A, {REQUEST: [ACME]})
        sender = self.peer(peers.TEST_CLIENT)
        sender.send(b"", REQUEST, key="tenant.acme.orders")
        self.assertEqual(keys_of(received(accepted, sender, REQUEST)), ["tenant.acme.orders"])

    def test_a_key_over_the_limit_is_dropped_and_reported(self) -> None:
        anyone = self.peer(peers.TEST_BACKEND_A)
        sender = self.peer(peers.TEST_CLIENT)
        too_long = sender.send(b"", REQUEST, key="k" * (MAX_KEY_SIZE + 1))
        delivery_error(sender, too_long)
        sender.send(b"", REQUEST, key="k" * MAX_KEY_SIZE)
        self.assertEqual(keys_of(received(anyone, sender, REQUEST)), ["k" * MAX_KEY_SIZE])

    def test_a_message_with_to_ignores_the_filters(self) -> None:
        backend = self.peer(peers.TEST_BACKEND_A, {REQUEST: [ACME]})
        sender = self.peer(peers.TEST_CLIENT)
        sender.send(b"", REQUEST, key="tenant.globex.orders", to=backend.instance_id)
        self.assertEqual(keys_of(received(backend, sender, REQUEST)), ["tenant.globex.orders"])

    def test_thousands_of_keys_and_hundreds_of_patterns(self) -> None:
        # Exact keys, looked up, and patterns, matched by one set, far past
        # what a 64-bit mask holds; each event reaches the one peer whose
        # filter takes its key, and the peer without a filter.
        filtered = []
        for index in range(8):
            exact = [r"model\.%d\-%d" % (index, key) for key in range(500)]
            prefixes = [r"family\.%d\..*" % (index * 30 + prefix) for prefix in range(30)]
            filtered.append(self.peer(peers.TEST_EVENT_BACKEND, {EVENT: exact + prefixes}))
        everyone = self.peer(peers.TEST_EVENT_BACKEND)
        sender = self.peer(peers.TEST_EVENT_CLIENT)
        sent = []
        for index in range(8):
            sent.append("model.%d-%d" % (index, index * 61))
            sent.append("family.%d.x" % (index * 30 + index))
        for key in sent + ["model.9-1", "family.999.x"]:  # the last two nobody filters for
            sender.send(b"", EVENT, key=key)
        for index, peer in enumerate(filtered):
            self.assertEqual(keys_of(received(peer, sender, EVENT)), sent[2 * index : 2 * index + 2])
        self.assertEqual(keys_of(received(everyone, sender, EVENT)), sent + ["model.9-1", "family.999.x"])


if __name__ == "__main__":
    unittest.main()
