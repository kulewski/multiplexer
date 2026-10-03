"""The sender field is `sender`, named `from`, a Python keyword, up to
2.3.1, under the same number, so that the wire and every recording are
unchanged: it is set, read, cleared and given to a constructor like any
field, where `from_` could only be read on protobuf's default runtime. For
a release, the names it had still read it, with a DeprecationWarning:
`from_` and getattr(message, "from") on MultiplexerMessage, and the
latter on a recording's RoutedMessage; setting through them is refused.
And a sender given to new_message() under a former name is taken, with
the same warning, where `from_=` raised or was overwritten.
"""

import unittest
import warnings

from multiplexer.clients import Client
from multiplexer.events_pb2 import Event
from multiplexer.multiplexer_constants import peers, types
from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.Recording_pb2 import RoutedMessage
from multiplexer.threaded_client import ThreadedClient

OTHER = 12345  # a sender that is not the client's own


class SenderFieldTest(unittest.TestCase):
    """See the module docstring."""

    def test_the_field_keeps_its_number(self) -> None:
        """The renamed fields have the numbers `from` and `from_` had."""
        self.assertEqual(2, MultiplexerMessage.DESCRIPTOR.fields_by_name["sender"].number)
        self.assertEqual(2, RoutedMessage.DESCRIPTOR.fields_by_name["sender"].number)
        self.assertEqual(8, Event.DESCRIPTOR.fields_by_name["sender"].number)

    def test_it_is_a_field_like_any_other(self) -> None:
        """Given to the constructor, set, read and cleared."""
        message = MultiplexerMessage(sender=OTHER)
        self.assertEqual(OTHER, message.sender)
        message.sender = 7
        self.assertTrue(message.HasField("sender"))
        message.ClearField("sender")
        self.assertEqual(0, message.sender)

    def test_the_former_names_read_it_with_a_warning(self) -> None:
        """`from_` and `from` read `sender`, each warning; neither sets."""
        message = MultiplexerMessage(sender=OTHER)
        routed = RoutedMessage(sender=OTHER)
        for read in (lambda: message.from_, lambda: getattr(message, "from"), lambda: getattr(routed, "from")):
            with self.assertWarns(DeprecationWarning):
                self.assertEqual(OTHER, read())
        with self.assertRaises(AttributeError):
            message.from_ = 7  # type: ignore[misc]  # read-only
        with self.assertRaises(AttributeError):
            setattr(message, "from", 7)
        self.assertEqual(OTHER, message.sender)

    def test_new_message_takes_a_sender_under_any_of_its_names(self) -> None:
        """sender= without a word, `from` and `from_` with a warning, the
        client's own id without one; in both clients."""
        sync = Client([], type=peers.WEBSITE)
        threaded = ThreadedClient([], type=peers.TEST_ACTIVE_CLIENT)
        try:
            for client in (sync, threaded):
                with warnings.catch_warnings():
                    warnings.simplefilter("error")  # none for sender= or no sender
                    self.assertEqual(OTHER, client.new_message(type=types.TEST_UNROUTED, sender=OTHER).sender)
                    self.assertEqual(client.instance_id, client.new_message(type=types.TEST_UNROUTED).sender)
                for former in ({"from": OTHER}, {"from_": OTHER}):
                    with self.assertWarns(DeprecationWarning):
                        self.assertEqual(OTHER, client.new_message(type=types.TEST_UNROUTED, **former).sender)
                with self.assertWarns(DeprecationWarning):
                    both = client.new_message(type=types.TEST_UNROUTED, sender=OTHER, **{"from": 1})
                self.assertEqual(OTHER, both.sender, "sender= wins over a former name")
        finally:
            sync.shutdown()
            threaded.shutdown()


if __name__ == "__main__":
    unittest.main()
