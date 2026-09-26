"""The synchronous client's two names: SyncClient, the name the docs use
since 2.4.0, and Client, its name before, are one class under every import
path a release had. A subclass and an annotation through the old name mean
the new class, to Python and to pyright, which check.sh runs over this file."""

import typing
import unittest

import multiplexer.testing.fakes
from multiplexer.clients import BasicClient, Client, MxClient, SyncClient


def adopt(client: Client) -> SyncClient:
    """Returns its argument: pyright accepts that only if a Client is a SyncClient."""
    return client


class Mine(Client):
    """A subclass through the old name, as code written before 2.4.0 has them."""


class ClientNamesTest(unittest.TestCase):
    def test_the_old_name_is_the_new_class(self):
        """One class object, so isinstance, subclassing and the class constants work under either name."""
        self.assertIs(Client, SyncClient)
        self.assertEqual("SyncClient", SyncClient.__name__)
        self.assertTrue(issubclass(SyncClient, BasicClient))
        self.assertTrue(issubclass(Mine, SyncClient))
        self.assertIs(Client.ONE, SyncClient.ONE)

    def test_the_old_import_path_through_the_harness_still_works(self):
        """multiplexer.testing.fakes imported Client in every release, so it could be imported from there."""
        self.assertIs(SyncClient, multiplexer.testing.fakes.Client)

    def test_the_holder_hands_out_the_new_class(self):
        """MxClient.get() hands out the synchronous class."""
        self.assertIs(SyncClient, typing.get_type_hints(MxClient.get)["return"])


if __name__ == "__main__":
    unittest.main()
