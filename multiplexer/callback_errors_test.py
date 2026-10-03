"""What a Python callback the library runs raises goes to
sys.unraisablehook with the callback, as CPython does with what it cannot
raise anywhere: SystemExit too, which, printed with PyErr_Print(), exited
the whole interpreter from the io thread while the program ran on; and
nothing keeps the exception after, where PyErr_Print() left it in
sys.last_value, the callback's frames, its data and the client with it,
until the next error printed. Counted, not timed: a client with nothing to
connect to drops a message sent with no time to wait at once, telling its
on_drop on its io thread, and the hook counts what it heard.
"""

import sys
import threading
import unittest

from multiplexer.multiplexer_constants import peers, types
from multiplexer.threaded_client import ThreadedClient

BOUND = 30  # seconds the hook may take to hear of it: a failure detector only


class CallbackErrorsTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self) -> None:
        self.heard: list[tuple[type, object]] = []
        self.reported = threading.Event()
        previous = sys.unraisablehook

        def hook(unraisable) -> None:
            """What the binding reported, kept."""
            self.heard.append((unraisable.exc_type, unraisable.object))
            self.reported.set()

        sys.unraisablehook = hook
        self.addCleanup(setattr, sys, "unraisablehook", previous)

    def drop_one(self, on_drop) -> None:
        """One message dropped, `on_drop` told, and the hook heard of what it raised."""
        client = ThreadedClient([], type=peers.WEBSITE, on_drop=on_drop)
        try:
            client.send_message(b"x", type=types.TEST_UNROUTED, timeout=0)
            self.assertTrue(self.reported.wait(BOUND), "the hook heard of it")
        finally:
            client.shutdown()

    def test_system_exit_in_a_callback_ends_nothing(self) -> None:
        """sys.exit() in on_drop: reported, and this process goes on."""

        def leave(message_id: int, reason: object) -> None:
            sys.exit(3)

        self.drop_one(leave)
        self.assertEqual([(SystemExit, leave)], self.heard)

    def test_nothing_keeps_what_a_callback_raised(self) -> None:
        """An exception out of on_drop is not left in sys.last_value."""
        before = getattr(sys, "last_value", None)

        def fail(message_id: int, reason: object) -> None:
            raise ValueError("the observer fails")

        self.drop_one(fail)
        self.assertEqual([(ValueError, fail)], self.heard)
        self.assertIs(before, getattr(sys, "last_value", None))


if __name__ == "__main__":
    unittest.main()
