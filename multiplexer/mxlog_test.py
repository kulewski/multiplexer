"""Unit tests for multiplexer.mxlog: entries with data must not fail."""

import io
import sys
import unittest
from contextlib import redirect_stderr

from multiplexer import mxlog as mxlogging


class LoggingTest(unittest.TestCase):
    """Entries with data, which used to fail on a str/bytes mismatch."""

    def test_pickled_data_is_accepted(self):
        # _do_log is the unwrapped path: a problem raises here instead of being
        # swallowed by never_throw.
        mxlogging._do_log(mxlogging.ERROR, mxlogging.LOWVERBOSITY, text="x", data=mxlogging.PickleData({"a": 1}))
        mxlogging._do_log(mxlogging.ERROR, mxlogging.LOWVERBOSITY, text="x", data=b"raw", data_type=1)
        mxlogging._do_log(mxlogging.ERROR, mxlogging.LOWVERBOSITY, text="x", data="text", data_type=1)

    def test_log_exception_logs_instead_of_complaining(self):
        captured = io.StringIO()
        with redirect_stderr(captured):
            try:
                raise ValueError("boom")
            except ValueError:
                mxlogging.log_exception(text="caught on purpose")
        self.assertNotIn("Ignored.", captured.getvalue(), "never_throw swallowed a failure inside the logger")


if __name__ == "__main__":
    unittest.main()
