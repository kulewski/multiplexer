"""Unit tests for multiplexer.mxlog: entries with data must not fail, and
the log streamer's child never returns into its caller's code."""

import io
import os
import subprocess
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


class VerbosityEnvironmentTest(unittest.TestCase):
    """MX_LOG_VERBOSITY is read when the extension loads, so a fresh
    interpreter per case."""

    def should_log_in_a_fresh_interpreter(self, environment: dict[str, str]) -> str:
        """What a new process says should_log(DEBUG, v) is for each verbosity, as a string of Y and N."""
        script = (
            "from multiplexer import mxlog as m;"
            "print(''.join('Y' if m.should_log(m.DEBUG, v) else 'N' "
            "for v in (m.LOWVERBOSITY, m.MEDIUMVERBOSITY, m.HIGHVERBOSITY, m.CHATTERBOX)))"
        )
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
        env.pop("MX_LOG_VERBOSITY", None)
        env.update(environment)
        done = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, timeout=60)
        self.assertEqual(0, done.returncode, done.stderr)
        return done.stdout.strip()

    def test_the_default_shows_connections_not_traffic(self):
        self.assertEqual("YYYN", self.should_log_in_a_fresh_interpreter({}))

    def test_the_variable_turns_traffic_on_or_quiets_a_process(self):
        self.assertEqual("YYYY", self.should_log_in_a_fresh_interpreter({"MX_LOG_VERBOSITY": "DEBUG:CHATTERBOX"}))
        self.assertEqual("YYNN", self.should_log_in_a_fresh_interpreter({"MX_LOG_VERBOSITY": "medium"}))
        self.assertEqual("YYYN", self.should_log_in_a_fresh_interpreter({"MX_LOG_VERBOSITY": "DEBUG:LOUD"}), "ignored")


class StreamingTest(unittest.TestCase):
    """The streamer's forked child, when it cannot become mxcontrol."""

    def test_a_streamer_that_cannot_start_runs_none_of_its_callers_code(self):
        # The caller catches what the call raises, as a server's error
        # handler would, then goes on. The child used to raise out of the
        # failed exec into that handler and go on too: two lines. Stdout is
        # read to its end, which waits for the forked child as well.
        script = (
            "from multiplexer.mxlog.streaming import enable_single_thread_log_streaming\n"
            "try:\n"
            "    enable_single_thread_log_streaming([('127.0.0.1', 1)], mxcontrol='/nonexistent/mxcontrol')\n"
            "except OSError:\n"
            "    pass\n"
            "print('after', flush=True)\n"
        )
        result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
        self.assertEqual(["after"], result.stdout.split(), result.stderr)


if __name__ == "__main__":
    unittest.main()
