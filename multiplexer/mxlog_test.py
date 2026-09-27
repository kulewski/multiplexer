"""Unit tests for multiplexer.mxlog: entries with data must not fail, the
log streamer's child never returns into its caller's code, a worker forked
from a streaming process that turns streaming on again streams to a
streamer of its own, and a process with stdin closed streams too."""

import io
import os
import subprocess
import sys
import tempfile
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

    def streams_of(self, script: str) -> tuple[dict[str, bytes], str]:
        """Run `script` in a fresh interpreter, with a shell script standing
        in for mxcontrol as sys.argv[1]: each copy of it writes what it reads
        to a file of its own. Returns what each copy read, by file name, and
        the script's stderr; stderr is read to its end, which waits for
        every copy."""
        directory = tempfile.mkdtemp(dir=os.environ.get("TEST_TMPDIR"))
        streamer = os.path.join(directory, "streamer")
        with open(streamer, "w") as script_file:
            script_file.write('#!/bin/sh\nexec cat > "$0.$$"\n')
        os.chmod(streamer, 0o755)
        result = subprocess.run([sys.executable, "-c", script, streamer], capture_output=True, text=True, timeout=60)
        self.assertEqual(0, result.returncode, result.stderr)
        streams = {}
        for name in os.listdir(directory):
            if name.startswith("streamer."):
                with open(os.path.join(directory, name), "rb") as stream:
                    streams[name] = stream.read()
        return streams, result.stderr

    def test_a_worker_that_turns_streaming_on_again_streams_to_its_own_streamer(self):
        # The documented use: a process streams, forks a worker, and the
        # worker turns streaming on again. Python closed the stream's
        # descriptor, which C++ still owned, the new pipe took its number,
        # and C++ closed that as it replaced the stream: the worker's entries
        # went nowhere.
        streams, stderr = self.streams_of(
            "import os, sys\n"
            "from multiplexer import mxlog\n"
            "from multiplexer.mxlog.streaming import enable_single_thread_log_streaming\n"
            "enable_single_thread_log_streaming([('127.0.0.1', 1)], mxcontrol=sys.argv[1])\n"
            "mxlog.log(mxlog.ERROR, mxlog.LOWVERBOSITY, text='from the parent')\n"
            "pid = os.fork()\n"
            "if pid == 0:\n"
            "    enable_single_thread_log_streaming([('127.0.0.1', 1)], mxcontrol=sys.argv[1])\n"
            "    mxlog.log(mxlog.ERROR, mxlog.LOWVERBOSITY, text='from the worker')\n"
            "    os._exit(0)\n"
            "os.waitpid(pid, 0)\n"
        )
        self.assertEqual(2, len(streams), stderr)
        parent = [name for name, data in streams.items() if b"from the parent" in data]
        worker = [name for name, data in streams.items() if b"from the worker" in data]
        self.assertEqual(1, len(parent), stderr)
        self.assertEqual(1, len(worker), "the worker's entry reached no streamer, or both")
        self.assertNotEqual(parent, worker)

    def test_a_process_with_stdin_closed_streams(self):
        # The pipe's reading end took descriptor 0, and the streamer, which
        # reads stdin, started with it closed: close-on-exec, as os.pipe()
        # makes it, where dup2() onto 0 had made any other inheritable.
        streams, stderr = self.streams_of(
            "import os, sys\n"
            "os.close(0)\n"
            "from multiplexer import mxlog\n"
            "from multiplexer.mxlog.streaming import enable_single_thread_log_streaming\n"
            "enable_single_thread_log_streaming([('127.0.0.1', 1)], mxcontrol=sys.argv[1])\n"
            "mxlog.log(mxlog.ERROR, mxlog.LOWVERBOSITY, text='with stdin closed')\n"
        )
        self.assertEqual(1, len(streams), stderr)
        self.assertIn(b"with stdin closed", list(streams.values())[0], stderr)


if __name__ == "__main__":
    unittest.main()
