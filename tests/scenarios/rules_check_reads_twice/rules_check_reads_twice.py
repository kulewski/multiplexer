"""the periodic rules check puts a change in use only once two checks have read the same bytes: the first step of a file written in two is never applied.

One multiplexer reads its rules file every 0.01 s. The file is a symlink
to a named pipe, a new pipe for every read, so that each check reads what
the test writes next: a check opens the pipe and waits for a writer, the
test's open of it waits for that check, by which the one before is over,
and the link points at the next pipe before this one closes. The file is
written in two steps: the first, the file with one of two new peer types,
valid on its own, is read by one check; the whole file by the next two.
The whole file is put in use at its second read, and the first step
never is, the log says. At the end the link points at a plain file, so
that the checks no longer wait and the multiplexer stops as usual.
"""

import contextlib
import os
import unittest
import zlib
from threading import Thread
from typing import Iterator, TextIO

from tests import harness
from tests.harness import Cluster, output_dir

FIRST = '\npeer {\n    type: 250\n    name: "TEST_FIRST_STEP"\n}\n'
SECOND = '\npeer {\n    type: 251\n    name: "TEST_SECOND_STEP"\n}\n'


def fingerprint(text: str) -> str:
    """What the multiplexer logs for a rules file: the CRC-32 of its bytes, eight hex digits."""
    return "%08x" % zlib.crc32(text.encode())


class Pipes:
    """The rules file `path`, a symlink to a new named pipe for every read."""

    def __init__(self, path: str):
        self.path = path
        self.made = 0
        self.current = self._new_pipe()
        os.symlink(self.current, path)

    def _new_pipe(self) -> str:
        """A fresh named pipe next to the link."""
        self.made += 1
        pipe = "%s.%d" % (self.path, self.made)
        os.mkfifo(pipe)
        return pipe

    def _point(self, target: str) -> None:
        """The link to `target`, in one rename."""
        link = self.path + ".link"
        os.symlink(target, link)
        os.replace(link, self.path)

    @contextlib.contextmanager
    def read(self) -> Iterator[TextIO]:
        """The writer of the next read, once a check opened the pipe: the
        check before is over. The link points at a new pipe before this
        one closes, so that the check after opens that one."""
        with open(self.current, "w") as pipe:
            try:
                yield pipe
            finally:
                self.current = self._new_pipe()
                self._point(self.current)

    def end(self, text: str) -> None:
        """The next read gives `text`, and the link points at a plain file of
        it before the pipe closes: no check waits any more."""
        plain = self.path + ".plain"
        with open(plain, "w") as rules:
            rules.write(text)
        with open(self.current, "w") as pipe:
            self._point(plain)
            pipe.write(text)


class RulesCheckReadsTwice(unittest.TestCase):
    """A file written in two steps: only the second, read twice, is put in use."""

    def test_the_first_step_of_a_write_is_never_applied(self):
        with open(harness.CONFIG.rules) as shipped:
            original = shipped.read()
        first, whole = original + FIRST, original + FIRST + SECOND
        pipes = Pipes(os.path.join(output_dir(), "piped.rules"))

        def starts() -> None:
            """The read at start."""
            with pipes.read() as pipe:
                pipe.write(original)

        starting = Thread(target=starts)
        starting.start()
        with Cluster(1, rules=pipes.path, rules_check_interval=0.01) as cluster:
            starting.join()
            multiplexer = cluster.mx[0]
            try:
                with pipes.read() as pipe:
                    pipe.write(first)
                with pipes.read() as pipe:  # the check that read the first step is over
                    self.assertFalse(multiplexer.log_contains("-> %s," % fingerprint(first)), "the first step")
                    pipe.write(whole)
                with pipes.read() as pipe:  # the one that read the whole file once
                    self.assertFalse(
                        multiplexer.log_contains("-> %s," % fingerprint(whole)), "in use at its first read"
                    )
                    pipe.write(whole)
                with pipes.read() as pipe:  # the one that read it twice
                    self.assertTrue(
                        multiplexer.log_contains("-> %s," % fingerprint(whole)), "in use at its second read"
                    )
                    pipe.write(whole)
            finally:
                pipes.end(whole)
            self.assertFalse(multiplexer.log_contains("-> %s," % fingerprint(first)), "the first step")


if __name__ == "__main__":
    harness.main()
