"""mxcontrol rules and mxcontrol recording stop waiting for answers at their
--timeout, however fast other messages keep coming to them: a multiplexer
that answers nothing, and writes the controller messages addressed to it
as fast as the controller reads them, has the command end with 1 and say
that the multiplexer did not answer. Before, past the deadline each loop
read on 10 ms at a time and went past every message that was no answer,
so that a steady stream kept it going for as long as the stream lasted.
Counted, not timed: the stream never pauses, so on the code before the
command was still reading when the test's bound, a failure detector, ran
out.
"""

import itertools
import socket
import subprocess
import threading
import unittest

from multiplexer.Multiplexer_pb2 import MultiplexerMessage, WelcomeMessage
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import mx_runfile
from multiplexer.testing.raw_peer import HEADER, frame

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")
BOUND = 60  # seconds a command may take before the test calls it stuck: a failure detector only
BATCH = 100  # messages the stand-in writes at a time


class Flooding:
    """A multiplexer of the test's own on 127.0.0.1 that welcomes one
    controller and answers nothing: it reads and drops whatever the
    controller sends, and writes it messages addressed to it, each with an
    id of its own, as fast as the controller reads them, until stop()."""

    def __init__(self) -> None:
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.port: int = self.listener.getsockname()[1]
        self.id = 0x5100 + self.port  # this multiplexer's instance id
        self.connection: socket.socket | None = None
        self.stopped = threading.Event()
        self.written = 0  # messages written to the controller
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _read_exactly(self, count: int) -> bytes:
        """`count` bytes from the controller; ConnectionError when it ends first."""
        assert self.connection is not None
        data = b""
        while len(data) < count:
            chunk = self.connection.recv(count - len(data))
            if not chunk:
                raise ConnectionError("the connection ended")
            data += chunk
        return data

    def _frame(self) -> MultiplexerMessage:
        """The controller's next frame, as a message."""
        length, _ = HEADER.unpack(self._read_exactly(HEADER.size))
        return MultiplexerMessage.FromString(self._read_exactly(length))

    def _drain(self) -> None:
        """Reads and drops every frame the controller sends, so that its
        writes never wait, until the connection ends."""
        try:
            while True:
                self._frame()
        except (OSError, ConnectionError):
            pass

    def _serve(self) -> None:
        """The connection, the welcomes, then the stream, until stop() or
        the controller's end."""
        try:
            self.connection, _ = self.listener.accept()
            self.connection.settimeout(BOUND)
            controller = self._frame().sender
            welcome = MultiplexerMessage(
                id=1,
                type=types.CONNECTION_WELCOME,
                message=WelcomeMessage(type=peers.MULTIPLEXER, id=self.id).SerializeToString(),
            )
            welcome.sender = self.id
            self.connection.sendall(frame(welcome.SerializeToString()))
            threading.Thread(target=self._drain, daemon=True).start()
            ids = itertools.count(2)
            while not self.stopped.is_set():
                batch = b""
                for _ in range(BATCH):
                    mxmsg = MultiplexerMessage(id=next(ids), to=controller, type=types.TEST_EVENT, message=b"x")
                    mxmsg.sender = self.id
                    batch += frame(mxmsg.SerializeToString())
                self.connection.sendall(batch)
                self.written += BATCH
        except (OSError, ConnectionError):
            pass  # the controller ended its side

    def stop(self) -> None:
        """Ends the stream and the connection."""
        self.stopped.set()
        if self.connection is not None:
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.thread.join(BOUND)
        self.listener.close()


class AnswerDeadlineTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_stream_that_is_no_answer_keeps_nothing_going(self) -> None:
        """Both commands, one second's --timeout, against the flooding
        stand-in: each ends with 1, saying the multiplexer did not answer,
        the stream still going."""
        for command in (["rules", "status"], ["recording", "status"]):
            with self.subTest(command=command[0]):
                stand_in = Flooding()
                process = subprocess.Popen(
                    [MXCONTROL, *command, "--multiplexer=127.0.0.1:%d" % stand_in.port, "--timeout=1"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                try:
                    _, errors = process.communicate(timeout=BOUND)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate()
                    self.fail("still waiting for an answer %d s after its 1 s --timeout" % BOUND)
                finally:
                    stand_in.stop()
                self.assertEqual(1, process.returncode, errors[-2000:])
                self.assertIn("1 of 1 multiplexer(s) did not answer", errors)
                self.assertGreater(stand_in.written, BATCH, "the stream reached the controller")


if __name__ == "__main__":
    unittest.main()
