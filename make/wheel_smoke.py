"""Run from a Python that has the wheel installed: what one `pip install
mx-multiplexer` gives, through the installed package alone. The mxcontrol
inside it is executable and runs as the `mxcontrol` command and as `python
-m multiplexer.mxcontrol`; its generate_rules and generate_constants give
back the package's own constants; the multiplexer the command starts is the
package's binary, under the command's pid and name, and ends with 0 on
SIGTERM; the command leaves SIGPIPE and SIGXFSZ at their defaults, which
only run_multiplexer changes, ignoring SIGPIPE itself; a host name
resolves; Client, the synchronous client's name up to 2.3.1, is the class
SyncClient; and a
rules file of the smoke's own, the system rules with a client's and a
backend's types after them, runs two multiplexers that answer every query.
With MXCONTROL set, the harness runs that binary instead, and its rules and
constants must be the package's: the check that the wheel's mxcontrol and
the static one have not drifted apart. It runs from an empty directory, so
that nothing of the source tree can stand in for what the wheel must carry."""

import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

os.chdir(tempfile.mkdtemp())

import multiplexer.mxcontrol
from multiplexer import multiplexer_constants
from multiplexer.clients import Client, SyncClient
from multiplexer.testing import Cluster, FakePeer, ThreadedTestClient

BINARY = multiplexer.mxcontrol.binary_path()
COMMAND = os.path.join(os.path.dirname(sys.executable), "mxcontrol")
PACKAGE = os.path.dirname(os.path.abspath(multiplexer_constants.__file__))
# The client's and the backend's types, after the system rules.
OWN_TYPES = (
    'peer { type: 200 name: "WEB" }\n'
    'peer { type: 201 name: "COMPUTE" }\n'
    'type { type: 301 name: "COMPUTE_REQUEST" to { peer: "COMPUTE" whom: ANY } }\n'
    'type { type: 302 name: "COMPUTE_RESPONSE" }\n'
)


def run(*command: str) -> str:
    """Run a command to completion and return its stdout; a failure is fatal."""
    return subprocess.run(list(command), check=True, capture_output=True, text=True).stdout


def read(path: str) -> bytes:
    """The file's bytes."""
    with open(path, "rb") as handle:
        return handle.read()


def exe(pid: int) -> str:
    """The binary process `pid` runs, every link resolved."""
    return os.path.realpath(os.readlink("/proc/%d/exe" % pid))


def ignored_signals(pid: int) -> int:
    """The mask of the signals process `pid` ignores, bit N - 1 for signal N."""
    with open("/proc/%d/status" % pid) as status:
        return int(re.search(r"SigIgn:\s*([0-9a-f]+)", status.read()).group(1), 16)


# The binary is inside the package, executable.
assert BINARY.startswith(PACKAGE + os.sep), BINARY
assert os.access(BINARY, os.X_OK), BINARY

# The synchronous client under both names is one class.
assert Client is SyncClient, (Client, SyncClient)

# The command and `python -m` run it; help lists its subcommands on stderr.
for command in ([COMMAND, "help"], [sys.executable, "-m", "multiplexer.mxcontrol", "help"]):
    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode == 0 and "generate_rules" in result.stderr, (command, result)

# The system rules, and the package's own constants generated again from them.
run(COMMAND, "generate_rules", "multiplexer.rules")
run(COMMAND, "generate_constants", "multiplexer.rules", "--python", "system.py", "--pyi", "system.pyi")
assert read("system.py") == read(os.path.join(PACKAGE, "multiplexer_constants.py"))
assert read("system.pyi") == read(os.path.join(PACKAGE, "multiplexer_constants.pyi"))

# A multiplexer started by the command: the package's binary under the
# command's pid and name, SIGXFSZ not ignored, and a host name resolved.
# SIGPIPE it ignores itself, so that a log reader that goes away does not
# end it; the command leaves both at their defaults, as streamlogs shows.
process = subprocess.Popen(
    [COMMAND, "run_multiplexer", "--address", "127.0.0.1:0", "--rules", "multiplexer.rules", "--port-file", "port"],
    stderr=subprocess.PIPE,
)
assert process.stderr is not None
for line in process.stderr:  # blocks until the multiplexer says it listens, or ends
    if b"starting MX server" in line:
        break
else:
    raise AssertionError("the multiplexer ended before it listened: %d" % process.wait())
drain = threading.Thread(target=process.stderr.read, daemon=True)
drain.start()
assert exe(process.pid) == os.path.realpath(BINARY), exe(process.pid)
with open("/proc/%d/comm" % process.pid) as comm:
    assert comm.read().strip() == "mxcontrol"
ignored = ignored_signals(process.pid)
assert ignored & 1 << 24 == 0, "SIGXFSZ ignored: %x" % ignored
with open("port") as port_file:
    port = port_file.read().strip().rsplit(":", 1)[1]
run(BINARY, "rules", "status", "-M", "localhost:" + port)
# A command that changes neither: streamlogs, reading its stdin, once the
# command's exec made it the binary.
streamer = subprocess.Popen([COMMAND, "streamlogs", "--multiplexer", "localhost:" + port], stdin=subprocess.PIPE)
deadline = time.monotonic() + 30
while exe(streamer.pid) != os.path.realpath(BINARY):
    assert time.monotonic() < deadline, "the command did not exec the binary"
    time.sleep(0.01)
ignored = ignored_signals(streamer.pid)
assert ignored & (1 << 12 | 1 << 24) == 0, "SIGPIPE or SIGXFSZ ignored: %x" % ignored
assert streamer.stdin is not None
streamer.stdin.close()
assert streamer.wait() == 0, streamer.returncode
process.send_signal(signal.SIGTERM)
assert process.wait() == 0, process.returncode
drain.join()

# The smoke's own rules and constants, then two multiplexers answering.
with open("multiplexer.rules", "a") as rules:
    rules.write(OWN_TYPES)
run(COMMAND, "generate_constants", "multiplexer.rules", "--python", "smoke_constants.py")
# The constants were just written into the working directory, which is not
# on the path of a script run from elsewhere.
sys.path.insert(0, os.getcwd())
from smoke_constants import peers, types

expected = os.path.realpath(shutil.which(os.environ.get("MXCONTROL") or BINARY) or "")
with (
    Cluster(2, rules=os.path.abspath("multiplexer.rules")) as cluster,
    FakePeer(cluster, peers.COMPUTE) as backend,
    ThreadedTestClient(cluster, peers.WEB) as client,
):
    backend.reply_with(types.COMPUTE_REQUEST, b"from the wheel", types.COMPUTE_RESPONSE)
    answers = [client.query(b"hello", types.COMPUTE_REQUEST).message for _ in range(4)]
    assert answers == [b"from the wheel"] * 4, answers
    for mx in cluster.mx:
        assert mx.proc is not None and exe(mx.proc.pid) == expected, (exe(mx.proc.pid), expected)

# With MXCONTROL set, the other binary writes the same rules and constants.
if os.environ.get("MXCONTROL"):
    other = shutil.which(os.environ["MXCONTROL"])
    assert other is not None, os.environ["MXCONTROL"]
    assert run(other, "generate_rules", "-") == run(BINARY, "generate_rules", "-")
    run(other, "generate_constants", "multiplexer.rules", "--python", "other.py", "--pyi", "other.pyi")
    run(BINARY, "generate_constants", "multiplexer.rules", "--python", "ours.py", "--pyi", "ours.pyi")
    assert read("other.py") == read("ours.py") and read("other.pyi") == read("ours.pyi")

print("wheel smoke: ok, mxcontrol at", BINARY, "and", expected)
