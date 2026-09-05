"""Test infrastructure for programs built on the multiplexer: real multiplexer
processes, peers as processes or in-process fakes, and the waits that hold a
test together. Importable from any workspace that depends on @mx as
multiplexer.testing; this repository's own integration tests (tests/) are
built on it.

Cluster starts one or more multiplexers on ephemeral ports (Mx is one of
them). spawn() launches a role process that plays one peer and reports what it
does as Event lines on stdout (events.proto, one message per line in protocol
buffer text format); Role collects those events as dicts. The roles this
repository ships (tests/roles, in Python and C++) play a client or a backend
with scripted behaviour; a binary of your own plays a role by following the
same contract, see tests/README.md. FakePeer, BackendThread and TestClient
(fakes.py) are the in-process counterparts for unit tests; RawPeer
(raw_peer.py) speaks the wire format by hand. Every process's stderr is saved
under the test's undeclared outputs directory.

A scenario is a unittest file that calls main(); mx_integration_test
(defs.bzl) passes it --mx (how many multiplexers), --roles (who plays each
role), --rules (the rules file) and --params, available as CONFIG.
"""

import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from typing import Any, Callable

from google.protobuf import text_format

from multiplexer.testing import events_pb2

Event = dict[str, Any]

# The root of the multiplexer repository, in the runfiles tree of a test or
# checked out: this file is multiplexer/testing/__init__.py under it. Works
# whatever the repository is called in the consuming workspace.
_MX_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def mx_runfile(path: str) -> str:
    """The absolute path of a file of the multiplexer repository, given
    relative to its root: "mxcontrol/mxcontrol", "tests/roles/py/backend"."""
    return os.path.join(_MX_ROOT, path)


def mxcontrol_path() -> str:
    """The multiplexer binary: `$MXCONTROL` when set, for a build without
    Bazel (make), else the one in this repository's runfiles."""
    return os.environ.get("MXCONTROL") or mx_runfile("mxcontrol/mxcontrol")


def runfile(path: str) -> str:
    """The absolute path of a file of the test's own workspace in its
    runfiles, given as Bazel's $(rootpath) prints it; "external/REPO/..."
    names a file of another repository. Outside Bazel `path` is taken as
    is, relative to the working directory."""
    root = os.environ.get("TEST_SRCDIR")
    if not root:
        return os.path.abspath(path)
    parts = os.path.normpath(path).split("/", 2)
    if parts[0] == "external" and len(parts) == 3:
        return os.path.join(root, parts[1], parts[2])
    return os.path.join(root, os.environ.get("TEST_WORKSPACE", "__main__"), path)


def output_dir() -> str:
    """Where logs and events go: Bazel's undeclared outputs directory, its
    temporary directory, or a fresh temporary directory outside Bazel."""
    directory = os.environ.get("TEST_UNDECLARED_OUTPUTS_DIR") or os.environ.get("TEST_TMPDIR")
    if not directory:
        directory = tempfile.mkdtemp(prefix="mxtest-")
    os.makedirs(directory, exist_ok=True)
    return directory


def cpu_seconds(pid: int) -> float:
    """The CPU time the process has used so far, user plus system, in
    seconds, from /proc; 0 if it is gone. Exact, unlike sampling: an idle
    process barely moves it, a spinning one burns a whole core into it."""
    try:
        with open("/proc/%d/stat" % pid) as stat:
            fields = stat.read().rsplit(")", 1)[1].split()
    except OSError:
        return 0.0
    utime, stime = int(fields[11]), int(fields[12])  # fields 14 and 15 of the man page, after the comm
    return (utime + stime) / os.sysconf("SC_CLK_TCK")


def rss_kb(pid: int) -> int:
    """The process's resident set size in KiB, from /proc; 0 if it is gone."""
    try:
        with open("/proc/%d/status" % pid) as status:
            for line in status:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def child_env(native: bool) -> dict[str, str]:
    """The environment for a child process. Under an AddressSanitizer build
    (check.sh --leaks) leak detection is on for the C++ processes, the
    multiplexer and the C++ roles, whose leaks fail their exit code, and off
    for Python processes, whose interpreter's own allocations would drown
    ours. Harmless in a normal build."""
    env = dict(os.environ)
    env["ASAN_OPTIONS"] = "detect_leaks=%d:exitcode=23" % (1 if native else 0)
    return env


def tail(path: str, lines: int = 20) -> str:
    """The last `lines` lines of a file, for failure messages."""
    try:
        with open(path, "rb") as log:
            data = log.read().decode("utf-8", "replace").splitlines()
        return "\n".join(data[-lines:])
    except OSError:
        return "(no log)"


class Config:
    """What mx_integration_test passed on the command line: the number of
    multiplexers, who plays each role, the rules file, and free-form
    parameters."""

    def __init__(self, argv: list[str] | None = None):
        argv = list(sys.argv[1:] if argv is None else argv)
        self.mx = 1
        self.roles: dict[str, str] = {}
        self.role_binaries: dict[str, str] = {}
        self.rules: str | None = None
        self.params: dict[str, Any] = {}
        self.rest: list[str] = []  # what is left for unittest
        arguments = iter(argv)
        for argument in arguments:
            if argument == "--mx":
                self.mx = int(next(arguments))
            elif argument == "--roles":
                self.roles = json.loads(next(arguments))
            elif argument == "--role-binaries":
                self.role_binaries = json.loads(next(arguments))
            elif argument == "--rules":
                self.rules = runfile(next(arguments))
            elif argument == "--params":
                self.params = json.loads(next(arguments))
            else:
                self.rest.append(argument)

    def lang(self, role: str, default: str = "py") -> str:
        """Who plays `role` in this configuration: "py" or "cc" for the roles
        this repository ships, "bin" for a binary named in the BUILD file."""
        return self.roles.get(role, default)

    def param(self, name: str, default: Any = None) -> Any:
        """A free-form parameter from the BUILD file's `params`."""
        return self.params.get(name, default)


# The harness configuration of the running test, set by Cluster.
CONFIG: Config | None = None


class Mx:
    """One multiplexer process, started on an ephemeral port unless told
    otherwise. Its log goes to mx<index>.log in the output directory."""

    def __init__(
        self,
        index: int,
        rules: str,
        address: str = "127.0.0.1:0",
        memory_log_every: int = 0,
        record: bool = False,
        record_payload_bytes: int = 0,
    ):
        self.index = index
        self.rules = rules
        self.address = address
        self.memory_log_every = memory_log_every
        self.record = record
        self.record_payload_bytes = record_payload_bytes
        self.proc: subprocess.Popen | None = None
        self.log_path = os.path.join(output_dir(), "mx%d.log" % index)
        self.port_file = os.path.join(output_dir(), "mx%d.port" % index)
        # --peers-file: one line per connected peer, rewritten by the
        # multiplexer on every change; connected_peers() reads it.
        self.peers_file = os.path.join(output_dir(), "mx%d.peers" % index)
        # --record, when asked for: the recording, readable with multiplexer.recording.
        self.record_file = os.path.join(output_dir(), "mx%d.rec" % index)

    @property
    def host(self) -> str:
        """The host part of the address."""
        return self.address.rsplit(":", 1)[0]

    @property
    def port(self) -> int:
        """The port part of the address; the real port once started."""
        return int(self.address.rsplit(":", 1)[1])

    @property
    def endpoint(self) -> tuple[str, int]:
        """(host, port), as the client libraries take it."""
        return (self.host, self.port)

    def start(self, timeout: float = 15) -> "Mx":
        """Start the process and wait until it has written its port file, so
        that `address` names the port it actually listens on."""
        if os.path.exists(self.port_file):
            os.unlink(self.port_file)
        command = [
            mxcontrol_path(),
            "run_multiplexer",
            "--address",
            self.address,
            "--rules",
            self.rules,
            "--port-file",
            self.port_file,
            "--peers-file",
            self.peers_file,
        ]
        if self.memory_log_every:
            command += ["--memory-log-every", str(self.memory_log_every)]
        if self.record:
            command += ["--record", self.record_file, "--record-payload-bytes", str(self.record_payload_bytes)]
        self._log = open(self.log_path, "ab")
        self.proc = subprocess.Popen(command, stdout=self._log, stderr=self._log, env=child_env(native=True))
        deadline = time.time() + timeout
        while not os.path.exists(self.port_file):
            if self.proc.poll() is not None:
                raise RuntimeError(
                    "mx%d exited with %d before listening:\n%s"
                    % (self.index, self.proc.returncode, tail(self.log_path))
                )
            if time.time() > deadline:
                raise RuntimeError("mx%d did not report its port within %ss" % (self.index, timeout))
            time.sleep(0.02)
        with open(self.port_file) as port_file:
            self.address = port_file.read().strip()
        return self

    def stop(self, timeout: float = 10) -> int | None:
        """Ask the process to exit (SIGTERM, which it handles by closing
        everything and exiting 0), wait up to `timeout`, and return its exit
        code. Falls back to kill() if it does not exit in time. None if it
        was never started; the old exit code if it had already exited."""
        if self.proc is None or self.proc.poll() is not None:
            return None if self.proc is None else self.proc.returncode
        self.proc.terminate()
        try:
            return self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            return self.proc.wait(5)
        finally:
            self._log.close()

    def kill(self) -> None:
        """End the process at once (SIGKILL), for scenarios that simulate a
        crash rather than a shutdown. Nothing happens if it is not running."""
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(5)
            self._log.close()

    def restart(self) -> "Mx":
        """stop() then start() on the same address: what a supervisor does.
        Peers see their connection drop and reconnect on their own."""
        self.stop()
        return self.start()

    def pause(self) -> None:
        """Freeze the process (SIGSTOP): a hung multiplexer. Its sockets stay
        open, so peers notice nothing until their heartbeats go unanswered."""
        assert self.proc is not None
        self.proc.send_signal(signal.SIGSTOP)

    def resume(self) -> None:
        """Unfreeze the process (SIGCONT)."""
        assert self.proc is not None
        self.proc.send_signal(signal.SIGCONT)

    @property
    def running(self) -> bool:
        """Whether the process is alive right now."""
        return self.proc is not None and self.proc.poll() is None

    def cpu_seconds(self) -> float:
        """CPU time the multiplexer process has used so far, in seconds."""
        return cpu_seconds(self.proc.pid) if self.proc else 0.0

    def connected_peers(self) -> list[tuple[int, str, int]]:
        """(instance id, peer type name, peer type) for every peer registered
        on this multiplexer right now, from its peers file."""
        try:
            with open(self.peers_file) as peers:
                lines = peers.read().splitlines()
        except OSError:
            return []
        result = []
        for line in lines:
            instance_id, name, peer_type = line.split()
            result.append((int(instance_id), name, int(peer_type)))
        return result

    def rss_kb(self) -> int:
        """The multiplexer's resident set size in KiB right now."""
        return rss_kb(self.proc.pid) if self.proc else 0

    def memory_samples(self) -> list[tuple[int, int]]:
        """(messages routed, C heap bytes in use) from the log lines written
        every memory_log_every messages."""
        samples = []
        try:
            with open(self.log_path, "rb") as log:
                for line in log:
                    match = re.search(rb"memory: heap_in_use=(\d+) messages=(\d+)", line)
                    if match:
                        samples.append((int(match.group(2)), int(match.group(1))))
        except OSError:
            pass
        return samples


def default_rules() -> str:
    """The rules file a Cluster uses when given none: the one the BUILD file
    named (mx_integration_test's `rules`), else the file the
    multiplexer_rules flag names, which is the one the generated constants
    come from, in this workspace or in one that consumes @mx."""
    if CONFIG is not None and CONFIG.rules:
        return CONFIG.rules
    from multiplexer.testing import rules_path  # generated at build time

    return runfile(rules_path.RULES)


class Cluster:
    """`count` independent multiplexers with the same rules file. Use as a
    context manager: entering starts them, leaving stops every role that is
    still running and then the multiplexers."""

    def __init__(
        self,
        count: int = 1,
        rules: str | None = None,
        memory_log_every: int = 0,
        record: bool = False,
        record_payload_bytes: int = 0,
    ):
        rules = rules or default_rules()
        self.mx = [
            Mx(
                index,
                rules,
                memory_log_every=memory_log_every,
                record=record,
                record_payload_bytes=record_payload_bytes,
            )
            for index in range(count)
        ]

    def wait_for_peer(self, peer_type: int | str, count: int = 1, timeout: float = 15) -> None:
        """Block until at least `count` peers of `peer_type` (a peers.* value
        or its name) are registered on every multiplexer; raises
        TimeoutError naming what was missing."""
        deadline = time.time() + timeout
        while True:
            seen = [
                sum(1 for _, name, number in multiplexer.connected_peers() if peer_type in (name, number))
                for multiplexer in self.mx
            ]
            if all(found >= count for found in seen):
                return
            if time.time() > deadline:
                raise TimeoutError(
                    "waited %ss for %d peer(s) of type %s on every multiplexer; saw %s"
                    % (timeout, count, peer_type, seen)
                )
            time.sleep(0.02)

    def wait_for_peer_gone(self, peer_type: int | str, timeout: float = 15) -> None:
        """Block until no peer of `peer_type` (a peers.* value or its name)
        is registered on any multiplexer: a backend that was stopped has
        been noticed. Raises TimeoutError naming how many were still there."""
        deadline = time.time() + timeout
        while True:
            seen = [
                sum(1 for _, name, number in multiplexer.connected_peers() if peer_type in (name, number))
                for multiplexer in self.mx
            ]
            if not any(seen):
                return
            if time.time() > deadline:
                raise TimeoutError(
                    "waited %ss for every peer of type %s to be gone from every multiplexer; saw %s"
                    % (timeout, peer_type, seen)
                )
            time.sleep(0.02)

    def __enter__(self) -> "Cluster":
        for multiplexer in self.mx:
            multiplexer.start()
        return self

    def __exit__(self, *exc: object) -> None:
        for role in list(Role.running_roles()):
            role.stop()
        for multiplexer in self.mx:
            multiplexer.stop()

    @property
    def addresses(self) -> list[str]:
        """Every multiplexer's host:port, as the roles' --mx takes it."""
        return [multiplexer.address for multiplexer in self.mx]

    @property
    def endpoints(self) -> list[tuple[str, int]]:
        """Every multiplexer's (host, port), as the client libraries take it."""
        return [multiplexer.endpoint for multiplexer in self.mx]


class Role:
    """A launched role process and the events it printed.

    A reader thread parses stdout into `events` as they arrive; wait_for()
    blocks on a condition variable until a matching event exists or the
    process has exited, so a scenario never polls and fails fast with the
    process's last events and stderr when the role dies.
    """

    _all: list["Role"] = []
    _counter = 0

    def __init__(self, role: str, lang: str, argv: list[str], name: str | None = None, drain_file: bool | None = None):
        Role._counter += 1
        self.role, self.lang = role, lang
        self.name = name or "%s-%s-%d" % (role, lang, Role._counter)
        executable = self.executable(role, lang)
        # A backend role is asked to leave by creating its drain file, the way
        # a preStop hook would; see request_drain(). The backends this
        # repository ships take --drain-file; a binary of your own gets it
        # only when asked, drain_file=True.
        self.drain_file = os.path.join(output_dir(), self.name + ".drain")
        if drain_file is None:
            drain_file = role == "backend" and lang in ("py", "cc")
        if drain_file:
            argv = argv + ["--drain-file", self.drain_file]
        self.argv = executable + argv
        self.events: list[Event] = []
        self._eof = False
        self._cond = threading.Condition()
        self.stderr_path = os.path.join(output_dir(), self.name + ".stderr.log")
        self._stderr = open(self.stderr_path, "wb")
        self.events_path = os.path.join(output_dir(), self.name + ".events.txt")
        self._events_file = open(self.events_path, "w")
        self.proc = subprocess.Popen(
            self.argv, stdout=subprocess.PIPE, stderr=self._stderr, env=child_env(native=(lang == "cc"))
        )
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()
        Role._all.append(self)

    @staticmethod
    def executable(role: str, lang: str) -> list[str]:
        """The command that plays `role`: this repository's Python or C++
        role for "py" and "cc", the binary the BUILD file named for it (see
        mx_integration_test) for "bin"."""
        if lang == "py":
            return [mx_runfile("tests/roles/py/" + role)]
        if lang == "cc":
            return [mx_runfile("tests/roles/cc/mxtestroles"), role]
        if lang == "bin":
            if CONFIG is None or role not in CONFIG.role_binaries:
                raise ValueError("no binary was named for role %r; see mx_integration_test's roles" % role)
            return [runfile(CONFIG.role_binaries[role])]
        raise ValueError("unknown role language %r; expected py, cc or bin" % lang)

    @classmethod
    def running_roles(cls) -> list["Role"]:
        """Every role launched so far whose process is still alive."""
        return [role for role in cls._all if role.proc.poll() is None]

    @staticmethod
    def parse_event(line: str) -> Event:
        """An Event line as a dict of the fields it set, with Python values
        (ints stay ints); a line that is not an Event becomes a "stdout"
        event so nothing is lost."""
        try:
            message = text_format.Parse(line, events_pb2.Event())
        except text_format.ParseError:
            return {"event": "stdout", "line": line}
        return {descriptor.name: value for descriptor, value in message.ListFields()}

    def _read(self) -> None:
        """The reader thread: one Event per stdout line into `events`, also
        copied to the events file."""
        assert self.proc.stdout is not None
        for raw in self.proc.stdout:
            line = raw.decode("utf-8", "replace").strip()
            if not line:
                continue
            event = self.parse_event(line)
            self._events_file.write(line + "\n")
            self._events_file.flush()
            with self._cond:
                self.events.append(event)
                self._cond.notify_all()
        self._events_file.close()
        with self._cond:
            self._eof = True
            self._cond.notify_all()

    @staticmethod
    def _matches(event: Event, name: str, match: dict[str, Any]) -> bool:
        """Whether `event` is a `name` event whose fields include `match`."""
        return event.get("event") == name and all(event.get(key) == value for key, value in match.items())

    def events_of(self, name: str, **match: Any) -> list[Event]:
        """Every `name` event seen so far whose fields include `match`."""
        with self._cond:
            return [event for event in self.events if self._matches(event, name, match)]

    def wait_for(self, name: str, timeout: float = 15, **match: Any) -> Event:
        """Block until a `name` event with fields `match` has arrived and
        return it. Raises if the process exits first or `timeout` passes,
        with the last events and stderr in the message."""
        deadline = time.time() + timeout
        with self._cond:
            while True:
                for event in self.events:
                    if self._matches(event, name, match):
                        return event
                if self._eof and self.proc.poll() is not None:
                    raise RuntimeError(
                        "%s exited with %s before %r %s; events=%r; stderr:\n%s"
                        % (self.name, self.proc.returncode, name, match, self.events[-5:], tail(self.stderr_path))
                    )
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise TimeoutError(
                        "%s: no %r %s within %ss; events=%r; stderr:\n%s"
                        % (self.name, name, match, timeout, self.events[-5:], tail(self.stderr_path))
                    )
                self._cond.wait(min(remaining, 0.5))

    def wait(self, timeout: float = 60) -> int:
        """Wait for the process to finish on its own and return its exit
        code; raises subprocess.TimeoutExpired after `timeout`."""
        exit_code = self.proc.wait(timeout)
        self._reader.join(5)
        self._stderr.close()
        return exit_code

    def request_drain(self) -> None:
        """Ask a backend role to leave by creating its drain file, which its
        periodic_task() notices within one poll: the file mechanism a
        deployment's preStop hook uses, as opposed to a signal. The role then
        stops, or drains first when it was started with drain_seconds."""
        with open(self.drain_file, "w"):
            pass

    def stop(self, timeout: float = 10) -> int | None:
        """Ask the process to exit (SIGTERM; the roles catch it and finish
        cleanly), wait up to `timeout`, kill it if it does not exit, and
        return the exit code."""
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(5)
        self._reader.join(5)
        self._stderr.close()
        return self.proc.returncode

    def kill(self) -> int | None:
        """End the process at once (SIGKILL), simulating a crash, and return
        its exit code."""
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(5)
        return self.proc.returncode

    @property
    def returncode(self) -> int | None:
        """The exit code, or None while the process runs."""
        return self.proc.poll()

    def cpu_seconds(self) -> float:
        """CPU time the role's process has used so far, in seconds."""
        return cpu_seconds(self.proc.pid)

    def rss_kb(self) -> int:
        """The process's resident set size in KiB right now."""
        return rss_kb(self.proc.pid)


def wait_until(predicate: Callable[[], Any], timeout: float, what: str, interval: float = 0.02) -> Any:
    """Poll `predicate` until it returns something true and return that;
    raise TimeoutError naming `what` after `timeout` seconds."""
    deadline = time.time() + timeout
    while True:
        result = predicate()
        if result:
            return result
        if time.time() > deadline:
            raise TimeoutError("waited %ss for %s" % (timeout, what))
        time.sleep(interval)


def _argv(mx: list[str], type: int, opts: dict[str, Any]) -> list[str]:
    """The command line for a role: --mx per address, --type, and one flag
    per option. True becomes a bare flag, False and None are omitted, a dict
    becomes repeated key=value arguments, a list becomes repeated arguments
    (tuples joined with ':'), anything else becomes one argument."""
    argv: list[str] = []
    for address in mx:
        argv += ["--mx", address]
    argv += ["--type", str(type)]
    for key, value in opts.items():
        flag = "--" + key.replace("_", "-")
        if value is True:
            argv.append(flag)
        elif value is False or value is None:
            continue
        elif isinstance(value, dict):
            for inner_key, inner_value in value.items():
                argv += [flag, "%s=%s" % (inner_key, inner_value)]
        elif isinstance(value, (list, tuple)):
            for item in value:
                if isinstance(item, (list, tuple)):
                    argv += [flag, ":".join(str(part) for part in item)]
                else:
                    argv += [flag, str(item)]
        else:
            argv += [flag, str(value)]
    return argv


def spawn(
    role: str,
    lang: str,
    mx: list[str],
    type: int,
    name: str | None = None,
    drain_file: bool | None = None,
    **opts: Any,
) -> Role:
    """Launch a role process. `lang` is who plays it ("py", "cc" or "bin",
    see Role.executable), `mx` the list of host:port addresses, `type` the
    peer type id, `opts` the role's options as keyword arguments (see _argv
    for how they become flags)."""
    return Role(role, lang, _argv(mx, type, opts), name=name, drain_file=drain_file)


def configure(argv: list[str] | None = None) -> Config:
    """Parse the command line mx_integration_test built (sys.argv by
    default) into CONFIG and return it."""
    global CONFIG
    CONFIG = Config(argv)
    return CONFIG


def main() -> None:
    """Entry point for scenario files: parse the harness config, run unittest."""
    config = configure()
    unittest.main(argv=[sys.argv[0]] + config.rest)


# The in-process peers, re-exported so that a test imports everything from
# multiplexer.testing; fakes.py needs Cluster and wait_until, defined above.
from multiplexer.testing.fakes import BackendThread, FakePeer, TestClient  # noqa: E402
from multiplexer.testing.raw_peer import RawPeer  # noqa: E402
