"""The instructions the multiplexer spends routing, counted by valgrind's
callgrind and held to a budget: a query's round trip between the C++ client
and backend roles, its request and its reply each routed alone, as the
client sends the next only once the reply came. The count is the
multiplexer's over LONG round trips less its count over SHORT, so that its
start, its connections and its end cancel out, with the rules file's check,
a timer, turned off. A budget fails past DRIFT either way: a rise is what
the per-message path gained, and a fall a budget to lower, so that the next
rise shows. Counted, not timed: the count holds on a loaded machine, about
1% apart from run to run. Built -c opt, which the budget is for, and run by
./check.sh --valgrind; tagged manual, as valgrind is no requirement of the
build.
"""

import os
import shutil
import subprocess
import tempfile
import time
import unittest

from multiplexer.testing import mxcontrol_path, runfile
from tests.testing_constants import peers, types

# Instructions for one round trip, the multiplexer built -c opt by GCC on
# x86-64; a deliberate change to the per-message path moves it.
BUDGET = 10_900
DRIFT = 0.05
SHORT, LONG = 2000, 6000
BOUND = 600.0  # seconds a run may take before the test calls it stuck: a failure detector only


def wait_for(predicate, what: str) -> None:
    """Wait until `predicate()` holds, BOUND seconds at most."""
    deadline = time.monotonic() + BOUND
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError(what)
        time.sleep(0.05)


def instructions(rounds: int) -> int:
    """The multiplexer's instructions, under callgrind, over `rounds`
    lock-step round trips of 200-byte payloads, from its start to its end."""
    work = tempfile.mkdtemp()
    processes: list[subprocess.Popen] = []
    try:
        out = os.path.join(work, "callgrind.out")
        port_file = os.path.join(work, "port")
        command = ["valgrind", "--tool=callgrind", "--callgrind-out-file=" + out, mxcontrol_path(), "run_multiplexer"]
        command += ["--rules", runfile("tests/testing.rules"), "--address", "127.0.0.1:0", "--port-file", port_file]
        command += ["--drain-seconds", "0", "--rules-check-interval", "0"]
        with open(os.path.join(work, "mx.log"), "wb") as log:
            mx = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        processes.append(mx)
        wait_for(lambda: os.path.exists(port_file) and os.path.getsize(port_file) > 0, "the multiplexer's port")
        with open(port_file) as file:
            address = file.read().strip()
        roles = runfile("tests/roles/cc/mxtestroles")
        backend_log = os.path.join(work, "backend.log")
        with open(backend_log, "wb") as log:
            backend = subprocess.Popen(
                [roles, "backend", "--mx", address, "--type", str(peers.TEST_BACKEND_A)]
                + ["--serves", "%d=%d" % (types.TEST_REQUEST_A, types.TEST_RESPONSE), "--behaviour", "echo"],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        processes.append(backend)
        wait_for(lambda: b"connected" in open(backend_log, "rb").read(), "the backend's connection")
        client = subprocess.run(
            [roles, "client", "--mx", address, "--type", str(peers.TEST_CLIENT), "--query"]
            + ["%d:hello" % types.TEST_REQUEST_A, "--count", str(rounds), "--parallel", "1", "--payload-size", "200"],
            capture_output=True,
            timeout=BOUND,
        )
        answered = client.stdout.count(b"ms:")
        if answered != rounds:
            raise AssertionError("%d of %d round trips: %s" % (answered, rounds, client.stdout[-2000:]))
        for process in (backend, mx):  # the multiplexer's count is written as it exits
            process.terminate()
            process.wait(BOUND)
        with open(out) as file:
            for line in file:
                if line.startswith(("summary:", "totals:")):
                    return int(line.split()[1])
        raise AssertionError("no total in " + out)
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait()
        shutil.rmtree(work, ignore_errors=True)


class InstructionsTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_query_round_trip_keeps_to_its_budget(self) -> None:
        if shutil.which("valgrind") is None:
            self.fail("valgrind is not installed (apt install valgrind)")
        per = (instructions(LONG) - instructions(SHORT)) / (LONG - SHORT)
        print("instructions a round trip: %.0f, budget %d" % (per, BUDGET))
        self.assertLessEqual(per, BUDGET * (1 + DRIFT), "the per-message path grew: %.0f a round trip" % per)
        self.assertGreaterEqual(
            per, BUDGET * (1 - DRIFT), "the per-message path shrank to %.0f a round trip: lower BUDGET" % per
        )


if __name__ == "__main__":
    unittest.main()
