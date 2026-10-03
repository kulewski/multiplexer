"""A SyncClient destroyed while a name lookup of its is under way waits for
the lookup, which asio's resolver thread runs, as the client's io_service
ends, and it waits without the GIL, so that the program's other threads go
on, where it held the GIL as long as the lookup took, the system's DNS
timeout against a name server that does not answer. Shown, not timed, in a
child Python with lookup_join_probe.so preloaded: the probe holds the lookup
of lookup-under-way.test until its thread is being joined, then takes the
GIL there, which it gets only if the destruction let go of it. Where the
destruction holds the GIL the child never ends, the probe waiting for the
GIL and the destruction for the probe; the bound on the child only detects
that.
"""

import os
import subprocess
import sys
import unittest

from multiplexer.multiplexer_constants import peers
from multiplexer.testing import runfile

PROBE = runfile("multiplexer/lookup_join_probe.so")

# The child: a client whose one address is a name the probe holds the
# lookup of, destroyed once the lookup waits in the probe.
PROGRAM = """
import ctypes
import sys

from multiplexer.clients import SyncClient

probe = ctypes.CDLL(sys.argv[1])
client = SyncClient([], type=%(type)d)
client.async_connect(("lookup-under-way.test", 1980))
probe.wait_for_lookup()
del client  # its io_service ends here, joining the lookup's thread
print("gil free while joined: %%d" %% ctypes.c_int.in_dll(probe, "gil_free_while_joined").value)
"""


class SyncClientDestroyTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_lookup_under_way_is_waited_for_without_the_gil(self) -> None:
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
        # After what is preloaded already, a sanitizer's runtime say, which must come first.
        env["LD_PRELOAD"] = " ".join(filter(None, [os.environ.get("LD_PRELOAD"), PROBE]))
        try:
            result = subprocess.run(
                [sys.executable, "-c", PROGRAM % {"type": peers.WEBSITE}, PROBE],
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except subprocess.TimeoutExpired:
            self.fail("the client's destruction held the GIL while it waited for the lookup: the child never ended")
        said = [line for line in result.stderr.splitlines() if not line.startswith(("[DEBUG]", "[INFO]"))]
        self.assertEqual(0, result.returncode, "\n".join(said[-30:]))
        self.assertIn("gil free while joined: 1", result.stdout)


if __name__ == "__main__":
    unittest.main()
