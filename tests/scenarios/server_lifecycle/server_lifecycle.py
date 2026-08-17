"""the server reports the port it bound and exits cleanly on SIGTERM."""

import socket
import unittest

from tests import harness
from tests.harness import Cluster


class ServerLifecycle(unittest.TestCase):
    """Checks that the server reports the port it bound and exits cleanly on SIGTERM."""

    def test_port_file_and_clean_shutdown(self):
        with Cluster(1) as cluster:
            mx = cluster.mx[0]
            self.assertNotEqual(0, mx.port, "port file must carry the bound port")
            socket.create_connection(mx.endpoint, timeout=5).close()
            self.assertEqual(0, mx.stop(), "SIGTERM must end in exit code 0")
            with open(mx.log_path, "rb") as f:
                log = f.read().decode("utf-8", "replace")
            self.assertIn("received signal", log)
            self.assertIn("MX server stopped", log)

    def test_restart_keeps_the_address(self):
        with Cluster(1) as cluster:
            mx = cluster.mx[0]
            before = mx.address
            mx.restart()
            self.assertEqual(before, mx.address)
            socket.create_connection(mx.endpoint, timeout=5).close()


if __name__ == "__main__":
    harness.main()
