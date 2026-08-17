"""thousands of queries do not make the multiplexer, the backend or the client grow.

Exact numbers, not resident size: each process reports the bytes it has
allocated from the C heap (glibc's own count), the Python roles also what
the interpreter has allocated (tracemalloc) and how many objects exist, and
the multiplexer logs its heap after every so many routed messages. A leak
per message shows as growth in the second half of the run comparable to the
first; caches and buffers plateau after the first stretch. Every language
combination runs, with the synchronous Client and with a ThreadedClient.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 12000
EVERY = 1000
MARKS = (2000, 7000, 12000)
HEAP_SLACK = 64 * 1024  # bytes the second half may grow on its own
PY_SLACK = 16 * 1024
OBJECTS_SLACK = 50


class SoakMemory(unittest.TestCase):
    """Checks that thousands of queries do not make any process grow."""

    def assert_plateau(self, name: str, marks: list[int], values: list[int], slack: int) -> None:
        """Growth between the second and third mark is at most `slack`, or
        half of the growth between the first two (the warm-up)."""
        first_half = values[1] - values[0]
        second_half = values[2] - values[1]
        self.assertLessEqual(
            second_half, max(slack, first_half // 2), "%s keeps growing: %s at %s messages" % (name, values, marks)
        )

    def values_at(self, events: list[dict], field: str, marks: tuple) -> list[int]:
        """The `field` of the memory event reported at each mark."""
        by_after = {event["after"]: event for event in events}
        for mark in marks:
            self.assertIn(mark, by_after, "no memory event at %d; got %s" % (mark, sorted(by_after)))
        return [by_after[mark][field] for mark in marks]

    def run_and_check(self, threaded: bool) -> None:
        """Run the queries with memory reports, then check every process."""
        cfg = harness.CONFIG
        with Cluster(1, memory_log_every=EVERY) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
                memory_every=EVERY,
            )
            backend.wait_for("connected", connections=1)
            options = dict(threaded=True, **{"async": 8}) if threaded else {}
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_ACTIVE_CLIENT if threaded else C.peers.TEST_CLIENT,
                count=QUERIES,
                memory_every=EVERY,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
                **options,
            )
            self.assertEqual(0, client.wait(timeout=300))
            self.assertEqual([], client.events_of("error"))
            self.assertEqual(QUERIES, len(client.events_of("response")))
            self.assertEqual(0, backend.stop())

            for role, name in ((client, "client"), (backend, "backend")):
                memory = role.events_of("memory")
                self.assert_plateau(name + " C heap", MARKS, self.values_at(memory, "heap_bytes", MARKS), HEAP_SLACK)
                if role.lang == "py":
                    self.assert_plateau(
                        name + " Python heap", MARKS, self.values_at(memory, "py_bytes", MARKS), PY_SLACK
                    )
                    self.assert_plateau(
                        name + " objects", MARKS, self.values_at(memory, "objects", MARKS), OBJECTS_SLACK
                    )
            # The multiplexer routes two messages per query (request and reply).
            samples = dict(cluster.mx[0].memory_samples())
            mx_marks = tuple(2 * mark for mark in MARKS)
            for mark in mx_marks:
                self.assertIn(
                    mark, samples, "no multiplexer memory line at %d messages; got %s" % (mark, sorted(samples))
                )
            self.assert_plateau("multiplexer C heap", list(mx_marks), [samples[mark] for mark in mx_marks], HEAP_SLACK)

    def test_synchronous_client(self):
        self.run_and_check(threaded=False)

    def test_threaded_client(self):
        self.run_and_check(threaded=True)


if __name__ == "__main__":
    harness.main()
