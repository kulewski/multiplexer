"""AsyncClient against a real multiplexer and a scripted backend: every
verb, the exceptions, concurrency, cancellation, subscriptions, the queue,
a multiplexer restart, the loop rule, a fork, and the interpreter's exit.
"""

import asyncio
import os
import subprocess
import sys
import time
import unittest

from multiplexer.aio import AsyncClient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import NotConnected, OperationFailed, OperationTimedOut
from multiplexer.testing import Cluster, FakePeer, TestClient
from multiplexer.threaded_client import BackendError


class AsyncClientTest(unittest.IsolatedAsyncioTestCase):
    """One multiplexer for the class; a fake backend and a client per test."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(1).__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.__exit__(None, None, None)

    async def asyncSetUp(self):
        self.peer = FakePeer(self.cluster, peers.PYTHON_TEST_SERVER).start()
        self.peer.on(types.PYTHON_TEST_REQUEST, self.answer, types.PYTHON_TEST_RESPONSE)
        self.client = AsyncClient(self.cluster.endpoints, peers.PYTHON_TEST_CLIENT)

    async def asyncTearDown(self):
        await self.client.aclose()
        self.peer.stop()

    @staticmethod
    def answer(mxmsg):
        """The fake's script: upper-case, echo, nothing, or an error, by payload."""
        if mxmsg.message == b"":
            return None
        if mxmsg.message == b"raise":
            raise ValueError("as asked")
        return mxmsg.message.upper()

    async def test_query_awaits_the_reply(self):
        reply = await self.client.query(b"pears", types.PYTHON_TEST_REQUEST)
        self.assertEqual((types.PYTHON_TEST_RESPONSE, b"PEARS"), (reply.type, reply.message))
        self.assertEqual(b"PEARS", (await self.client.query("pears", types.PYTHON_TEST_REQUEST)).message)

    async def test_query_pickle_round_trips(self):
        self.peer.on(
            types.PYTHON_TEST_REQUEST,
            lambda m: __import__("pickle").dumps({"echo": __import__("pickle").loads(m.message)}),
            types.PYTHON_TEST_RESPONSE,
        )
        self.assertEqual({"echo": [1, 2]}, await self.client.query_pickle([1, 2], types.PYTHON_TEST_REQUEST))

    async def test_every_failure_is_an_exception(self):
        with self.assertRaises(BackendError):
            await self.client.query(b"raise", types.PYTHON_TEST_REQUEST)
        self.peer.errors.clear()  # the fake would fail the test at stop() for that raise, as designed
        with self.assertRaises((OperationTimedOut, OperationFailed)):
            await self.client.query(b"", types.PYTHON_TEST_REQUEST, timeout=0.5)
        with self.assertRaises(OperationFailed):
            await self.client.query(b"nobody serves this", types.PYTHON_TEST_RESPONSE, timeout=5)
        self.assertEqual(b"STILL", (await self.client.query(b"still", types.PYTHON_TEST_REQUEST)).message)

    async def test_a_hundred_queries_at_once_each_get_their_own_reply(self):
        replies = await asyncio.gather(
            *(self.client.query(b"n%d" % index, types.PYTHON_TEST_REQUEST) for index in range(100))
        )
        self.assertEqual([b"N%d" % index for index in range(100)], [reply.message for reply in replies])

    async def test_cancelling_an_await_leaves_the_client_usable(self):
        task = asyncio.ensure_future(self.client.query(b"", types.PYTHON_TEST_REQUEST, timeout=5))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(b"AFTER", (await self.client.query(b"after", types.PYTHON_TEST_REQUEST)).message)

    async def test_send_message_awaits_the_write_and_the_event_arrives(self):
        started = time.monotonic()
        mxmsg_id = await self.client.send_message(b"event", type=types.PYTHON_TEST_REQUEST)
        self.assertLess(time.monotonic() - started, 5)
        (received,) = self.peer.wait_for(types.PYTHON_TEST_REQUEST, matching=lambda m: m.message == b"event")
        self.assertEqual(mxmsg_id, received.id)
        ids = await asyncio.gather(
            *(self.client.send_message(b"e%d" % index, type=types.PYTHON_TEST_REQUEST) for index in range(100))
        )
        self.assertEqual(100, len(set(ids)))
        self.peer.wait_for(types.PYTHON_TEST_REQUEST, count=100, matching=lambda m: m.message.startswith(b"e"))
        await self.client.send_message(b"everyone", type=types.PYTHON_TEST_REQUEST, multiplexer=AsyncClient.ALL)
        self.peer.wait_for(types.PYTHON_TEST_REQUEST, matching=lambda m: m.message == b"everyone")

    def push(self, payload: bytes, to: int) -> None:
        """Send `payload` straight to a peer, as a backend pushing an event would."""
        with TestClient(self.cluster, peers.WEBSITE) as sender:
            sender.send(payload, types.PYTHON_TEST_RESPONSE, to=to)

    async def test_subscriptions_run_on_the_loop(self):
        seen: list[tuple[str, bytes]] = []
        loop = asyncio.get_running_loop()
        done = asyncio.Event()

        async def coroutine_handler(mxmsg):
            self.assertIs(loop, asyncio.get_running_loop())
            seen.append(("coroutine", mxmsg.message))
            if len(seen) == 3:
                done.set()

        def plain_handler(mxmsg):
            self.assertIs(loop, asyncio.get_running_loop())
            seen.append(("plain", mxmsg.message))
            if len(seen) == 3:
                done.set()

        unsubscribe = self.client.subscribe(types.PYTHON_TEST_RESPONSE, coroutine_handler)
        self.client.subscribe(None, plain_handler, matching=lambda m: m.message == b"only this")
        await loop.run_in_executor(None, self.push, b"only this", self.client.instance_id)
        await loop.run_in_executor(None, self.push, b"not that", self.client.instance_id)
        await asyncio.wait_for(done.wait(), 10)
        self.assertEqual(
            sorted([("coroutine", b"only this"), ("plain", b"only this"), ("coroutine", b"not that")]), sorted(seen)
        )
        unsubscribe()
        await loop.run_in_executor(None, self.push, b"after", self.client.instance_id)
        await asyncio.sleep(0.3)
        self.assertNotIn(("coroutine", b"after"), seen)

    async def test_messages_iterates_and_a_full_queue_drops_the_oldest(self):
        small = AsyncClient(self.cluster.endpoints, peers.PYTHON_TEST_CLIENT, queue_size=3)
        try:
            stream = small.messages(types.PYTHON_TEST_RESPONSE)
            loop = asyncio.get_running_loop()
            for index in range(6):
                await loop.run_in_executor(None, self.push, b"%d" % index, small.instance_id)
            await asyncio.sleep(0.5)  # all six delivered to the loop; the queue kept the last three
            kept = [await asyncio.wait_for(stream.__anext__(), 10) for _ in range(3)]
            self.assertEqual([b"3", b"4", b"5"], [m.message for m in kept])
        finally:
            await small.aclose()

    async def test_a_multiplexer_restart_under_a_live_client(self):
        self.assertEqual(b"BEFORE", (await self.client.query(b"before", types.PYTHON_TEST_REQUEST)).message)
        self.cluster.mx[0].restart()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
        started = time.monotonic()
        self.assertEqual(b"AFTER", (await self.client.query(b"after", types.PYTHON_TEST_REQUEST, timeout=15)).message)
        self.assertLess(time.monotonic() - started, 9, "within the reconnect delay")

    async def test_another_loop_is_refused(self):
        client = self.client

        def elsewhere():
            return asyncio.run(client.query(b"x", types.PYTHON_TEST_REQUEST))

        with self.assertRaises(RuntimeError):
            await asyncio.get_running_loop().run_in_executor(None, elsewhere)
        with self.assertRaises(RuntimeError):
            await asyncio.get_running_loop().run_in_executor(None, lambda: client._check_loop())


class HolderTest(unittest.TestCase):
    """One client per process, on the loop that first asks; a forked child
    gets its own, the way an ASGI server's workers do after forking."""

    def test_the_holder_makes_one_client_per_process(self):
        with Cluster(1) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:
            peer.on(types.PYTHON_TEST_REQUEST, lambda m: m.message.upper(), types.PYTHON_TEST_RESPONSE)
            holder = AsyncClient.holder(peers.PYTHON_TEST_CLIENT, lambda: cluster.endpoints)

            async def use(payload: bytes) -> tuple[AsyncClient, bytes]:
                client = holder.get()
                assert client is holder.get()
                return client, (await client.query(payload, types.PYTHON_TEST_REQUEST)).message

            parent_client, reply = asyncio.run(use(b"parent"))
            self.assertEqual(b"PARENT", reply)
            pid = os.fork()
            if pid == 0:  # the child: its own loop, its own client from the same holder
                try:
                    child_client, child_reply = asyncio.run(use(b"child"))
                    assert child_client is not parent_client, "the parent's client leaked into the child"
                    assert child_reply == b"CHILD", child_reply
                    child_client.close()
                    os._exit(0)
                except BaseException as error:  # noqa: BLE001  reported through the exit code
                    print("child failed:", repr(error), file=sys.stderr)
                    os._exit(1)
            _, status = os.waitpid(pid, 0)
            self.assertEqual(0, os.waitstatus_to_exitcode(status))
            holder.close()


EXIT_PROBE = """
import asyncio, sys
from multiplexer.aio import AsyncClient
host, port = sys.argv[1].rsplit(":", 1)

async def main():
    client = AsyncClient([(host, int(port))], type=%(peer)d)
    try:
        await client.query(b"nobody serves this", type=%(request)d, timeout=0.05)
    except Exception:
        pass
    # leave without closing: the io thread is still alive when the interpreter exits

asyncio.run(main())
print("leaving with the io thread alive")
"""


class InterpreterExitTest(unittest.TestCase):
    """A program that exits with an AsyncClient alive exits cleanly."""

    def test_exit_with_a_live_client(self):
        with Cluster(1) as cluster:
            env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
            program = EXIT_PROBE % {"peer": peers.PYTHON_TEST_CLIENT, "request": types.PYTHON_TEST_RESPONSE}
            for _ in range(5):
                result = subprocess.run(
                    [sys.executable, "-c", program, cluster.addresses[0]],
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=30,
                )
                self.assertEqual(0, result.returncode, result.stderr.decode(errors="replace")[-2000:])


if __name__ == "__main__":
    unittest.main()
