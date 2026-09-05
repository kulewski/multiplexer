"""Worker threads sharing one ThreadedClient.

Each thread has work of its own and a queue. When it needs the backend it
calls query() with a callback and goes on working; the reply is delivered by the
client's io thread into the worker's queue, which is where the worker picks
it up when it has time. No thread ever blocks on the network, and the other
threads are not involved at all.

Usage: workers [host:port] [workers] [jobs per worker]
"""

import queue
import sys
import threading
import time

from multiplexer.multiplexer_constants import peers, types
from multiplexer.threaded_client import ThreadedClient

print_lock = threading.Lock()


def worker(name: str, client: ThreadedClient, jobs: list[str]) -> None:
    """One thread's life: ask for each job, work meanwhile, report replies."""
    inbox = queue.Queue()  # replies land here, from the io thread
    pending = 0
    for job in jobs:
        client.query(job.encode(), type=types.ECHO_REQUEST, callback=inbox.put)
        pending += 1
        time.sleep(0.05)  # the thread's own work, while the reply is on its way
        # Take whatever replies have arrived meanwhile, without waiting.
        while pending:
            try:
                reply = inbox.get_nowait()
            except queue.Empty:
                break
            pending -= 1
            report(name, reply)
    # Out of work: wait for the rest.
    while pending:
        report(name, inbox.get())
        pending -= 1


def report(name: str, reply: object) -> None:
    """Print one reply, or the error a query ended with."""
    with print_lock:
        if isinstance(reply, Exception):
            print("%s: %s" % (name, type(reply).__name__))
        else:
            print("%s: %s" % (name, reply.message.decode()))
        sys.stdout.flush()


def main(argv: list[str]) -> None:
    """Start the workers, wait for them, shut the client down."""
    host, port = (argv[1] if len(argv) > 1 else "127.0.0.1:1980").rsplit(":", 1)
    n_workers = int(argv[2]) if len(argv) > 2 else 3
    n_jobs = int(argv[3]) if len(argv) > 3 else 4
    client = ThreadedClient([(host, int(port))], type=peers.ECHO_CLIENT)
    threads = [
        threading.Thread(target=worker, args=("worker-%d" % w, client, ["w%d job %d" % (w, j) for j in range(n_jobs)]))
        for w in range(n_workers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    client.shutdown()


if __name__ == "__main__":
    main(sys.argv)
