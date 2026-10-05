"""A worker whose health check watches its serve loop: a backend on
`BaseMultiplexerServer` that answers `WORK`, and an HTTP endpoint on a
thread of its own, `GET /healthz`, for a Kubernetes liveness probe.

    python backend.py [ADDRESSES] [--name NAME] [--health-port PORT]
                      [--stale-seconds S] [--stall-seconds S] [--drain-file PATH]

ADDRESSES is host:port of every multiplexer, comma-separated, default
127.0.0.1:1980. serve_forever() calls periodic_task() after every message
and after every poll that found none, and the worker records the time
there. The endpoint answers 200 while that time is recent, and 503 once
the loop has not come round for --stale-seconds: a thread of its own
answers whatever the loop does, so the time is the only thing it can know
the loop by. --stall-seconds dumps every thread's stack to stderr when one
message's handling takes longer, so that the log says where the loop
stopped before the probe has the worker restarted. Creating --drain-file
asks the worker to leave, as the manifest's preStop hook does; the worker
removes the file once it has left, which is what the hook waits for."""

import argparse
import contextlib
import http.server
import os
import socket
import threading
import time

from multiplexer.endpoints import parse_endpoint
from multiplexer.servers import BaseMultiplexerServer

from multiplexer_constants import peers, types

POLL = 1.0  # seconds serve_forever() waits for a message before it calls periodic_task() all the same
DRAIN_SECONDS = 20.0  # the most a drain takes; the manifest's grace period of 30 s leaves room for the close
DRAIN_CHECK_EVERY = 0.5  # seconds between looks for the drain file
HEALTH_PATH = "/healthz"


class Worker(BaseMultiplexerServer):
    """Answers every WORK request, and records when its loop last came round."""

    multiplexer_client_type = peers.WORKER

    def __init__(self, addresses: list[tuple[str, int]], name: str | None = None, drain_file: str | None = None):
        super().__init__(addresses)
        self.name = name or f"{socket.gethostname()}:{os.getpid()}"
        self.drain_file = drain_file
        self.served = 0
        # time.monotonic() at the loop's last turn, written by the loop and
        # read by the health endpoint's thread; a float is replaced whole.
        self.loop_alive_at = time.monotonic()
        self._drain_checked_at = 0.0

    def handle_message(self, mxmsg) -> None:
        """Do the work a request asks for and say so; anything else needs no answer."""
        if mxmsg.type != types.WORK:
            self.no_response()
            return
        seconds = float(mxmsg.message)
        self.work(seconds)
        self.served += 1
        self.send_message(message=f"worked {seconds:g} s, by {self.name}", type=types.WORK_DONE)

    def work(self, seconds: float) -> None:
        """The work, a stand-in that takes as long as the request says; a
        real handler calls a database or another service here, and that is
        where one hangs."""
        time.sleep(seconds)

    def periodic_task(self) -> None:
        """After every message and every poll: record that the loop came
        round, then leave once the drain file exists, looked for twice a
        second rather than after every message."""
        super().periodic_task()
        now = time.monotonic()
        self.loop_alive_at = now
        if self.drain_file and now - self._drain_checked_at >= DRAIN_CHECK_EVERY:
            self._drain_checked_at = now
            if os.path.exists(self.drain_file):
                self.start_draining()


class HealthEndpoint:
    """`GET /healthz` on a thread of its own: 200 while the worker's loop
    came round within `stale_seconds`, 503 once it has not. Each request
    gets a thread of its own too, so that a connection that never sends
    its request holds up no probe."""

    def __init__(self, worker: Worker, port: int, stale_seconds: float):
        self.worker = worker
        self.stale_seconds = stale_seconds
        verdict = self.verdict

        class Handler(http.server.BaseHTTPRequestHandler):
            """One request: the verdict on /healthz, 404 elsewhere."""

            timeout = 10  # seconds a connection may take to send its request

            def do_GET(self) -> None:
                """The verdict as the status, and why as the text."""
                if self.path != HEALTH_PATH:
                    self.send_error(404)
                    return
                status, text = verdict()
                body = (text + "\n").encode()
                self.send_response(status)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                """Nothing: a probe every few seconds is not worth a line each."""

        # On every address, the pod's among them, which the kubelet probes.
        self.server = http.server.ThreadingHTTPServer(("", port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, name="health", daemon=True)

    @property
    def port(self) -> int:
        """The port it listens on, the one the system picked for 0."""
        return self.server.server_address[1]

    def verdict(self) -> tuple[int, str]:
        """The status for the probe, and the text that says why."""
        since = time.monotonic() - self.worker.loop_alive_at
        if since > self.stale_seconds:
            return 503, f"stale: the loop has not come round for {since:.1f} s, more than {self.stale_seconds:g} s"
        return 200, f"ok: the loop came round {since:.1f} s ago"

    def start(self) -> "HealthEndpoint":
        """Serve on the endpoint's thread, a daemon one, which ends with the process."""
        self.thread.start()
        return self

    def close(self) -> None:
        """Stop serving and close the port, for a program that goes on without it, a test."""
        self.server.shutdown()
        self.server.server_close()


def main() -> None:
    """Serve until asked to leave, the endpoint beside the loop."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "addresses", nargs="?", default="127.0.0.1:1980", help="host:port of every multiplexer, comma-separated"
    )
    parser.add_argument("--name", help="how this worker signs its answers; default host:pid")
    parser.add_argument("--health-port", type=int, default=8080, help="the port of GET /healthz; 0 picks a free one")
    parser.add_argument(
        "--stale-seconds", type=float, default=30.0, help="seconds without the loop coming round before a 503"
    )
    parser.add_argument(
        "--stall-seconds", type=float, default=20.0, help="a message handled for longer dumps every stack; 0 for never"
    )
    parser.add_argument("--drain-file", help="creating this file asks the worker to leave; it is removed once it has")
    args = parser.parse_args()
    worker = Worker([parse_endpoint(text) for text in args.addresses.split(",")], args.name, args.drain_file)
    health = HealthEndpoint(worker, args.health_port, args.stale_seconds).start()
    # For whoever runs the steps, and the test reads the port; it does not mean connected: serve_forever() connects.
    print(f"ready: worker {worker.name}, instance {worker.conn.instance_id}, health on port {health.port}", flush=True)
    worker.serve_forever(poll=POLL, drain_seconds=DRAIN_SECONDS, stall_seconds=args.stall_seconds)
    # Left, the connections closed: the preStop hook that wrote the file waits for it to go.
    if args.drain_file:
        with contextlib.suppress(FileNotFoundError):
            os.remove(args.drain_file)
    print(f"left after {worker.served} requests", flush=True)


if __name__ == "__main__":
    main()
