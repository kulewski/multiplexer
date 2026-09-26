"""A model worker: a backend on BaseThreadedMultiplexerServer that loads the
model once and answers every INFER_REQUEST with the digit it reads and the
probabilities.

    python backend.py [ADDRESSES] [--name NAME] [--weights PATH] [--workers N] [--torch-threads N]
                      [--slow SECONDS] [--drain-file PATH]

ADDRESSES is host:port of every multiplexer, comma-separated, default
127.0.0.1:1980. The worker is a BaseThreadedMultiplexerServer: --workers
is how many requests it handles at once, and its io thread heartbeats
while the model runs, so even an inference longer than the multiplexer's
drop interval, a minute and a half, never looks like a dead peer.
--weights is the model to load, the committed weights.pt by default;
--slow adds a sleep per request, for the walkthrough to show what a slow
model does to the system. SIGTERM, or creating --drain-file, asks the
worker to leave the way a deployment's rolling restart does: it tells the
multiplexers to route it nothing new, serves what was already on its way
and exits once they have confirmed, so that a restart costs nobody a
request."""

import argparse
import os
import signal
import socket
import time

import torch
from multiplexer.threaded_server import BaseThreadedMultiplexerServer, Request

from inference_pb2 import InferRequest, InferResponse
from model import WEIGHTS, DigitNet, load, predict
from multiplexer_constants import peers, types


class Classifier(BaseThreadedMultiplexerServer):
    """Answers INFER_REQUEST; everything else is dropped."""

    multiplexer_client_type = peers.INFERENCE

    def __init__(
        self,
        addresses: list[tuple[str, int]],
        weights: str = WEIGHTS,
        name: str | None = None,
        slow: float = 0.0,
        drain_file: str | None = None,
        workers: int = 1,
    ) -> None:
        # The model first: loading is the slow part and the one that can
        # fail. The order is free otherwise: the base class connects and
        # handles only in serve_forever().
        self.model: DigitNet = load(weights)
        self.name = name or f"{socket.gethostname()}:{os.getpid()}"
        self.slow = slow
        self.drain_file = drain_file
        self.asked_to_leave = False  # set by the SIGTERM handler, read by periodic_task()
        self.answered = 0
        super().__init__(addresses, workers=workers)

    def handle_message(self, request: Request) -> None:
        """One image in, one label out; the request id is the message's, the
        answer goes back the way the request came."""
        if request.mxmsg.type != types.INFER_REQUEST:
            request.no_response()
            return
        infer = InferRequest()
        infer.ParseFromString(request.mxmsg.message)
        started = time.monotonic()
        label, probabilities = predict(self.model, infer.image)
        if self.slow:
            time.sleep(self.slow)
        self.answered += 1
        request.reply(
            InferResponse(
                label=label, probabilities=probabilities, worker=self.name, seconds=time.monotonic() - started
            ),
            type=types.INFER_RESPONSE,
        )

    def periodic_task(self) -> None:
        """Leave once asked, by SIGTERM or the drain file; runs every poll."""
        if self.asked_to_leave or (self.drain_file and os.path.exists(self.drain_file)):
            self.start_draining()


def parse_addresses(text: str) -> list[tuple[str, int]]:
    """ "host:port,host:port" as the list the library takes."""
    return [(host, int(port)) for host, port in (item.rsplit(":", 1) for item in text.split(","))]


def main() -> None:
    """Serve until asked to leave."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "addresses", nargs="?", default="127.0.0.1:1980", help="host:port of every multiplexer, comma-separated"
    )
    parser.add_argument("--weights", default=WEIGHTS, help="the model's weights; default the committed weights.pt")
    parser.add_argument("--name", help="how this worker signs its answers; default host:pid")
    parser.add_argument("--workers", type=int, default=1, help="requests handled at once")
    parser.add_argument(
        "--torch-threads", type=int, default=0, help="threads torch may use per inference; 0 for its default"
    )
    parser.add_argument("--slow", type=float, default=0.0, help="seconds to add to every inference")
    parser.add_argument("--drain-file", help="creating this file asks the worker to leave gracefully")
    args = parser.parse_args()
    if args.torch_threads:
        torch.set_num_threads(args.torch_threads)
    worker = Classifier(
        parse_addresses(args.addresses), args.weights, args.name, args.slow, args.drain_file, args.workers
    )
    # SIGTERM, what an orchestrator sends, drains as the drain file does. The
    # handler only sets a flag, which periodic_task() reads within one poll:
    # it may interrupt the main thread inside the library, so it calls none.

    def asked_to_leave(signum: int, frame: object) -> None:
        """SIGTERM's handler: the flag, and nothing else."""
        worker.asked_to_leave = True

    signal.signal(signal.SIGTERM, asked_to_leave)
    # The notebook waits for this line before it sends, so the line must mean
    # reachable: connect() first; serve_forever() would otherwise.
    worker.connect()
    print(f"ready: worker {worker.name}, instance {worker.instance_id}", flush=True)
    worker.serve_forever(poll=0.5, drain_seconds=5)
    print(f"left after {worker.answered} answers", flush=True)


if __name__ == "__main__":
    main()
