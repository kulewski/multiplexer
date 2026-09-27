"""A generator: a worker that answers a prompt token by token, the way a
language model serves an answer, without the model. The answer is words
of a fixed text, from a place the prompt picks, at a steady rate; the
point is the transport: one request, its tokens as follow-ups addressed
to the requester while the request waits, one reply at the end, and a
requester that asks again for what a dead connection lost.

    python generator.py [ADDRESSES] [--name NAME] [--tokens-per-second N] [--answers N]

ADDRESSES is host:port of every multiplexer, comma-separated, default
127.0.0.1:1980. The generator is a BaseThreadedMultiplexerServer whose
workers only take the messages in: every answer runs on a thread of its
own, from a pool of `--answers`, so that a request joining an answer, a
resend or a cancel is served at once however many answers are under
way. SIGTERM or Ctrl-C asks it to leave: the multiplexers route it no
new prompt, the answers under way end, and then it closes."""

import argparse
import logging
import os
import signal
import socket
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

from multiplexer.threaded_server import BaseThreadedMultiplexerServer, Request

from multiplexer_constants import peers, types
from mxstream.client import MAX_TOKENS, parse_addresses
from stream_pb2 import Cancel, Generated, Prompt, Resume, Resumed, Token

# The text the answers are made of: a prompt picks where to start reading.
TEXT = """The multiplexer is a message broker for a datacenter, one binary, that
routes typed messages between the programs connected to it by the rules in
one file. A request goes to any one backend of its type and comes back
answered; an event goes to every peer of its type; a message addressed to
an instance goes there and nowhere else. Several multiplexers run side by
side and every peer connects to all of them, so a multiplexer may die and
nothing is lost but the messages it held. A backend leaves by telling the
multiplexers to route it nothing new, serving what was on its way, and
closing; a client finds a live backend on its own. Nothing is persisted and
nothing is queued beyond a socket, which is what makes it simple to run
and fast enough to carry a stream of ten millisecond frames."""
WORDS = TEXT.split()
FORGET_AFTER = 60.0  # seconds a finished answer's tokens are kept, for a late resend
LEAVE_SECONDS = 60.0  # the longest a leave waits for the answers under way
LINGER = 1.0  # seconds a leaving generator stays after its last answer ended, for a late request for its tail

log = logging.getLogger("generator")
leaving = threading.Event()  # set by SIGTERM and SIGINT


def answer_words(prompt: str, count: int) -> list[str]:
    """The answer to `prompt`, `count` words from the text, read from the
    place the prompt's checksum picks and wrapping round; deterministic, so
    that a test knows what to expect."""
    start = zlib.crc32(prompt.encode()) % len(WORDS)
    return [WORDS[(start + index) % len(WORDS)] for index in range(count)]


class Answer:
    """One answer: its tokens so far, the requests that asked for it, and
    whether it ended. More than one request asks for the same answer when
    the requester's connection to this generator died under it: it sends
    the request again, addressed here, with the same stream_id, and the
    answer goes on the new request's way."""

    def __init__(self, request: Request, prompt: Prompt):
        self.prompt = prompt
        self.requester = request.mxmsg.from_
        self.requests = [request]
        self.tokens: list[Token] = []
        self.done = False  # ended: whole, cancelled or failed
        self.cancelled = False
        self.failed = ""  # what went wrong, for an answer that failed
        self.resent = 0  # tokens sent again, to a request that joined or for a RESUME
        self.finished_at = 0.0
        self.seconds = 0.0


class Generator(BaseThreadedMultiplexerServer):
    """Answers GENERATE token by token, RESUME with tokens sent again, CANCEL by stopping."""

    multiplexer_client_type = peers.GENERATOR

    def __init__(
        self,
        addresses: list[tuple[str, int]],
        name: str | None = None,
        tokens_per_second: float = 20.0,
        answers: int = 8,
    ):
        super().__init__(addresses, workers=2)  # the workers only take messages in; the answers have their own threads
        self.name = name or f"{socket.gethostname()}:{os.getpid()}"
        self.tokens_per_second = tokens_per_second
        self.answers: dict[int, Answer] = {}
        self.lock = threading.Lock()  # the workers and the answers' threads share the answers
        self.pool = ThreadPoolExecutor(answers, thread_name_prefix="answer")
        self.started = 0  # answers started here, the joins not counted
        self.answered = 0  # answers that ended whole
        self.leave_started = 0.0

    def handle_message(self, request: Request) -> None:
        """A prompt, a resend or a cancel; anything else dropped."""
        kind = request.mxmsg.type
        if kind == types.GENERATE:
            self._generate(request)
        elif kind == types.RESUME:
            self._resume(request)
        elif kind == types.CANCEL:
            self._cancel(request)
        else:
            request.no_response()

    def _generate(self, request: Request) -> None:
        """A new answer, started on a thread of the pool. Or the requester's
        request again, for an answer under way here, after its connection
        died: it joins the answer, the tokens from `from_seq` on go again,
        and the rest, and the reply, go its way."""
        prompt = request.parse_message(Prompt)
        with self.lock:
            answer = self.answers.get(prompt.stream_id)
            if answer is None:
                answer = self.answers[prompt.stream_id] = Answer(request, prompt)
                self.started += 1
                again, done = None, False
            else:
                done = answer.done
                if not done:
                    answer.requests.append(request)
                again = [token for token in answer.tokens if token.seq >= prompt.from_seq] if prompt.from_seq else []
                answer.resent += len(again)
        if again is None:
            self.pool.submit(self._answer, answer)
            return
        for token in again:
            self._send_token(request, token)
        if done and answer.failed:
            request.report_error(answer.failed)
        elif done:
            request.reply(self._generated(answer), type=types.GENERATED)

    def _answer(self, answer: Answer) -> None:
        """On a thread of the pool: the tokens, each to the latest request,
        then the reply. A cancel stops it at the next token; an error, as a
        real model's call can raise, ends it with BACKEND_ERROR to the
        requester. Either way the answer is never left open."""
        prompt = answer.prompt
        count = max(1, min(prompt.tokens or 40, MAX_TOKENS))
        started = time.monotonic()
        try:
            for seq, text in enumerate(answer_words(prompt.prompt, count), start=1):
                token = Token(stream_id=prompt.stream_id, seq=seq, text=text, last=seq == count)
                with self.lock:
                    if answer.cancelled:
                        break
                    answer.tokens.append(token)
                    latest = answer.requests[-1]
                self._send_token(latest, token)
                time.sleep(1.0 / self.tokens_per_second)
        except Exception as error:
            log.exception("answer %d failed", prompt.stream_id)
            self._end(answer, started, lambda request: request.report_error(repr(error)), failed=repr(error))
            return
        self._end(answer, started, lambda request: request.reply(self._generated(answer), type=types.GENERATED))

    def _end(self, answer: Answer, started: float, reply: Callable[[Request], object], failed: str = "") -> None:
        """The answer marked ended, whole unless cancelled or `failed`, its
        latest request answered with `reply`, the ones that request
        superseded declared done: one reply per answer."""
        with self.lock:
            answer.done = True
            answer.failed = failed
            answer.finished_at = time.time()
            answer.seconds = time.monotonic() - started
            if not answer.cancelled and not failed:
                self.answered += 1
            requests = list(answer.requests)
        for earlier in requests[:-1]:
            earlier.no_response()
        try:
            reply(requests[-1])
        except Exception:  # closed under a leave that ran out of time: nobody to tell
            requests[-1].no_response()
            log.warning("the reply to answer %d could not be sent", answer.prompt.stream_id)

    def _send_token(self, request: Request, token: Token) -> None:
        """A follow-up, not a reply: addressed to the requester, referencing
        nothing, on the connection the request came through, so that the
        tokens arrive in order, and on another when that one is gone."""
        self.send_message(token, type=types.TOKEN, to=request.mxmsg.from_, multiplexer=request.connection)

    def _generated(self, answer: Answer) -> Generated:
        """The reply: how long the answer was, how long it took, who made it, whether it was stopped."""
        return Generated(
            stream_id=answer.prompt.stream_id,
            tokens=len(answer.tokens),
            seconds=answer.seconds,
            worker=self.name,
            cancelled=answer.cancelled,
            resent=answer.resent,
        )

    def _resume(self, request: Request) -> None:
        """The tokens asked for, sent again to whoever asks, then the reply
        saying how many; an unknown stream gets none and `done`."""
        resume = request.parse_message(Resume)
        until = resume.until_seq or MAX_TOKENS + 1
        with self.lock:
            answer = self.answers.get(resume.stream_id)
            tokens = [token for token in answer.tokens if resume.from_seq <= token.seq < until] if answer else []
            done = answer.done if answer else True
            if answer is not None:
                answer.resent += len(tokens)
        for token in tokens:
            self._send_token(request, token)
        request.reply(Resumed(stream_id=resume.stream_id, resent=len(tokens), done=done), type=types.RESUMED)

    def _cancel(self, request: Request) -> None:
        """Stop an answer at its next token, when its own requester asks."""
        cancel = request.parse_message(Cancel)
        with self.lock:
            answer = self.answers.get(cancel.stream_id)
            if answer is not None and answer.requester == request.mxmsg.from_:
                answer.cancelled = True
        request.no_response()

    def periodic_task(self) -> None:
        """Every poll, on serve_forever()'s thread: start leaving when a
        signal asked, and forget answers finished a minute ago."""
        now = time.time()
        with self.lock:
            under_way = sum(not answer.done for answer in self.answers.values())
            old = [
                key for key, answer in self.answers.items() if answer.done and now - answer.finished_at > FORGET_AFTER
            ]
            for key in old:
                del self.answers[key]
        if leaving.is_set() and not self.draining:
            print(f"leaving, answers under way: {under_way}", flush=True)
            self.leave_started = time.monotonic()
            self.start_draining()

    def drained(self) -> bool:
        """The library's drain, confirmed by the multiplexers, and every
        answer under way ended LINGER ago, or LEAVE_SECONDS gone: a leaving
        generator finishes what it started, and stays a moment for a
        request for an answer's tail that a dead connection lost."""
        if not super().drained():
            return False
        if time.monotonic() - self.leave_started >= LEAVE_SECONDS:
            return True
        with self.lock:
            under_way = any(not answer.done for answer in self.answers.values())
            ended = max((answer.finished_at for answer in self.answers.values() if answer.done), default=0.0)
        return not under_way and time.time() - ended >= LINGER

    def close(self) -> None:
        """The library's close; an answer still under way ends at its next token, which cannot be sent."""
        super().close()
        self.pool.shutdown(wait=False, cancel_futures=True)


def main() -> None:
    """Answer until asked to leave."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "addresses", nargs="?", default="127.0.0.1:1980", help="host:port of every multiplexer, comma-separated"
    )
    parser.add_argument("--name", help="how this generator signs its answers; default host:pid")
    parser.add_argument("--tokens-per-second", type=float, default=20.0, help="the rate of an answer")
    parser.add_argument("--answers", type=int, default=8, help="answers under way at once")
    args = parser.parse_args()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: leaving.set())
    generator = Generator(parse_addresses(args.addresses), args.name, args.tokens_per_second, args.answers)
    # The line is for whoever runs the steps; nothing waits for it, and serve_forever() connects.
    print(f"ready: generator {generator.name}, instance {generator.instance_id}", flush=True)
    generator.serve_forever(poll=0.2, drain_seconds=LEAVE_SECONDS)
    print(f"left, answers finished: {generator.answered}", flush=True)


if __name__ == "__main__":
    main()
