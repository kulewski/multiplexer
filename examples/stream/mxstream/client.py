"""The requester's side: an answer as an async iterator of its tokens.

`Streams` holds the answers a process has open and one subscription to
TOKEN on its AsyncClient, handing each token to the answer it belongs to
by `stream_id`. `Stream`, what `Streams.open()` returns, sends the request
as a query on a pinned lane, so that the tokens, which the generator
sends back the way the request came, arrive in order while that way
lives. The first token names the answer's generator, and a token from
any other is refused and its sender told to stop. Tokens are yielded in
order, one that came early held back; when the one due is missing, what
a dead connection lost, the generator of the answer is asked, addressed
with `to`, for the tokens missing. When the lane's multiplexer dies, the
request goes again through another, addressed to that generator once a
token has named it, and the generator joins it to the answer under way.
The query's reply ends the answer.
With `pinned=True` an answer whose multiplexer dies ends with
NotConnected instead, for a caller that would rather start over."""

import asyncio
import random
import time
from typing import AsyncIterator

from multiplexer.aio import AsyncClient
from multiplexer.endpoints import parse_endpoint
from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.mxclient import NotConnected, OperationTimedOut

from multiplexer_constants import types
from stream_pb2 import Cancel, Generated, Prompt, Resume, Resumed, Token

MAX_TOKENS = 1000  # the longest answer a generator gives
REORDER_WAIT = 0.05  # seconds a missing token gets, from when a later one showed it missing, before it is asked for
NAME_WAIT = 0.5  # seconds a request whose multiplexer died waits for a token naming its generator, then any takes it
STALL = 5.0  # seconds without a token, once the generator is known, before it is asked whether it is still there
RESUME_TIMEOUT = 10.0
MARGIN = 5.0  # the library's own timeout of the request is the answer's and this: the answer's deadline comes first


class Incomplete(Exception):
    """The generator has no more of an answer that is missing tokens."""


def parse_addresses(text: str) -> list[tuple[str, int]]:
    """ "host:port,host:port", an IPv6 address as [address]:port, as the list the library takes."""
    return [parse_endpoint(item) for item in text.split(",")]


class Streams:
    """The answers a process has open, fed by one subscription to TOKEN."""

    def __init__(self, client: AsyncClient, stall: float = STALL):
        self.client = client
        self.stall = stall
        self._open: dict[int, "Stream"] = {}
        self._cancelled: set[tuple[int, int]] = set()  # (stream_id, generator) told to stop, so that it is told once
        self._cancelling: set[asyncio.Task] = set()  # the CANCELs on their way: the loop keeps only weak references
        self._unsubscribe = client.subscribe(types.TOKEN, self._on_token)

    def open(self, prompt: str, tokens: int = 40, *, timeout: float = 60.0, pinned: bool = False) -> "Stream":
        """Ask, and get the answer's tokens as they come:
        `async with streams.open(...) as stream: async for text in stream`.
        `timeout` bounds the whole answer; `pinned` ends it with
        NotConnected when its multiplexer dies, rather than going on
        through another."""
        stream = Stream(self, prompt, tokens, timeout, pinned)
        self._open[stream.stream_id] = stream
        return stream

    def _on_token(self, mxmsg: MultiplexerMessage) -> None:
        """On the loop, for every TOKEN: to the answer it belongs to, while
        it is open; the generator of one nobody reads any more is told to stop."""
        token = Token()
        token.ParseFromString(mxmsg.message)
        stream = self._open.get(token.stream_id)
        if stream is not None:
            stream._arrived(token, mxmsg.sender)
        else:
            self._cancel(token.stream_id, mxmsg.sender)

    def _cancel(self, stream_id: int, generator: int) -> None:
        """Tell `generator` to stop the answer, once; an event, not awaited by anyone."""
        if (stream_id, generator) in self._cancelled:
            return
        if len(self._cancelled) > 10000:
            self._cancelled.clear()  # a bound; a generator told twice is no harm
        self._cancelled.add((stream_id, generator))
        task = asyncio.ensure_future(self._send_cancel(stream_id, generator))
        self._cancelling.add(task)
        task.add_done_callback(self._cancelling.discard)

    async def _send_cancel(self, stream_id: int, generator: int) -> None:
        """The CANCEL itself, held a second while no connection is live, then
        given up on: the generator then finishes the answer to nobody."""
        try:
            await self.client.send_message(Cancel(stream_id=stream_id), type=types.CANCEL, to=generator, timeout=1.0)
        except Exception:  # the client closed meanwhile; the generator finishes likewise
            pass

    def _forget(self, stream: "Stream") -> None:
        """A stream that ended: its tokens are not taken any more."""
        self._open.pop(stream.stream_id, None)

    def close(self) -> None:
        """End the subscription; the client is the caller's."""
        self._unsubscribe()


class Stream:
    """One answer: its tokens in order, the count of what was lost and
    asked for again, and the reply's summary once it ended. An async
    context manager: leaving it, or aclose(), before the end abandons the
    answer: its generator told to stop, its reply no longer waited for."""

    def __init__(self, streams: Streams, prompt: str, tokens: int, timeout: float, pinned: bool):
        self.streams = streams
        self.stream_id = random.getrandbits(63)  # the requester's own number for the answer
        self.prompt = Prompt(stream_id=self.stream_id, prompt=prompt, tokens=tokens)
        self.pinned = pinned
        self.lane = streams.client.lane(pinned=True)  # the request's connection, which the tokens come back on
        self.worker = 0  # the answer's generator, from its first token
        self.next_seq = 1  # the token due
        self.received = 0
        self.gaps = 0  # times a missing token was asked for
        self.resent = 0  # tokens the generator sent again for this requester's RESUMEs
        self.reattached = 0  # times the request went again, its multiplexer dead
        self.drop = 0  # a test's knob: the next tokens to drop on arrival, as a dead connection would
        self.answer: Generated | None = None  # the reply, once the answer ended
        self.started = time.monotonic()
        self.deadline = self.started + timeout
        self.seconds = 0.0
        self._early: dict[int, Token] = {}  # arrived before their turn, by number
        self._queue: asyncio.Queue[Token] = asyncio.Queue()
        self._last_seen = False
        self._missing_since: float | None = None  # when the token due was first seen missing
        self._named = asyncio.Event()  # set once a token or the reply names the answer's generator
        self._resent_after_end = 0  # tokens sent again for a RESUME the generator served after the answer ended
        self._task = asyncio.ensure_future(self._ask(timeout))

    async def _ask(self, timeout: float) -> Generated:
        """The request, and the reply that ends the answer. When the lane's
        multiplexer dies under it, the request goes again through another,
        with the number of the token due, addressed to the answer's
        generator, which joins it to the answer and sends the answer on
        from that token. With no token yet to name the generator, the
        request waits NAME_WAIT for one; with none, it goes to any
        generator, and follows the generator a token names after all. A
        reply from any generator but the answer's, one told to stop, sends
        the request to the answer's own."""
        prompt = Prompt()
        prompt.CopyFrom(self.prompt)
        client = self.streams.client
        while True:
            target = self.worker
            query = asyncio.ensure_future(
                client.query(prompt, types.GENERATE, timeout=timeout + MARGIN, to=target, multiplexer=self.lane)
            )
            waiting = {query}
            if not target and prompt.from_seq:  # sent again to any generator: it follows the one a token names
                waiting.add(asyncio.ensure_future(self._named.wait()))
            try:
                done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
            except asyncio.CancelledError:
                for task in waiting:
                    task.cancel()
                raise
            for task in waiting - done:
                task.cancel()  # a generator the request left, if it starts, is told to stop by its first token
            if query not in done:
                prompt.from_seq = self.next_seq
                continue
            try:
                reply = query.result()
            except NotConnected:
                if self.pinned or time.monotonic() >= self.deadline:
                    raise
                await asyncio.sleep(REORDER_WAIT)  # a moment for the library to see which connections live
                self.lane = client.lane(pinned=True)
                self.reattached += 1
                prompt.from_seq = self.next_seq
                if not self.worker:
                    try:
                        await asyncio.wait_for(self._named.wait(), NAME_WAIT)
                    except asyncio.TimeoutError:
                        pass  # nobody named: the request may have died with the multiplexer
                continue
            generated = Generated()
            generated.ParseFromString(reply.message)
            if self.worker and reply.sender != self.worker:
                prompt.from_seq = self.next_seq
                continue
            if not self.worker:
                self.worker = reply.sender
                self._named.set()
            return generated

    def _arrived(self, token: Token, sender: int) -> None:
        """On the loop: a token, in whatever order it came; the first names
        the answer's generator, and another's is refused."""
        if self.drop:
            self.drop -= 1
            return
        if not self.worker:
            self.worker = sender
            self._named.set()
        elif sender != self.worker:
            self.streams._cancel(self.stream_id, sender)
            return
        self._queue.put_nowait(token)

    def __aiter__(self) -> AsyncIterator[str]:
        """The stream itself: `async for text in stream`."""
        return self

    async def __anext__(self) -> str:
        """The next token in order, or StopAsyncIteration after the last;
        an answer that fails is abandoned: its generator told to stop, and
        the wait for its reply given up, though the library keeps the
        request itself until its timeout."""
        try:
            return await self._next()
        except StopAsyncIteration:
            raise
        except BaseException:
            self._abandon()
            raise

    async def __aenter__(self) -> "Stream":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Abandon the answer, unless it ended: a consumer that stops early closes its stream."""
        self._abandon()

    async def _next(self) -> str:
        """The token due, once it arrived. One missing while a later one, or
        the reply, is in gets REORDER_WAIT to come the other way, from when
        that was seen, and is then asked for. Nothing at all for `stall`,
        once the generator is known, is a question to it; before the first
        token nothing is asked, since the request may wait in a busy
        generator's queue. The whole answer has its deadline."""
        while True:
            token = self._early.pop(self.next_seq, None)
            if token is not None:
                self.next_seq += 1
                self.received += 1
                self._missing_since = None
                if token.last:
                    self._last_seen = True
                return token.text
            if self._last_seen:
                await self._finish()
                raise StopAsyncIteration
            if self._task.done() and self._task.exception() is not None:
                error = self._task.exception()
                assert error is not None
                raise error  # no generator, a pinned lane's connection gone
            now = time.monotonic()
            if now >= self.deadline:
                raise OperationTimedOut(f"the answer took longer than its {self.deadline - self.started:.0f} s")
            if self._early or self._task.done():
                if self._missing_since is None:
                    self._missing_since = now
                wait = self._missing_since + REORDER_WAIT - now
                if wait > 0:
                    await self._next_arrival(min(wait, self.deadline - now))
                else:
                    self.gaps += 1
                    self._missing_since = None
                    await self._resume(min(self._early, default=0))
            elif not self.worker:
                await self._next_arrival(self.deadline - now)
            elif not await self._next_arrival(min(self.streams.stall, self.deadline - now)):
                # A stall: what came since, a gap when it is anything, or
                # OperationFailed when its generator is gone.
                if time.monotonic() < self.deadline and await self._resume(0):
                    self.gaps += 1

    def _abandon(self) -> None:
        """Forget the answer, stop waiting for its reply and tell its
        generator to stop; nothing, once the answer ended. The library
        keeps the request until its timeout, and a generator it reaches
        after that is told to stop by its first token."""
        if self.answer is not None:
            return
        self.streams._forget(self)
        if not self._task.done():
            self._task.cancel()
        if self.worker:
            self.streams._cancel(self.stream_id, self.worker)

    async def _next_arrival(self, timeout: float | None) -> bool:
        """Wait for a token, or for the reply while it is not in, at most
        `timeout`; then keep every queued token. Whether anything came."""
        get = asyncio.ensure_future(self._queue.get())
        waiting = {get} if self._task.done() else {get, self._task}
        try:
            done, _ = await asyncio.wait(waiting, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        finally:
            if not get.done():
                get.cancel()
        if get.done() and not get.cancelled():
            self._keep(get.result())
        self._keep_queued()
        return bool(done)

    def _keep(self, token: Token) -> None:
        """A token for its turn; one sent again and seen already is dropped."""
        if token.seq >= self.next_seq:
            self._early[token.seq] = token

    def _keep_queued(self) -> None:
        """Every token that has arrived, each for its turn."""
        while not self._queue.empty():
            self._keep(self._queue.get_nowait())

    async def _resume(self, until: int) -> int:
        """Ask the generator of this answer, addressed, for the tokens from
        the one due up to `until`, the first one here already, or on to the
        last it has; they come through the subscription ahead of the reply,
        so they are all queued once it is in. How many came again. A
        generator that is gone makes this OperationFailed at once, as any
        addressed query; the question waits no longer than the answer may."""
        resume = Resume(stream_id=self.stream_id, from_seq=self.next_seq, until_seq=until)
        timeout = max(0.01, min(RESUME_TIMEOUT, self.deadline - time.monotonic()))
        reply = await self.streams.client.query(resume, types.RESUME, timeout=timeout, to=self.worker)
        resumed = Resumed()
        resumed.ParseFromString(reply.message)
        self.resent += resumed.resent
        if resumed.done:
            self._resent_after_end += resumed.resent  # after the reply's count was made
        self._keep_queued()
        if resumed.resent == 0 and resumed.done and self.next_seq not in self._early:
            raise Incomplete(f"the generator has nothing from token {self.next_seq} on")
        return resumed.resent

    async def _finish(self) -> None:
        """The reply, once the last token was yielded; it may be in already,
        and it comes within the answer's deadline or not at all."""
        try:
            self.answer = await asyncio.wait_for(asyncio.shield(self._task), max(0.0, self.deadline - time.monotonic()))
        except asyncio.TimeoutError:
            raise OperationTimedOut(f"the answer took longer than its {self.deadline - self.started:.0f} s") from None
        self.seconds = time.monotonic() - self.started
        self.streams._forget(self)

    @property
    def tokens_per_second(self) -> float:
        """The answer's tokens over its seconds; 0 until its reply came."""
        return self.received / self.seconds if self.seconds else 0.0

    def summary(self) -> dict[str, str | int | float]:
        """What the answer cost, for a meter or a log line."""
        return {
            "tokens": self.received,
            "seconds": round(self.seconds, 3),
            "tokens_per_second": round(self.tokens_per_second, 1),
            "worker": self.answer.worker if self.answer else "",
            "gaps": self.gaps,
            # The reply's count takes in the joins too, but not a RESUME served after the answer ended.
            "resent": self.answer.resent + self._resent_after_end if self.answer else self.resent,
            "reattached": self.reattached,
        }
