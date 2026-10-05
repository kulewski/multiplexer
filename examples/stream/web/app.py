"""The web side: a FastAPI app whose /generate streams an answer as
Server-Sent Events, one event per token as the generator produces it,
and a page that shows the tokens appear. One AsyncClient per process,
from a holder, as the async web server recipe has it; the answer's tokens
reach the socket as they reach the process, with nothing buffered between.

    MX_ADDRESSES=127.0.0.1:1980,127.0.0.1:1981 uvicorn app:app --port 8000
    curl -N 'http://127.0.0.1:8000/generate?prompt=hello&tokens=20'
"""

import json
import os
import sys

# The example's directory, for its constants, payloads and client.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from multiplexer.aio import AsyncClient

from multiplexer_constants import peers
from mxstream import Streams, parse_addresses
from mxstream.client import MAX_TOKENS

ENDPOINTS = parse_addresses(os.environ.get("MX_ADDRESSES", "127.0.0.1:1980"))
MX = AsyncClient.holder(peers.STREAM_CLIENT, lambda: ENDPOINTS)
app = FastAPI(title="stream")
_streams: Streams | None = None


async def streams() -> Streams:
    """The process's Streams over the holder's client, made on first use,
    on a thread, so that the loop never waits for the handshakes."""
    global _streams
    client = await MX.aget()
    if _streams is None or _streams.client is not client:
        _streams = Streams(client)
    return _streams


@app.get("/generate")
async def generate(prompt: str, tokens: int = Query(40, ge=1, le=MAX_TOKENS)) -> StreamingResponse:
    """The answer as Server-Sent Events: `data` per token, `done` with the summary, or `error`."""

    async def events():
        """The answer's tokens as Server-Sent Events, then its summary, or the error that ended it."""
        try:
            # Closed however events() ends, a browser gone at a yield included: the answer abandoned, its
            # generator told to stop.
            async with (await streams()).open(prompt, tokens) as stream:
                async for text in stream:
                    yield f"data: {json.dumps(text)}\n\n"
                yield f"event: done\ndata: {json.dumps(stream.summary())}\n\n"
        except Exception as error:  # no generator, a timeout: the client gets a name rather than a cut
            yield f"event: error\ndata: {json.dumps(type(error).__name__)}\n\n"

    return StreamingResponse(events(), media_type="text/event-stream")


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    """A prompt, and the answer appearing token by token."""
    return PAGE


PAGE = """<!doctype html>
<title>Streaming answers</title>
<style>
  body { font-family: sans-serif; margin: 1em; max-width: 50em; }
  #answer { border: 1px solid #ccc; min-height: 6em; padding: 0.5em; margin: 0.5em 0; line-height: 1.5; }
  #meter { font-family: monospace; white-space: pre; }
</style>
<h1>Streaming answers</h1>
<p><input id="prompt" size="40" value="what is a multiplexer"> tokens <input id="tokens" size="4" value="60">
<button id="ask">Ask</button></p>
<div id="answer"></div>
<div id="meter"></div>
<script>
  const answer = document.querySelector("#answer"), meter = document.querySelector("#meter");
  let source = null;
  document.querySelector("#ask").onclick = () => {
    if (source) source.close();
    answer.textContent = ""; meter.textContent = "asking";
    const started = performance.now();
    let count = 0, first = 0;
    const query = new URLSearchParams({prompt: document.querySelector("#prompt").value, tokens: document.querySelector("#tokens").value});
    source = new EventSource("/generate?" + query);
    source.onmessage = (event) => {
      if (!count) first = performance.now() - started;
      count++;
      answer.textContent += (count > 1 ? " " : "") + JSON.parse(event.data);
      meter.textContent = count + " tokens, the first after " + first.toFixed(0) + " ms";
    };
    source.addEventListener("done", (event) => {
      const s = JSON.parse(event.data);
      meter.textContent = s.tokens + " tokens in " + s.seconds + " s, " + s.tokens_per_second + " tokens/s, by " + s.worker +
        ", the first after " + first.toFixed(0) + " ms" + (s.reattached ? ", asked again through another multiplexer" : "") +
        (s.gaps ? ", " + s.gaps + " gap(s) asked for" : "") + (s.resent ? ", " + s.resent + " tokens sent again" : "");
      source.close();
    });
    source.addEventListener("error", (event) => {
      meter.textContent = "failed: " + (event.data ? JSON.parse(event.data) : "the connection");
      source.close();
    });
  };
</script>
"""
