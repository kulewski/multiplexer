"""The requester's side of the streaming example: `Streams`, one
subscription to the tokens over an `AsyncClient`, and the `Stream` it
opens, an async iterator of an answer's tokens in order, with what a dead
connection lost asked for again."""

from mxstream.client import Incomplete, Stream, Streams, parse_addresses

__all__ = ["Incomplete", "Stream", "Streams", "parse_addresses"]
