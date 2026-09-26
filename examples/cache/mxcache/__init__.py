"""The client side of the replicated cache: `Cache`, a class over the
threaded client that reads from any replica and writes to every one, and
`mxcache.backend.MultiplexerCache`, Django's cache API on top of it."""

from mxcache.client import Cache, parse_addresses

__all__ = ["Cache", "parse_addresses"]
