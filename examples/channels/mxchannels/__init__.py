"""A Django Channels channel layer on the multiplexer: `MultiplexerChannelLayer`."""

from mxchannels.layer import MultiplexerChannelLayer, parse_addresses

__all__ = ["MultiplexerChannelLayer", "parse_addresses"]
