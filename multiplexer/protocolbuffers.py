"""Adds MultiplexerMessage.from_ as an alias of the `from` field, which is a
Python keyword and otherwise reachable only through getattr(). Imported for
its side effect by mxclient."""

from multiplexer.Multiplexer_pb2 import MultiplexerMessage


def _get_from(self):
    """The `from` field."""
    return getattr(self, "from")


def _set_from(self, value):
    """Set the `from` field."""
    return setattr(self, "from", value)


def _del_from(self):
    """Clear the `from` field."""
    delattr(self, "from")


MultiplexerMessage.from_ = property(_get_from, _set_from, _del_from)
