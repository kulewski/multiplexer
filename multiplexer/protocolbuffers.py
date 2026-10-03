"""Read-only aliases of the sender field under the names it had: `from_`,
the property this module gave MultiplexerMessage while the field was named
`from`, a Python keyword, up to 2.3.1, and `from` itself, as
getattr(message, "from") read it, on MultiplexerMessage and on a
recording's RoutedMessage. The field is `sender` now, under the same
number, so that the wire and every recording are unchanged. Each alias
reads `sender` with a DeprecationWarning and refuses to set it; they will
go. Imported for its side effect by mxclient and threaded_client."""

import warnings

from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.Recording_pb2 import RoutedMessage


def _former(name: str) -> property:
    """A read-only property under `name`, a former name of the field, that
    reads `sender` and warns that it is deprecated."""

    def read(self) -> int:
        """The `sender` field, under a name it had."""
        warnings.warn("%s is the field sender now: read .sender" % name, DeprecationWarning, stacklevel=2)
        return self.sender

    return property(read)


setattr(MultiplexerMessage, "from_", _former("from_"))
setattr(MultiplexerMessage, "from", _former("from"))
setattr(RoutedMessage, "from", _former("from"))
