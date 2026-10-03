"""A multiplexer's address, as the clients take it and as text.

Endpoint is the (host, port) every client and server class takes; an IPv6
address in it has no brackets, as socket calls take it. As text an
endpoint is host:port, or [address]:port for an IPv6 address, the form
URLs use, since the address has colons of its own: mxcontrol's --address
and -M, the test roles' --mx, the port file and the log lines write it so.
parse_endpoint() and format_endpoint() are the C++ client library's
(multiplexer/endpoint.h, through the extension), the same rules in both
languages, for a program that reads addresses from its command line or its
configuration.
"""

from multiplexer import _native

__all__ = ["Endpoint", "format_endpoint", "parse_endpoint"]

Endpoint = tuple[str, int]


def parse_endpoint(text: str, default_port: int | None = None) -> Endpoint:
    """`text` as (host, port): host:port, or [address]:port for an IPv6
    address, whose host comes back without the brackets. The host may be
    empty, as in ":1980", for the caller to fill in. Without a port, "host"
    or "[address]", the port is `default_port`, and ValueError when there
    is none. ValueError, saying what is wrong, for anything else: an IPv6
    address out of brackets ("::1:1980" could be either), brackets around
    anything but an IPv6 address, a port that is not a number from 0 to
    65535."""
    return _native.parse_endpoint(text, default_port)


def format_endpoint(endpoint: Endpoint) -> str:
    """`endpoint` as parse_endpoint() reads it: [address]:port for an IPv6
    address, host:port otherwise."""
    return _native.format_endpoint(endpoint[0], endpoint[1])
