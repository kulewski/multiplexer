// A multiplexer's address as text: host:port, or [address]:port for an IPv6
// address, the form URLs use, since the address has colons of its own.
// parse_endpoint() reads it and format_endpoint() writes it, for every
// command line that takes an address (mxcontrol's --address and -M, the
// test roles' --mx), the port file and the log lines; the Python package
// has the same two functions (multiplexer/endpoints.py) through the
// extension. The clients' connect() takes the host as parse_endpoint()
// returns it, an IPv6 address without brackets.
//
// An IPv6 address out of brackets is refused rather than split at its last
// colon: "::1:1980" could be ::1 port 1980 or ::1:1980 with no port, and
// "::1" alone, where a port may be left out, would read as host ":" port 1.
#ifndef MX_MULTIPLEXER_ENDPOINT_H_
#define MX_MULTIPLEXER_ENDPOINT_H_

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

namespace multiplexer {

// `text` as (host, port): host:port, or [address]:port for an IPv6 address,
// whose host comes back without the brackets. The host may be empty, as in
// ":1980", for the caller to fill in. Without a port, "host" or
// "[address]", the port is `default_port`, and the text is refused when
// there is none. Refused with std::invalid_argument, whose what() names the
// text and says what is wrong: an IPv6 address out of brackets, brackets
// around anything but an IPv6 address or anywhere but at the start, a port
// that is not a number from 0 to 65535 (a sign included), empty text.
std::pair<std::string, std::uint16_t> parse_endpoint(const std::string& text,
                                                     std::optional<std::uint16_t> default_port = std::nullopt);

// `host` and `port` as parse_endpoint() reads them: [host]:port when the
// host has a colon, an IPv6 address, host:port otherwise.
std::string format_endpoint(const std::string& host, std::uint16_t port);

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_ENDPOINT_H_
