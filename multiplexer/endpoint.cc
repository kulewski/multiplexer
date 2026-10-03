// parse_endpoint() and format_endpoint(); see endpoint.h.
#include "multiplexer/endpoint.h"

#include <stdexcept>

namespace multiplexer {
namespace {

// Refuses `text` for `why`.
[[noreturn]] void refuse(const std::string& text, const std::string& why) {
  throw std::invalid_argument("'" + text + "': " + why);
}

// `port`, what follows the colon in `text`, as a port number: digits only,
// so no sign and no space, at most 65535.
std::uint16_t port_number(const std::string& text, const std::string& port) {
  if (port.empty()) {
    refuse(text, "no port after the colon");
  }
  unsigned long value = 0;
  for (const char digit : port) {
    if (digit < '0' || digit > '9') {
      refuse(text, "the port is a number from 0 to 65535");
    }
    value = value * 10 + static_cast<unsigned long>(digit - '0');
    if (value > 65535) {
      refuse(text, "the port is a number from 0 to 65535");
    }
  }
  return static_cast<std::uint16_t>(value);
}

}  // namespace

std::pair<std::string, std::uint16_t> parse_endpoint(const std::string& text,
                                                     std::optional<std::uint16_t> default_port) {
  if (text.empty()) {
    refuse(text, "no address");
  }
  std::string host;
  std::string::size_type colon;  // the one before the port, or text.size() when there is no port
  if (text[0] == '[') {
    const std::string::size_type close = text.find(']');
    if (close == std::string::npos) {
      refuse(text, "no ] after the IPv6 address");
    }
    host = text.substr(1, close - 1);
    if (host.find(':') == std::string::npos || host.find('[') != std::string::npos) {
      refuse(text, "brackets hold an IPv6 address");
    }
    colon = close + 1;
    if (colon < text.size() && text[colon] != ':') {
      refuse(text, "only :port may follow the ]");
    }
  } else {
    if (text.find_first_of("[]") != std::string::npos) {
      refuse(text, "brackets go around an IPv6 address, at the start");
    }
    colon = text.find(':');
    if (colon != std::string::npos && text.find(':', colon + 1) != std::string::npos) {
      refuse(text, "an IPv6 address goes in brackets, as in [::1]:1980");
    }
    if (colon == std::string::npos) {
      colon = text.size();
    }
    host = text.substr(0, colon);
  }
  if (colon == text.size()) {
    if (!default_port) {
      refuse(text, "no port: host:port, or [address]:port for an IPv6 address");
    }
    return std::make_pair(host, *default_port);
  }
  return std::make_pair(host, port_number(text, text.substr(colon + 1)));
}

std::string format_endpoint(const std::string& host, std::uint16_t port) {
  if (host.find(':') != std::string::npos) {
    return "[" + host + "]:" + std::to_string(port);
  }
  return host + ":" + std::to_string(port);
}

}  // namespace multiplexer
