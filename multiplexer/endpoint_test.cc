// parse_endpoint() and format_endpoint(): every form an address is
// accepted in, every way one is refused with what() saying why, and the
// two functions agreeing, a formatted endpoint parsing back as it was.
#include "multiplexer/endpoint.h"

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace {

using multiplexer::format_endpoint;
using multiplexer::parse_endpoint;
using Endpoint = std::pair<std::string, std::uint16_t>;

// What parse_endpoint() refuses `text` with, without a default port unless given.
std::string refusal(const std::string& text, std::optional<std::uint16_t> default_port = std::nullopt) {
  try {
    parse_endpoint(text, default_port);
  } catch (const std::invalid_argument& error) {
    return error.what();
  }
  return "accepted";
}

TEST(Endpoint, HostAndPort) {
  EXPECT_EQ(Endpoint("127.0.0.1", 1980), parse_endpoint("127.0.0.1:1980"));
  EXPECT_EQ(Endpoint("mx.example.com", 1980), parse_endpoint("mx.example.com:1980"));
  EXPECT_EQ(Endpoint("0.0.0.0", 0), parse_endpoint("0.0.0.0:0"));
  EXPECT_EQ(Endpoint("host", 65535), parse_endpoint("host:65535"));
  EXPECT_EQ(Endpoint("host", 80), parse_endpoint("host:0080"));
}

TEST(Endpoint, EmptyHostIsTheCallers) { EXPECT_EQ(Endpoint("", 1980), parse_endpoint(":1980")); }

TEST(Endpoint, IPv6InBrackets) {
  EXPECT_EQ(Endpoint("::1", 1980), parse_endpoint("[::1]:1980"));
  EXPECT_EQ(Endpoint("::", 0), parse_endpoint("[::]:0"));
  EXPECT_EQ(Endpoint("fe80::1%eth0", 1980), parse_endpoint("[fe80::1%eth0]:1980"));
  EXPECT_EQ(Endpoint("::ffff:127.0.0.1", 1980), parse_endpoint("[::ffff:127.0.0.1]:1980"));
}

TEST(Endpoint, DefaultPort) {
  EXPECT_EQ(Endpoint("0.0.0.0", 1980), parse_endpoint("0.0.0.0", 1980));
  EXPECT_EQ(Endpoint("::", 1980), parse_endpoint("[::]", 1980));
  EXPECT_EQ(Endpoint("host", 7), parse_endpoint("host:7", 1980));
  EXPECT_EQ("'0.0.0.0': no port: host:port, or [address]:port for an IPv6 address", refusal("0.0.0.0"));
  EXPECT_EQ("'[::1]': no port: host:port, or [address]:port for an IPv6 address", refusal("[::1]"));
}

TEST(Endpoint, IPv6OutOfBracketsIsRefused) {
  EXPECT_EQ("'::1:1980': an IPv6 address goes in brackets, as in [::1]:1980", refusal("::1:1980"));
  EXPECT_EQ("'::1': an IPv6 address goes in brackets, as in [::1]:1980", refusal("::1", 1980));
  EXPECT_EQ("'fe80::1': an IPv6 address goes in brackets, as in [::1]:1980", refusal("fe80::1", 1980));
}

TEST(Endpoint, BracketsHoldAnIPv6AddressAtTheStart) {
  EXPECT_EQ("'[::1': no ] after the IPv6 address", refusal("[::1"));
  EXPECT_EQ("'[::1]1980': only :port may follow the ]", refusal("[::1]1980"));
  EXPECT_EQ("'[::1]x:1980': only :port may follow the ]", refusal("[::1]x:1980"));
  EXPECT_EQ("'[127.0.0.1]:1980': brackets hold an IPv6 address", refusal("[127.0.0.1]:1980"));
  EXPECT_EQ("'[host]:1980': brackets hold an IPv6 address", refusal("[host]:1980"));
  EXPECT_EQ("'[]:1980': brackets hold an IPv6 address", refusal("[]:1980"));
  EXPECT_EQ("'[[::1]]:1980': brackets hold an IPv6 address", refusal("[[::1]]:1980"));
  EXPECT_EQ("'host[1]:1980': brackets go around an IPv6 address, at the start", refusal("host[1]:1980"));
  EXPECT_EQ("'host]:1980': brackets go around an IPv6 address, at the start", refusal("host]:1980"));
}

TEST(Endpoint, PortIsANumberFrom0To65535) {
  for (const char* text : {"host:-1", "host:+80", "host: 80", "host:80 ", "host:8o", "host:65536", "host:99999999999",
                           "[::1]:-1", "[::1]:65536"}) {
    EXPECT_EQ("'" + std::string(text) + "': the port is a number from 0 to 65535", refusal(text)) << text;
  }
  EXPECT_EQ("'host:': no port after the colon", refusal("host:"));
  EXPECT_EQ("'[::1]:': no port after the colon", refusal("[::1]:"));
  EXPECT_EQ("':': no port after the colon", refusal(":"));
}

TEST(Endpoint, EmptyTextIsRefused) {
  EXPECT_EQ("'': no address", refusal(""));
  EXPECT_EQ("'': no address", refusal("", 1980));
}

TEST(Endpoint, Format) {
  EXPECT_EQ("127.0.0.1:1980", format_endpoint("127.0.0.1", 1980));
  EXPECT_EQ("mx.example.com:0", format_endpoint("mx.example.com", 0));
  EXPECT_EQ("[::1]:1980", format_endpoint("::1", 1980));
  EXPECT_EQ("[fe80::1%eth0]:65535", format_endpoint("fe80::1%eth0", 65535));
  EXPECT_EQ(":1980", format_endpoint("", 1980));
}

TEST(Endpoint, FormattedParsesBack) {
  for (const Endpoint& endpoint :
       {Endpoint("127.0.0.1", 1980), Endpoint("::1", 0), Endpoint("::", 1980), Endpoint("mx.example.com", 65535),
        Endpoint("", 1980), Endpoint("fe80::1%eth0", 7)}) {
    EXPECT_EQ(endpoint, parse_endpoint(format_endpoint(endpoint.first, endpoint.second))) << endpoint.first;
  }
}

}  // namespace
