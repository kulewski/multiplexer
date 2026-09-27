// Task: option parsing and the shared --multiplexer client.
#include "mxcontrol/task.h"

#include <asio/ip/tcp.hpp>
#include <iostream>

#include "lib/repr.h"

namespace mxcontrol {

void Task::_add_multiplexer_client_options(mx::options::Options& options) {
  options.add("multiplexer,M", &multiplexers_,
              "multiplexer server address in a form [hostip]:port; "
              "hostip defaults to 127.0.0.1; may be repeated");
}

std::vector<std::pair<std::string, std::uint16_t>> Task::_multiplexer_addresses() const {
  std::vector<std::pair<std::string, std::uint16_t>> addresses;
  for (const std::string& address : multiplexers_) {
    const std::string::size_type colonpos = address.find(':');
    if (colonpos == std::string::npos || colonpos != address.rfind(':')) {
      MX_LOG(WARNING, LOWVERBOSITY, TEXT("invalid MX server address '" + address + "' ignored"));
      continue;
    }
    std::string host = address.substr(0, colonpos);
    if (host.empty()) {
      host = "127.0.0.1";
    }
    addresses.emplace_back(host, mx::from_string<std::uint16_t>(address.substr(colonpos + 1)));
  }
  return addresses;
}

void Task::__create_multiplexer_client(std::uint32_t peer_type) {
  io_service();  // force *io_service_ instantiation
  multiplexer_client_.reset(new multiplexer::Client(io_service_, peer_type));
  for (const std::pair<std::string, std::uint16_t>& address : _multiplexer_addresses()) {
    multiplexer_client_->connect(address.first, address.second);
  }
}

unsigned int Task::_connect_to_every_address(multiplexer::Client& client, const std::vector<std::string>& addresses,
                                             float timeout) {
  unsigned int reached = 0;
  for (const std::string& address : addresses) {
    std::string::size_type colon = address.rfind(':');
    if (colon == std::string::npos) {
      std::cerr << "invalid multiplexer address " << address << " (host:port expected)\n";
      continue;
    }
    std::string host = address.substr(0, colon);
    if (host.empty()) {
      host = "127.0.0.1";
    }
    const std::string port = address.substr(colon + 1);
    asio::ip::tcp::resolver resolver(io_service());
    asio::ip::tcp::resolver::iterator end;
    unsigned int connected = 0;
    try {
      asio::ip::tcp::resolver::query query(host, port);
      for (asio::ip::tcp::resolver::iterator entry = resolver.resolve(query); entry != end; ++entry) {
        const asio::ip::tcp::endpoint endpoint = *entry;
        // Connected means welcomed: a socket that opened but never finished
        // the handshake, refused or black-holed, is no multiplexer reached.
        multiplexer::ConnectionWrapper connection = client.async_connect(endpoint);
        if (client.wait_for_connection(connection, timeout)) {
          ++connected;
        } else {
          std::cerr << "cannot connect to " << endpoint << (connection ? " (no handshake in time)" : "") << "\n";
        }
      }
    } catch (const std::exception& e) {
      std::cerr << "cannot resolve " << address << ": " << e.what() << "\n";
    }
    if (connected) {
      ++reached;
    }
  }
  return reached;
}

void Task::parse_options(std::vector<std::string>& args) { _options().parse(args); }

void Task::print_help(std::ostream& out) { out << _options(); }
};  // namespace mxcontrol
