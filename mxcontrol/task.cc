// Task: option parsing and the shared --multiplexer client.
#include "mxcontrol/task.h"

#include <stdexcept>

#include "multiplexer/endpoint.h"

namespace mxcontrol {

void Task::_add_multiplexer_client_options(mx::options::Options& options) {
  options.add("multiplexer,M", &multiplexers_,
              "a multiplexer's address, host:port or [IPv6 address]:port, an empty host meaning 127.0.0.1; "
              "may be repeated");
}

std::vector<std::pair<std::string, std::uint16_t>> Task::_multiplexer_addresses() const {
  std::vector<std::pair<std::string, std::uint16_t>> addresses;
  for (const std::string& address : multiplexers_) {
    std::pair<std::string, std::uint16_t> endpoint = multiplexer::parse_endpoint(address);
    if (endpoint.first.empty()) {
      endpoint.first = "127.0.0.1";
    }
    addresses.push_back(endpoint);
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

void Task::parse_options(std::vector<std::string>& args) {
  _options().parse(args);
  for (const std::string& address : multiplexers_) {
    try {
      multiplexer::parse_endpoint(address);
    } catch (const std::invalid_argument& error) {
      throw mx::options::Error(std::string("--multiplexer: ") + error.what());
    }
  }
}

void Task::print_help(std::ostream& out) { out << _options(); }
};  // namespace mxcontrol
