// Task: option parsing and the shared --multiplexer client.
#include "mxcontrol/task.h"

#include "lib/repr.h"

namespace mxcontrol {

void Task::_add_multiplexer_client_options(mx::options::Options& options) {
  options.add("multiplexer,M", &multiplexers_,
              "multiplexer server address in a form [hostip]:port; "
              "hostip defaults to 127.0.0.1; may be repeated");
}

void Task::__create_multiplexer_client(std::uint32_t peer_type) {
  using std::cerr;
  using std::string;

  io_service();  // force *io_service_ instantiation
  multiplexer_client_.reset(new multiplexer::Client(io_service_, peer_type));
  if (multiplexers_.size()) {
    for (const std::string& address : multiplexers_) {
      string::size_type colonpos = address.find(':');
      if (colonpos == string::npos || colonpos != address.rfind(':')) {
        MX_LOG(WARNING, LOWVERBOSITY, TEXT("invalid MX server address '" + address + "' ignored"));
      } else {
        string host = address.substr(0, colonpos);
        std::uint16_t port = mx::from_string<std::uint16_t>(address.substr(colonpos + 1, address.size()));
        if (host.empty()) {
          host = "127.0.0.1";
        }
        multiplexer_client_->connect(host, port);
      }
    }
  }
}

void Task::parse_options(std::vector<std::string>& args) { _options().parse(args); }

void Task::print_help(std::ostream& out) { out << _options(); }
};  // namespace mxcontrol
