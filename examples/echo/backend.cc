// Echo backend in C++: answers every ECHO_REQUEST with the upper-cased payload.
//
// Usage: backend_cc [host:port]     (default 127.0.0.1:1980)
//
// SIGTERM asks the backend to leave: the handler only sets a flag, which
// periodic_task() reads between iterations, the one async-signal-safe way.
// A pure C++ process may rely on a signal like this; a Python process
// should not (see docs/faq.md), which is why the Python echo backend
// watches a file instead.

#include <cctype>
#include <csignal>
#include <iostream>
#include <string>

static volatile std::sig_atomic_t leave_requested = 0;

#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/multiplexer.constants.h" // generated from echo.rules

using multiplexer::MultiplexerMessage;
using multiplexer::backend::BaseMultiplexerServer;
using multiplexer::backend::MultiplexerAddresses;
using mx::util::kwargs::Kwargs;

class EchoBackend : public BaseMultiplexerServer {
public:
  EchoBackend(const MultiplexerAddresses &addresses, multiplexer::backend::PeerType type)
      : BaseMultiplexerServer(addresses, type) {}

protected:
  void handle_message(MultiplexerMessage &mxmsg) override {
    std::string payload = mxmsg.message();
    for (char &character : payload)
      character = std::toupper(static_cast<unsigned char>(character));
    send_message(
        Kwargs().set("message", payload).set("type", static_cast<boost::uint32_t>(multiplexer::types::ECHO_RESPONSE)));
  }

  // Runs after every iteration: start draining once the signal flag is set.
  void periodic_task() override {
    if (leave_requested)
      start_draining();
  }
};

int main(int argc, char **argv) {
  std::string address = argc > 1 ? argv[1] : "127.0.0.1:1980";
  std::string::size_type colon = address.rfind(':');
  MultiplexerAddresses addresses;
  addresses.push_back(
      std::make_pair(address.substr(0, colon), static_cast<boost::uint16_t>(std::stoi(address.substr(colon + 1)))));
  EchoBackend backend(addresses, multiplexer::peers::ECHO_BACKEND);
  std::cout << "ready" << std::endl;
  // Drain for five seconds once asked: searches are declined so no retried
  // request comes here, requests that still arrive are served, then the
  // process exits. A rolling restart costs nobody a timeout.
  std::signal(SIGTERM, [](int) { leave_requested = 1; });
  std::signal(SIGINT, [](int) { leave_requested = 1; });
  backend.serve_forever(0.5f, 5.0f);
  return 0;
}
