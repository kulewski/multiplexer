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
#include "multiplexer/endpoint.h"
#include "multiplexer/multiplexer.constants.h"  // generated from echo.rules

using multiplexer::MultiplexerMessage;
using multiplexer::backend::BaseMultiplexerServer;
using multiplexer::backend::MultiplexerAddresses;
using mx::util::kwargs::Kwargs;

class EchoBackend : public BaseMultiplexerServer {
 public:
  EchoBackend(const MultiplexerAddresses& addresses, multiplexer::backend::PeerType type)
      : BaseMultiplexerServer(addresses, type) {
    // The drain as the last resort of the type: alone, the backend keeps
    // serving through it; beside another, it gets nothing new.
    multiplexer::Routing routing = multiplexer::backend::direct_only_routing();
    routing.set_last_resort(true);
    set_drain_routing(routing);
  }

 protected:
  void handle_message(MultiplexerMessage& mxmsg) override {
    std::string payload = mxmsg.message();
    for (char& character : payload) {
      character = std::toupper(static_cast<unsigned char>(character));
    }
    send_message(
        Kwargs().set("message", payload).set("type", static_cast<std::uint32_t>(multiplexer::types::ECHO_RESPONSE)));
  }

  // Runs after every iteration: start draining once the signal flag is set.
  void periodic_task() override {
    if (leave_requested) {
      start_draining();
    }
  }
};

int main(int argc, char** argv) {
  MultiplexerAddresses addresses;  // host:port, or [address]:port for an IPv6 one
  addresses.push_back(multiplexer::parse_endpoint(argc > 1 ? argv[1] : "127.0.0.1:1980"));
  EchoBackend backend(addresses, multiplexer::peers::ECHO_BACKEND);
  // The echo test reads "ready" and queries at once, so the line must mean
  // reachable: connect() first; serve_forever() would otherwise.
  backend.connect();
  std::cout << "ready" << std::endl;
  // Drain for five seconds once asked, as the last resort: while another
  // backend is there the multiplexers route nothing new here, and alone
  // this one keeps serving to the end. A rolling restart costs nobody a
  // request.
  std::signal(SIGTERM, [](int) { leave_requested = 1; });
  std::signal(SIGINT, [](int) { leave_requested = 1; });
  backend.serve_forever(0.5f, 5.0f);
  return 0;
}
