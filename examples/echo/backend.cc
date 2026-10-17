// Echo backend in C++: answers every ECHO_REQUEST with the upper-cased payload.
//
// Usage: backend_cc [host:port]     (default 127.0.0.1:1980)
//
// SIGTERM asks the backend to leave: the handler only sets a flag, which
// periodic_task() reads between iterations, the one async-signal-safe way.
// A Python backend may do the same (docs/api_python.md); the Python echo
// backend watches a file instead, as a deployment's preStop hook asks.

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

  // "ready" on stdout, once, when a multiplexer has welcomed the backend:
  // the echo test reads the line and queries at once, so it must mean
  // reachable. True once said.
  bool say_ready_once_connected() {
    if (!ready_said_ && conn->connections_count() > 0) {
      std::cout << "ready" << std::endl;
      ready_said_ = true;
    }
    return ready_said_;
  }

 protected:
  void handle_message(MultiplexerMessage& mxmsg) override {
    std::string payload = mxmsg.message();
    for (char& character : payload) {
      character = std::toupper(static_cast<unsigned char>(character));
    }
    send_message(Kwargs().set("message", payload).set("type", multiplexer::types::ECHO_RESPONSE));
  }

  // Runs after every iteration: "ready" once a multiplexer welcomed the
  // backend, if it had none at the start; the drain once the signal flag
  // is set.
  void periodic_task() override {
    say_ready_once_connected();
    if (leave_requested) {
      start_draining();
    }
  }

 private:
  bool ready_said_ = false;
};

int main(int argc, char** argv) {
  MultiplexerAddresses addresses;  // host:port, or [address]:port for an IPv6 one
  addresses.push_back(multiplexer::parse_endpoint(argc > 1 ? argv[1] : "127.0.0.1:1980"));
  // The handlers first: a SIGTERM that comes once the backend is
  // connected starts the drain, never the default end.
  std::signal(SIGTERM, [](int) { leave_requested = 1; });
  std::signal(SIGINT, [](int) { leave_requested = 1; });
  EchoBackend backend(addresses, multiplexer::peers::ECHO_BACKEND);
  // connect() waits for the handshakes, so that "ready" can come at once;
  // with no multiplexer reachable, the loop goes on trying and says it
  // once one welcomes the backend.
  backend.connect();
  if (!backend.say_ready_once_connected()) {
    std::cerr << "no multiplexer reachable yet; ready once one is" << std::endl;
  }
  // Drain for five seconds once asked, as the last resort: while another
  // backend is there the multiplexers route nothing new here, and alone
  // this one keeps serving to the end, so a replacement that registers
  // within those five seconds takes over without a failed request.
  backend.serve_forever(0.5f, 5.0f);
  return 0;
}
