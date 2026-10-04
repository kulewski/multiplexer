// A C++ backend built in a workspace with no Python rules: it answers
// nothing, and is made only for the build to link it.
#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/multiplexer.constants.h"

namespace {

// Declines every message.
class Quiet : public multiplexer::backend::BaseMultiplexerServer {
 public:
  explicit Quiet(const multiplexer::backend::MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::LOG_RECEIVER_EXAMPLE) {}

 protected:
  void handle_message(multiplexer::MultiplexerMessage&) override { no_response(); }
};

}  // namespace

int main() {
  Quiet backend({{"127.0.0.1", 1980}});
  return 0;
}
