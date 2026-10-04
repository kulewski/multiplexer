// A C++ client built in a workspace with no Python rules: made, it says
// its instance id, and connects to nothing.
#include "multiplexer/client.h"

#include <iostream>

#include "multiplexer/multiplexer.constants.h"

int main() {
  multiplexer::Client client(multiplexer::peers::LOG_RECEIVER_EXAMPLE);
  std::cout << client.instance_id() << "\n";
  return 0;
}
