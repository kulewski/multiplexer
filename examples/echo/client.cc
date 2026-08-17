// Echo client in C++: sends one ECHO_REQUEST and prints the answer.
//
// Usage: client_cc [host:port] [text]     (default 127.0.0.1:1980 "hello multiplexer")

#include <iostream>
#include <string>

#include "multiplexer/client.h"
#include "multiplexer/multiplexer.constants.h" // generated from echo.rules

int main(int argc, char **argv) {
  std::string address = argc > 1 ? argv[1] : "127.0.0.1:1980";
  std::string text = argc > 2 ? argv[2] : "hello multiplexer";
  std::string::size_type colon = address.rfind(':');

  multiplexer::Client client(multiplexer::peers::ECHO_CLIENT);
  client.connect(address.substr(0, colon), static_cast<boost::uint16_t>(std::stoi(address.substr(colon + 1))));

  // query() sends the request through one connection and returns the reply;
  // it throws when there is none: OperationFailed when no backend of the
  // type is connected anywhere, OperationTimedOut when nothing answered.
  try {
    multiplexer::IncomingMessage reply = client.query(text, multiplexer::types::ECHO_REQUEST, /*timeout=*/10);
    std::cout << reply.third->message() << std::endl;
  } catch (const multiplexer::Client::OperationFailed &) {
    std::cerr << "no ECHO_BACKEND is connected" << std::endl;
    return 1;
  } catch (const multiplexer::Client::OperationTimedOut &) {
    std::cerr << "no answer within the timeout" << std::endl;
    return 1;
  }
  client.shutdown();
  return 0;
}
