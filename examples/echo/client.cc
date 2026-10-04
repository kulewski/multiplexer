// Echo client in C++: sends one ECHO_REQUEST and prints the answer.
//
// Usage: client_cc [host:port] [text]     (default 127.0.0.1:1980 "hello multiplexer")

#include "multiplexer/client.h"

#include <iostream>
#include <string>

#include "multiplexer/endpoint.h"
#include "multiplexer/multiplexer.constants.h"  // generated from echo.rules

int main(int argc, char** argv) {
  // host:port, or [address]:port for an IPv6 one
  const std::pair<std::string, std::uint16_t> endpoint =
      multiplexer::parse_endpoint(argc > 1 ? argv[1] : "127.0.0.1:1980");
  std::string text = argc > 2 ? argv[2] : "hello multiplexer";

  multiplexer::SyncClient client(multiplexer::peers::ECHO_CLIENT);
  client.connect(endpoint.first, endpoint.second);

  // query() sends the request through one connection and returns the reply;
  // it throws when there is none: OperationFailed when no backend of the
  // type is connected anywhere, OperationTimedOut when nothing answered.
  try {
    multiplexer::IncomingMessage reply = client.query(text, multiplexer::types::ECHO_REQUEST, /*timeout=*/10);
    std::cout << reply.third->message() << std::endl;
  } catch (const multiplexer::SyncClient::OperationFailed&) {
    std::cerr << "no ECHO_BACKEND is connected" << std::endl;
    return 1;
  } catch (const multiplexer::SyncClient::OperationTimedOut&) {
    std::cerr << "no answer within the timeout" << std::endl;
    return 1;
  }
  client.shutdown();
  return 0;
}
