// BaseMultiplexerServer::connect(), and BaseThreadedMultiplexerServer's,
// starts a connection to every address at once and waits for them all
// against one deadline, where it connected to one address after another,
// each waited for up to its timeout: a multiplexer that never welcomes
// held up every address after it that long. Two listeners that accept and
// never answer each get their connection while connect() still waits;
// where the second got nothing until the first's timeout ran out. Once
// both hang up, connect() returns, throwing nothing, with nothing
// connected. And serve_forever() waits for no handshake: a backend given a
// real multiplexer and a listener that never answers serves what the
// multiplexer routes to it while the listener holds, where serve_forever()
// first waited in connect() for every handshake. Counted, not timed: the
// accepts, the return, the registration and the answer are events; the
// bounds on waiting for them only detect a failure, each shorter than the
// timeout it tells apart.
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <string>
#include <thread>

#include "lib/kwargs.h"
#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/backend/base_threaded_multiplexer_server.h"
#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"

using multiplexer::backend::BaseMultiplexerServer;
using multiplexer::backend::BaseThreadedMultiplexerServer;
using multiplexer::backend::MultiplexerAddresses;
using multiplexer::testing::InProcessMultiplexer;

namespace {

// A backend that is only connected.
class ConnectingBackend : public BaseMultiplexerServer {
 public:
  explicit ConnectingBackend(const MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER) {}
  unsigned int connections() { return conn->connections_count(); }

 protected:
  void handle_message(multiplexer::MultiplexerMessage&) override { no_response(); }
};

// A threaded backend that is only connected.
class ThreadedConnectingBackend : public BaseThreadedMultiplexerServer {
 public:
  explicit ThreadedConnectingBackend(const MultiplexerAddresses& addresses)
      : BaseThreadedMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER) {}
  ~ThreadedConnectingBackend() override { close(); }
  unsigned int connections() { return client().connections_count(); }

 protected:
  void handle_message(const multiplexer::backend::RequestPtr& request) override { request->no_response(); }
};

// A backend that answers every request with "re: " and its payload; its
// loop notes once it is registered with a multiplexer, and it stops once
// `leave` is set.
class AnsweringBackend : public BaseMultiplexerServer {
 public:
  explicit AnsweringBackend(const MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER) {}
  std::atomic<bool> registered{false};
  std::atomic<bool> leave{false};

 protected:
  void handle_message(multiplexer::MultiplexerMessage& mxmsg) override {
    mx::util::kwargs::Kwargs reply;
    reply.set("message", "re: " + mxmsg.message());
    reply.set("type", static_cast<std::uint32_t>(multiplexer::types::PYTHON_TEST_RESPONSE));
    send_message(reply);
  }
  void periodic_task() override {
    if (conn->connections_count() > 0) {
      registered = true;
    }
    if (leave) {
      working = false;
    }
  }
};

// A listener on 127.0.0.1 that accepts and never answers: a multiplexer
// that never sends its welcome.
class SilentListener {
 public:
  SilentListener() : listening_(socket(AF_INET, SOCK_STREAM, 0)) {
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    EXPECT_EQ(0, bind(listening_, reinterpret_cast<sockaddr*>(&address), sizeof(address)));
    EXPECT_EQ(0, listen(listening_, 4));
    socklen_t length = sizeof(address);
    EXPECT_EQ(0, getsockname(listening_, reinterpret_cast<sockaddr*>(&address), &length));
    port = ntohs(address.sin_port);
  }
  ~SilentListener() {
    hang_up();
    close(listening_);
  }

  // Whether a connection came within `seconds`, a failure detector only;
  // it is held, never answered.
  bool accepted_within(int seconds) {
    pollfd ready = {listening_, POLLIN, 0};
    if (poll(&ready, 1, seconds * 1000) != 1) {
      return false;
    }
    accepted_ = accept(listening_, nullptr, nullptr);
    return accepted_ >= 0;
  }
  // The connection held is closed, which ends its handshake.
  void hang_up() {
    if (accepted_ >= 0) {
      close(accepted_);
      accepted_ = -1;
    }
  }

  unsigned short port = 0;

 private:
  int listening_;
  int accepted_ = -1;
};

// Two silent listeners each get their connection while the connect() of a
// `Backend` made for both still waits, and once both hang up it returns.
template <typename Backend>
void every_address_tried_at_once() {
  SilentListener first;
  SilentListener second;
  std::promise<std::string> outcome;
  std::future<std::string> connected = outcome.get_future();
  // The backend is made, connected and dropped on a thread of its own, the
  // one it belongs to.
  std::thread connecting([&] {
    try {
      Backend backend({{"127.0.0.1", first.port}, {"127.0.0.1", second.port}});
      backend.connect();
      outcome.set_value("returned, " + std::to_string(backend.connections()) + " connected");
    } catch (const std::exception& error) {
      outcome.set_value(std::string("threw ") + error.what());
    }
  });
  EXPECT_TRUE(first.accepted_within(5));
  EXPECT_TRUE(second.accepted_within(5)) << "the second address waited for the first's handshake";
  EXPECT_EQ(std::future_status::timeout, connected.wait_for(std::chrono::seconds(0))) << "connect() still waits";
  first.hang_up();
  second.hang_up();
  EXPECT_EQ(std::future_status::ready, connected.wait_for(std::chrono::seconds(5)))
      << "connect() ends with the last connection, not at its timeout";
  EXPECT_EQ("returned, 0 connected", connected.get());
  connecting.join();
}

}  // namespace

TEST(ServerConnect, EveryAddressIsTriedAtOnceAgainstOneDeadline) { every_address_tried_at_once<ConnectingBackend>(); }

TEST(ServerConnect, AThreadedServerTriesEveryAddressAtOnceToo) {
  every_address_tried_at_once<ThreadedConnectingBackend>();
}

// serve_forever() waits for no handshake: what the in-process multiplexer
// routes to the backend is answered while the silent listener, its other
// address, still holds its handshake. Its loop reports the registration,
// and the query is answered, each within half of the DEFAULT_TIMEOUT that
// serve_forever() first waited in connect() for a welcome that never
// comes.
TEST(ServerConnect, ServeForeverServesWhileAHandshakeHangs) {
  InProcessMultiplexer mx;
  SilentListener silent;
  const float half = multiplexer::DEFAULT_TIMEOUT / 2;
  AnsweringBackend backend({{"127.0.0.1", mx.port}, {"127.0.0.1", silent.port}});
  std::thread serving([&backend] { backend.serve_forever(0.05f); });
  EXPECT_TRUE(silent.accepted_within(5));
  const std::chrono::steady_clock::time_point until =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(static_cast<int>(half * 1000));
  while (!backend.registered.load() && std::chrono::steady_clock::now() < until) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(backend.registered.load()) << "the loop did not run while a handshake hung";
  {
    multiplexer::Client client(multiplexer::peers::PYTHON_TEST_CLIENT);
    ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
    try {
      const multiplexer::IncomingMessage reply = client.query("x", multiplexer::types::PYTHON_TEST_REQUEST, half);
      EXPECT_EQ("re: x", reply.third->message());
    } catch (const std::exception& error) {
      ADD_FAILURE() << "the query failed: " << error.what();
    }
  }
  backend.leave = true;
  serving.join();
  silent.hang_up();
}
