// Connecting by host name: the name is resolved inside the library, on
// every attempt, so a name that does not resolve yet is retried rather
// than fatal, and a multiplexer that moved is found at the next reconnect.
// The resolver is a hook here, a map the test changes mid-way.
#include <gtest/gtest.h>

#include <asio/ip/tcp.hpp>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

using multiplexer::AUTO_RECONNECT_TIME;
using multiplexer::BasicClient;
using multiplexer::Client;
using multiplexer::ThreadedClient;
using multiplexer::testing::InProcessMultiplexer;
namespace peers = multiplexer::peers;
typedef asio::ip::tcp::endpoint Endpoint;

namespace {

Endpoint local(unsigned short port) { return Endpoint(asio::ip::make_address("127.0.0.1"), port); }

// Waits up to `seconds` for the client to have `count` live connections.
bool connections_become(ThreadedClient& client, unsigned int count, float seconds) {
  std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(static_cast<long>(seconds * 1000));
  while (std::chrono::steady_clock::now() < deadline) {
    if (client.connections_count() == count) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return client.connections_count() == count;
}

}  // namespace

TEST(ConnectByName, ANameThatDoesNotResolveYetIsRetriedNotFatal) {
  InProcessMultiplexer mx;
  ThreadedClient client(peers::PYTHON_TEST_CLIENT);
  std::atomic<bool> published{false};
  client.set_resolver([&](const std::string& host, std::uint16_t, asio::error_code& error) {
    std::vector<Endpoint> found;
    if (host == "mx" && published.load()) {
      found.push_back(local(mx.port));
    } else {
      error = asio::error::host_not_found;
    }
    return found;
  });
  std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
  EXPECT_FALSE(client.connect("mx", 1980, 5)) << "no address yet";
  EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(2)) << "at once, not after the timeout";
  EXPECT_EQ(0u, client.connections_count());
  published = true;
  EXPECT_TRUE(connections_become(client, 1, AUTO_RECONNECT_TIME + 3.0f)) << "found once the name resolves";
  client.shutdown();
}

TEST(ConnectByName, AMultiplexerThatMovedIsFoundAtTheNextReconnect) {
  std::unique_ptr<InProcessMultiplexer> first(new InProcessMultiplexer);
  InProcessMultiplexer second;
  std::atomic<unsigned short> where{first->port};
  ThreadedClient client(peers::PYTHON_TEST_CLIENT);
  client.set_resolver([&](const std::string&, std::uint16_t, asio::error_code&) {
    return std::vector<Endpoint>(1, local(where.load()));
  });
  ASSERT_TRUE(client.connect("mx", 1980, 5));
  where = second.port;  // the name now points at the other multiplexer
  first.reset();        // and the one the client is on goes away
  EXPECT_TRUE(connections_become(client, 0, 5));
  EXPECT_TRUE(connections_become(client, 1, AUTO_RECONNECT_TIME + 3.0f))
      << "reconnected to the address the name resolves to now; the old one is gone";
  client.shutdown();
}

TEST(ConnectByName, EveryAddressOfANameIsTriedInTurn) {
  InProcessMultiplexer mx;
  ThreadedClient client(peers::PYTHON_TEST_CLIENT);
  client.set_resolver([&](const std::string&, std::uint16_t, asio::error_code&) {
    std::vector<Endpoint> found;
    found.push_back(local(1));  // nothing listens here
    found.push_back(local(mx.port));
    return found;
  });
  EXPECT_TRUE(client.connect("mx", 1980, 5)) << "the second address after the first refused";
  client.shutdown();
}

TEST(ConnectByName, TheSystemResolverAndLocalhost) {
  InProcessMultiplexer mx;
  ThreadedClient client(peers::PYTHON_TEST_CLIENT);
  // A generous timeout: the system resolver in a test sandbox can be slow.
  EXPECT_TRUE(client.connect("localhost", mx.port, 10)) << "whichever address localhost has first";
  client.shutdown();
}

TEST(ConnectByName, TheSynchronousClientDoesNotThrowOnAnUnknownName) {
  Client client(peers::WEBSITE);
  client.set_resolver([&](const std::string&, std::uint16_t, asio::error_code& error) {
    error = asio::error::host_not_found;
    return std::vector<Endpoint>();
  });
  multiplexer::ConnectionWrapper wrapper;
  EXPECT_NO_THROW(wrapper = client.connect("mx", 1980, 0.5f));
  EXPECT_FALSE(wrapper);
  EXPECT_EQ(std::string("mx"), wrapper.target().first);
}
