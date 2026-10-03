// disconnect(): a client drops a multiplexer it was given, in both C++
// clients. The target is gone, observed without waiting: with the only one
// dropped, nothing is connected or on its way, so a flushing send throws
// NotConnected at once where a reconnect would have reached the
// multiplexer listening there; a second disconnect() finds nothing, no
// reconnect left; what a live connection had not written goes to another
// connection, which writes it; a connect() waiting for the target returns
// false; and the calls refuse after shutdown() and on the io thread, as
// connect() does. Counted, not timed: nothing here waits for a reconnect
// that does not come.
#include <gtest/gtest.h>

#include <asio/ip/tcp.hpp>
#include <atomic>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

using multiplexer::Client;
using multiplexer::ConnectionWrapper;
using multiplexer::MultiplexerMessage;
using multiplexer::ThreadedClient;
using multiplexer::testing::FILL_FRAMES;
using multiplexer::testing::fill_size;
using multiplexer::testing::InProcessMultiplexer;
namespace peers = multiplexer::peers;
namespace types = multiplexer::types;
typedef asio::ip::tcp::endpoint Endpoint;

namespace {

// Holds a multiplexer's io thread, so that it reads nothing while the
// sockets stay open: a multiplexer frozen, until the end of the scope.
struct Freeze {
  explicit Freeze(InProcessMultiplexer& mx) {
    std::shared_future<void> gate = released.get_future().share();
    std::promise<void> frozen;
    mx.io_service.post([gate, &frozen] {
      frozen.set_value();
      gate.wait();
    });
    frozen.get_future().wait();
  }
  ~Freeze() { released.set_value(); }
  std::promise<void> released;
};

// A port nothing listens on: one a multiplexer had, gone now, which a
// test may bring back up there.
unsigned short port_left_free() {
  InProcessMultiplexer gone;
  return gone.port;
}

// A message of `type`, asking for no delivery error, with its id and
// sender.
MultiplexerMessage message(Client& client, std::uint32_t type, const std::string& payload) {
  MultiplexerMessage msg;
  msg.set_id(client.random64());
  msg.set_sender(client.instance_id());
  msg.set_type(type);
  msg.set_report_delivery_error(false);
  msg.set_message(payload);
  return msg;
}

}  // namespace

// The only multiplexer dropped while its connection lives: none is left,
// nor any reconnect, so a flushing send throws NotConnected at once,
// where a reconnect would have reached the multiplexer, still up, and
// written it.
TEST(Disconnect, ASyncClientWithItsOnlyTargetDroppedHasNothingComing) {
  InProcessMultiplexer mx;
  Client client(peers::WEBSITE);
  ASSERT_TRUE(client.wait_for_connection(client.connect("127.0.0.1", mx.port, 5), 5));
  EXPECT_TRUE(client.disconnect("127.0.0.1", mx.port));
  EXPECT_EQ(0u, client.connections_count());
  EXPECT_THROW(client.send(message(client, types::TEST_UNROUTED, "after"), 60), Client::NotConnected);
  EXPECT_FALSE(client.disconnect(Endpoint(asio::ip::make_address("127.0.0.1"), mx.port))) << "gone, reconnect and all";
  EXPECT_FALSE(client.disconnect("127.0.0.1", 1)) << "never given";
}

// A target whose connection failed has a reconnect armed: dropped, it
// never reaches the multiplexer that comes up there since, the flushing
// send throwing NotConnected at once. The address matches in any
// spelling.
TEST(Disconnect, ASyncClientsReconnectEndsWithItsTarget) {
  const unsigned short port = port_left_free();
  Client client(peers::WEBSITE);
  EXPECT_FALSE(client.wait_for_connection(client.connect("127.0.0.1", port, 5), 0)) << "refused: a reconnect armed";
  EXPECT_FALSE(client.wait_for_connection(client.connect("0:0:0:0:0:0:0:1", port, 5), 0));
  InProcessMultiplexer there(port);  // what the reconnect would reach
  EXPECT_TRUE(client.disconnect("127.0.0.1", port));
  EXPECT_TRUE(client.disconnect("::1", port)) << "the address connect() was given, spelled otherwise";
  EXPECT_THROW(client.send(message(client, types::TEST_UNROUTED, "after"), 60), Client::NotConnected);
  EXPECT_FALSE(client.disconnect("0:0:0:0:0:0:0:1", port));
}

// What a live connection had not written when its multiplexer was
// dropped, everything waiting behind a frozen multiplexer's full socket,
// goes to the other connection, which writes it: the last message
// reaches a backend behind the other multiplexer only, the lane it went
// through following it, and nothing is given up on.
TEST(Disconnect, WhatALiveConnectionHadNotWrittenGoesToAnother) {
  InProcessMultiplexer dropped_mx;
  InProcessMultiplexer other_mx;
  Client backend(peers::PYTHON_TEST_SERVER);
  ASSERT_TRUE(backend.wait_for_connection(backend.connect("127.0.0.1", other_mx.port, 5), 5));
  Client client(peers::WEBSITE);
  const ConnectionWrapper dropped = client.connect("127.0.0.1", dropped_mx.port, 5);
  ASSERT_TRUE(client.wait_for_connection(dropped, 5));
  ASSERT_TRUE(client.wait_for_connection(client.connect("127.0.0.1", other_mx.port, 5), 5));
  const multiplexer::LanePtr lane = std::make_shared<multiplexer::Lane>(dropped);
  const MultiplexerMessage last = message(client, types::PYTHON_TEST_REQUEST, "last");
  {
    Freeze frozen(dropped_mx);
    const std::string fill(fill_size(), 'f'), chunk(16 * 1024, 'x');
    // The sockets full however far the kernel grew them, then twice the queue.
    for (int index = 0; index < FILL_FRAMES + 2 * 1024; ++index) {
      const std::string& payload = index < FILL_FRAMES ? fill : chunk;
      client.queue(message(client, types::TEST_UNROUTED, payload), multiplexer::DEFAULT_TIMEOUT, lane);
    }
    const Client::ScheduledMessageTracker tracker = client.queue(last, multiplexer::DEFAULT_TIMEOUT, lane);
    ASSERT_TRUE(tracker);
    ASSERT_TRUE(tracker.in_queue()) << "behind the frozen multiplexer's full socket";
    EXPECT_TRUE(client.disconnect("127.0.0.1", dropped_mx.port));
    EXPECT_EQ(1u, client.connections_count());
    client.flush(tracker, 60);
    EXPECT_TRUE(tracker.is_sent()) << "written, by the other connection";
    EXPECT_EQ(other_mx.port, lane->connection().target().second);
    EXPECT_EQ(0u, client.dropped());
  }
  std::pair<std::shared_ptr<MultiplexerMessage>, ConnectionWrapper> arrived = backend.receive_message(60);
  EXPECT_EQ(last.id(), arrived.first->id());
}

// After shutdown() disconnect() throws NotConnected, as connect() does.
TEST(Disconnect, ASyncClientRefusesAfterShutdown) {
  InProcessMultiplexer mx;
  Client client(peers::WEBSITE);
  client.connect("127.0.0.1", mx.port, 5);
  client.shutdown();
  EXPECT_THROW(client.disconnect("127.0.0.1", mx.port), Client::NotConnected);
}

// On the io thread: done before disconnect() returns, so the count is
// down at once, and no reconnect is left, which a second disconnect()
// would find; a lost connection's reconnect is found once.
TEST(Disconnect, AThreadedClientDropsOnItsIoThread) {
  InProcessMultiplexer mx;
  ThreadedClient client(peers::PYTHON_TEST_CLIENT);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  EXPECT_TRUE(client.disconnect("127.0.0.1", mx.port));
  EXPECT_EQ(0u, client.connections_count());
  EXPECT_FALSE(client.disconnect("127.0.0.1", mx.port)) << "no reconnect left";
  const unsigned short port = port_left_free();
  EXPECT_FALSE(client.connect("127.0.0.1", port, 5)) << "refused: a reconnect armed";
  EXPECT_TRUE(client.disconnect("127.0.0.1", port));
  EXPECT_FALSE(client.disconnect("127.0.0.1", port));
  EXPECT_TRUE(client.connect("127.0.0.1", mx.port, 5)) << "connect() gives the target again";
  client.shutdown();
}

// A connect() still waiting for the handshake, its multiplexer frozen,
// returns false once its target is dropped; it waited with no deadline.
// The name's lookup, a hook on the io thread inside the connect, says
// when the target exists.
TEST(Disconnect, AThreadedConnectWaitingForTheDroppedTargetReturnsFalse) {
  InProcessMultiplexer mx;
  ThreadedClient client(peers::PYTHON_TEST_CLIENT);
  std::promise<void> looked_up;
  std::atomic<bool> told{false};
  client.set_resolver([&](const std::string&, std::uint16_t, asio::error_code&) {
    if (!told.exchange(true)) {
      looked_up.set_value();
    }
    return std::vector<Endpoint>(1, Endpoint(asio::ip::make_address("127.0.0.1"), mx.port));
  });
  {
    Freeze frozen(mx);  // no welcome comes back
    std::future<bool> connected = std::async(std::launch::async, [&client] { return client.connect("mx", 1980, -1); });
    looked_up.get_future().wait();
    EXPECT_TRUE(client.disconnect("mx", 1980));
    EXPECT_FALSE(connected.get());
  }
  client.shutdown();
}

// From a callback, on the io thread, disconnect() throws std::logic_error,
// as connect() does there; after shutdown() NotConnected.
TEST(Disconnect, AThreadedClientRefusesOnItsIoThreadAndAfterShutdown) {
  InProcessMultiplexer mx;
  ThreadedClient client(peers::PYTHON_TEST_CLIENT);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::promise<std::string> outcome;
  client.send(client.new_message(types::TEST_UNROUTED, "x"), multiplexer::LanePtr(), [&](unsigned int) {
    try {
      client.disconnect("127.0.0.1", mx.port);
      outcome.set_value("returned");
    } catch (const std::logic_error&) {
      outcome.set_value("logic_error");
    }
  });
  EXPECT_EQ("logic_error", outcome.get_future().get());
  EXPECT_EQ(1u, client.connections_count()) << "still connected";
  client.shutdown();
  EXPECT_THROW(client.disconnect("127.0.0.1", mx.port), ThreadedClient::NotConnected);
}
