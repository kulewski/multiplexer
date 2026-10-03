// connect() to a multiplexer the client has a connection to keeps that
// connection, in both C++ clients: the second call returns it, and a
// pinned lane through it still sends, where the call closed the live
// connection and opened another, ending the pinned lane for good and
// losing what the multiplexer had queued for the client there. Counted,
// not timed: a flushing send through the lane says whether its connection
// lived.
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

using multiplexer::Client;
using multiplexer::ConnectionWrapper;
using multiplexer::Lane;
using multiplexer::LanePtr;
using multiplexer::MultiplexerMessage;
using multiplexer::ThreadedClient;
using multiplexer::testing::InProcessMultiplexer;
namespace peers = multiplexer::peers;
namespace types = multiplexer::types;

namespace {

// An event of a type nothing routes, with its id and sender, asking for
// no delivery error: a message whose only job is to be written.
MultiplexerMessage event(Client& client, const std::string& payload) {
  MultiplexerMessage msg;
  msg.set_id(client.random64());
  msg.set_from(client.instance_id());
  msg.set_type(types::TEST_UNROUTED);
  msg.set_report_delivery_error(false);
  msg.set_message(payload);
  return msg;
}

}  // namespace

// The synchronous client's second connect() returns the live connection,
// and the pinned lane on it writes on; it threw NotConnected, the lane's
// connection closed by that connect().
TEST(ConnectAgain, ASyncClientKeepsItsLiveConnection) {
  InProcessMultiplexer mx;
  Client client(peers::WEBSITE);
  const ConnectionWrapper first = client.connect("127.0.0.1", mx.port, 5);
  ASSERT_TRUE(client.wait_for_connection(first, 5));
  const LanePtr pinned = std::make_shared<Lane>(true);
  client.send(event(client, "before"), 5, pinned);
  const ConnectionWrapper again = client.connect("127.0.0.1", mx.port, 5);
  EXPECT_TRUE(again.is_same_connection(first));
  EXPECT_FALSE(pinned->closed()) << "the pinned lane's connection was closed";
  EXPECT_NO_THROW(client.send(event(client, "after"), 5, pinned));
  EXPECT_EQ(1u, client.connections_count());
}

// The threaded client's second connect() is true at once, the connection
// kept, and the pinned lane on it writes on; the lane's connection was
// closed by that connect(), and the send through it came to nothing.
TEST(ConnectAgain, AThreadedClientKeepsItsLiveConnection) {
  InProcessMultiplexer mx;
  ThreadedClient client(peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  const LanePtr pinned = std::make_shared<Lane>(true);
  MultiplexerMessage before = client.new_message(types::TEST_UNROUTED, "before");
  before.set_report_delivery_error(false);
  ASSERT_EQ(1u, client.send(before, pinned, 5));
  EXPECT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  EXPECT_FALSE(pinned->closed()) << "the pinned lane's connection was closed";
  MultiplexerMessage after = client.new_message(types::TEST_UNROUTED, "after");
  after.set_report_delivery_error(false);
  EXPECT_EQ(1u, client.send(after, pinned, 5));
}
