// A connection or a lane belongs to its client: given one of another
// client's, every call that would place a message on it throws
// std::invalid_argument on the caller's thread, in both C++ clients, where
// it placed the message on the other client's connection, from this
// client's thread, the events of its write going to the other client. A
// lane of the client's own, and one that took no connection yet, go on as
// before.
#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
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

// A message nobody answers, asking for no delivery error, with its id and
// sender.
MultiplexerMessage message(std::uint64_t id, std::uint64_t sender) {
  MultiplexerMessage msg;
  msg.set_id(id);
  msg.set_sender(sender);
  msg.set_type(types::PYTHON_TEST_REQUEST);
  msg.set_report_delivery_error(false);
  msg.set_message("foreign");
  return msg;
}

// A SyncClient connected to `mx`.
std::unique_ptr<Client> sync_client(InProcessMultiplexer& mx) {
  std::unique_ptr<Client> client(new Client(peers::WEBSITE));
  client->connect("127.0.0.1", mx.port, 5);
  return client;
}

// A ThreadedClient connected to `mx`.
std::unique_ptr<ThreadedClient> threaded_client(InProcessMultiplexer& mx) {
  std::unique_ptr<ThreadedClient> client(new ThreadedClient(peers::WEBSITE));
  client->connect("127.0.0.1", mx.port, 5);
  return client;
}

}  // namespace

TEST(ForeignLane, TheSynchronousClientRefusesAnotherClientsLaneAndConnection) {
  InProcessMultiplexer mx;
  std::unique_ptr<Client> owner = sync_client(mx);
  std::unique_ptr<Client> other = sync_client(mx);
  LanePtr lane(new Lane());
  owner->send(message(owner->random64(), owner->instance_id()), 5, lane);
  ASSERT_TRUE(lane->holds_connection());
  const ConnectionWrapper connection = lane->connection();
  const MultiplexerMessage msg = message(other->random64(), other->instance_id());
  EXPECT_THROW(other->send(msg, 5, lane), std::invalid_argument);
  EXPECT_THROW(other->queue(msg, 5, lane), std::invalid_argument);
  EXPECT_THROW(other->query(msg, 1, lane), std::invalid_argument);
  EXPECT_THROW(other->send(msg, connection), std::invalid_argument);
  EXPECT_THROW(other->schedule_one(msg, connection), std::invalid_argument);
  EXPECT_THROW(other->query(msg, connection, 1), std::invalid_argument);
  LanePtr own(new Lane());
  EXPECT_NO_THROW(other->send(msg, 5, own));  // its own, empty until now: taken
  EXPECT_NO_THROW(owner->send(message(owner->random64(), owner->instance_id()), 5, lane));  // still the owner's
}

TEST(ForeignLane, TheThreadedClientRefusesAnotherClientsLaneAndConnection) {
  InProcessMultiplexer mx;
  std::unique_ptr<ThreadedClient> owner = threaded_client(mx);
  std::unique_ptr<ThreadedClient> other = threaded_client(mx);
  LanePtr lane(new Lane());
  ASSERT_EQ(1u, owner->send(owner->new_message(types::PYTHON_TEST_REQUEST, "first"), lane, 5.0f));
  ASSERT_TRUE(lane->holds_connection());
  const ConnectionWrapper connection = lane->connection();
  const MultiplexerMessage msg = other->new_message(types::PYTHON_TEST_REQUEST, "foreign");
  EXPECT_THROW(other->send(msg, lane), std::invalid_argument);
  EXPECT_THROW(other->send(msg, lane, 5.0f), std::invalid_argument);
  EXPECT_THROW(other->query(msg, 1.0f, lane), std::invalid_argument);
  EXPECT_THROW(other->send(msg, connection), std::invalid_argument);
  EXPECT_THROW(other->query(msg, connection, 1.0f), std::invalid_argument);
  std::unique_ptr<Client> sync = sync_client(mx);  // a lane of the other class's client too
  EXPECT_THROW(sync->send(message(sync->random64(), sync->instance_id()), 5, lane), std::invalid_argument);
  LanePtr own(new Lane());
  EXPECT_EQ(1u, other->send(msg, own, 5.0f));  // its own, empty until now: taken
  EXPECT_EQ(1u, owner->send(owner->new_message(types::PYTHON_TEST_REQUEST, "again"), lane, 5.0f));
}
