// A message the synchronous client drops for a full incoming queue is not
// remembered as seen: its copy through another multiplexer, arriving once
// there is room, is taken, where it was dropped as a duplicate of the copy
// the full queue had dropped. Counted, not timed: the client's loop runs
// until the first copy is dropped (incoming_dropped()), and the second
// copy is followed on its connection by a marker, so that once the marker
// is in, the copy was taken or not.
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"

using multiplexer::BasicClient;
using multiplexer::ConnectionWrapper;
using multiplexer::MultiplexerMessage;
using multiplexer::testing::InProcessMultiplexer;

TEST(Duplicates, ACopyDroppedForAFullQueueLetsTheNextCopyIn) {
  InProcessMultiplexer first, second;
  asio::io_service io_service;
  std::shared_ptr<BasicClient> receiver = BasicClient::Create(io_service, multiplexer::peers::WEBSITE);
  ASSERT_TRUE(receiver->wait_for_connection(receiver->connect("127.0.0.1", first.port, 5), 5));
  ASSERT_TRUE(receiver->wait_for_connection(receiver->connect("127.0.0.1", second.port, 5), 5));
  multiplexer::Client sender(multiplexer::peers::WEBSITE);
  const ConnectionWrapper through_first = sender.connect("127.0.0.1", first.port, 5);
  const ConnectionWrapper through_second = sender.connect("127.0.0.1", second.port, 5);
  ASSERT_TRUE(through_first && through_second);
  auto message = [&](std::uint64_t id, const std::string& payload) {
    MultiplexerMessage msg;
    msg.set_id(id);
    msg.set_sender(sender.instance_id());
    msg.set_to(receiver->instance_id());
    msg.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
    msg.set_message(payload);
    return msg;
  };
  std::unique_ptr<mx::SimpleTimer> timer = receiver->create_timer(30);  // a failure detector only

  // The queue full, then the first copy dropped.
  for (unsigned int index = 0; index < multiplexer::DEFAULT_INCOMING_QUEUE_MAX_SIZE; ++index) {
    sender.send(message(sender.random64(), "filler"), through_first, 5);
  }
  while (!receiver->incoming_queue_full() && !timer->expired()) {
    receiver->run_one();
  }
  ASSERT_TRUE(receiver->incoming_queue_full());
  const std::uint64_t id = sender.random64();
  sender.send(message(id, "copy"), through_first, 5);
  while (receiver->incoming_dropped() == 0 && !timer->expired()) {
    receiver->run_one();
  }
  ASSERT_EQ(1u, receiver->incoming_dropped());
  while (receiver->has_incoming_messages()) {
    receiver->next_incoming_message();  // room again
  }

  // The second copy, and a marker behind it on the same connection.
  sender.send(message(id, "copy"), through_second, 5);
  sender.send(message(sender.random64(), "marker"), through_second, 5);
  std::vector<std::string> arrived;
  while ((arrived.empty() || arrived.back() != "marker") && !timer->expired()) {
    receiver->run_one();
    while (receiver->has_incoming_messages()) {
      arrived.push_back(receiver->next_incoming_message().third->message());
    }
  }
  EXPECT_EQ((std::vector<std::string>{"copy", "marker"}), arrived);
  receiver->shutdown();
}
