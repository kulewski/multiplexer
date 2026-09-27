// The multiplexer's routing index, the peers of each type: a peer type that
// a rule, a peer's override_rrules or a search names, and nobody has
// connected as, stays out of it, where every such type stayed in it for
// good, an empty list that every ALL_TYPES rule and search then walked: a
// peer naming ever new types grew it, and those walks, without bound.
// Counted on the multiplexer's own thread, once the peer's message to
// itself says the multiplexer has handled everything sent before it.
#include <gtest/gtest.h>

#include <future>
#include <string>

#include "multiplexer/Multiplexer.pb.h"
#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"

namespace {

using multiplexer::testing::InProcessMultiplexer;

// How many peer types the multiplexer's routing index holds, asked on its thread.
std::size_t indexed(InProcessMultiplexer& mx) {
  std::promise<std::size_t> answer;
  mx.io_service.post([&mx, &answer] { answer.set_value(mx.server->peer_types_indexed()); });
  return answer.get_future().get();
}

// A message from `client` of `type`, with its id and sender.
multiplexer::MultiplexerMessage message(multiplexer::Client& client, std::uint32_t type) {
  multiplexer::MultiplexerMessage msg;
  msg.set_id(client.random64());
  msg.set_from(client.instance_id());
  msg.set_type(type);
  msg.set_message("x");
  return msg;
}

TEST(RoutingIndex, APeerTypeNobodyHasStaysOutOfIt) {
  InProcessMultiplexer mx;
  multiplexer::Client client(multiplexer::peers::PYTHON_TEST_CLIENT);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  const std::size_t before = indexed(mx);
  multiplexer::MultiplexerMessage named = message(client, multiplexer::types::PYTHON_TEST_REQUEST);
  for (std::uint32_t peer_type = 5000; peer_type < 5100; ++peer_type) {
    multiplexer::MultiplexerMessageDescription::RoutingRule* rule = named.add_override_rrules();
    rule->set_peer_type(peer_type);
    rule->set_whom(multiplexer::MultiplexerMessageDescription::RoutingRule::ALL);
    rule->set_report_delivery_error(false);
  }
  client.flush(client.schedule_one(named), 5);
  multiplexer::MultiplexerMessage last = message(client, multiplexer::types::PYTHON_TEST_REQUEST);
  last.set_to(client.instance_id());
  client.flush(client.schedule_one(last), 5);
  for (;;) {  // behind everything sent before it
    std::pair<std::shared_ptr<multiplexer::MultiplexerMessage>, multiplexer::ConnectionWrapper> got =
        client.receive_message(10);
    if (got.first->id() == last.id()) {
      break;
    }
  }
  EXPECT_EQ(before, indexed(mx)) << "peer types a rule only named entered the index";
}

}  // namespace
