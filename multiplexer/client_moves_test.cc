// A client moves and does not copy: SyncClient and ThreadedClient in a
// vector that grows keep working, each of them. Copyable, they shared their
// BasicClient or Core with the copy, and the destructor of the one left
// behind shut the shared client down under the other, whose every call then
// failed with NotConnected; a vector's growth copied and destroyed each one
// it held. A moved-from client is only destroyed, which does nothing.
#include <gtest/gtest.h>

#include <type_traits>
#include <utility>
#include <vector>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

namespace {

using multiplexer::testing::InProcessMultiplexer;

static_assert(!std::is_copy_constructible<multiplexer::ThreadedClient>::value, "a copy would share the client");
static_assert(!std::is_copy_assignable<multiplexer::ThreadedClient>::value, "a copy would share the client");
static_assert(std::is_nothrow_move_constructible<multiplexer::ThreadedClient>::value, "a vector moves it");
static_assert(!std::is_copy_constructible<multiplexer::SyncClient>::value, "a copy would share the client");
static_assert(!std::is_copy_assignable<multiplexer::SyncClient>::value, "a copy would share the client");
static_assert(std::is_nothrow_move_constructible<multiplexer::SyncClient>::value, "a vector moves it");

const int CLIENTS = 5;  // past the first few capacities of a vector: it grows, moving what it holds, more than once

TEST(ClientMoves, AVectorOfThreadedClientsGrowsWithEveryOneConnected) {
  InProcessMultiplexer mx;
  std::vector<multiplexer::ThreadedClient> clients;
  for (int index = 0; index < CLIENTS; ++index) {
    clients.emplace_back(multiplexer::peers::WEBSITE);
    ASSERT_TRUE(clients.back().connect("127.0.0.1", mx.port, 5));
  }
  for (multiplexer::ThreadedClient& client : clients) {
    EXPECT_EQ(1u, client.connections_count());
  }
}

TEST(ClientMoves, AVectorOfSyncClientsGrowsWithEveryOneConnected) {
  InProcessMultiplexer mx;
  std::vector<multiplexer::SyncClient> clients;
  for (int index = 0; index < CLIENTS; ++index) {
    clients.emplace_back(multiplexer::peers::WEBSITE);
    ASSERT_TRUE(clients.back().connect("127.0.0.1", mx.port, 5));
  }
  for (multiplexer::SyncClient& client : clients) {
    EXPECT_EQ(1u, client.connections_count());
  }
}

}  // namespace
