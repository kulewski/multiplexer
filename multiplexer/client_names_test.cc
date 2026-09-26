// The synchronous client's two names: SyncClient, the name the docs use
// after 2.3.1, is the class Client, which code written before may still
// name, forward-declare or derive from. Most of this is checked when it
// compiles; the test constructs one through each spelling.

namespace multiplexer {
class Client;  // a forward declaration as code written for 2.3.1 may have it
}  // namespace multiplexer

#include <gtest/gtest.h>

#include <type_traits>

#include "multiplexer/client.h"

static_assert(std::is_same_v<multiplexer::SyncClient, multiplexer::Client>);
static_assert(std::is_same_v<multiplexer::SyncClient::OperationFailed, multiplexer::Client::OperationFailed>);

namespace {

constexpr std::uint32_t kPeerType = 200;  // any peer type: nothing connects

// Derived through the new name and initialized through the base's own
// name, Client, which a derived class finds as the injected-class-name.
struct ThroughTheClassName : multiplexer::SyncClient {
  explicit ThroughTheClassName(std::uint32_t type) : Client(type) {}
};

using multiplexer::SyncClient;

// Derived and initialized through the new name, found by ordinary lookup.
struct ThroughTheNewName : SyncClient {
  explicit ThroughTheNewName(std::uint32_t type) : SyncClient(type) {}
};

TEST(ClientNamesTest, EachSpellingConstructsAClient) {
  ThroughTheClassName through_the_class_name(kPeerType);
  ThroughTheNewName through_the_new_name(kPeerType);
  multiplexer::Client& first = through_the_class_name;
  multiplexer::SyncClient& second = through_the_new_name;
  EXPECT_NE(first.instance_id(), second.instance_id());
}

}  // namespace
