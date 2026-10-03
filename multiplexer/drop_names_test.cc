// A dropped message is reported by the id and the type it was framed with,
// in both C++ clients and every form of send: the drop observer hears the
// id and the drop line names both, read from the frame (RawMessage::id(),
// type()), which nothing parses for them. Nothing is connected, and each
// message goes with no time to wait or at the shutdown, with a type of its
// own, so that its line is the first of its kind (LogSummary), which names
// the message. Counted, not timed: the SyncClient drops inside the call,
// ThreadedClient on its io thread, which shutdown() ends.
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <regex>
#include <set>
#include <string>
#include <utility>

#include "multiplexer/client.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

using multiplexer::MultiplexerMessage;
using multiplexer::RawMessage;
using multiplexer::ThreadedClient;

namespace {

// The types the tests give their messages, one each, from this one up:
// nothing would route them, and nothing is connected.
const std::uint32_t FIRST_TYPE = 3000;

// A message's id and type: what a drop line names.
typedef std::pair<std::uint64_t, std::uint32_t> Named;

// The ids a client's drop observer was told, from the client's loop thread.
struct Told {
  // The observer that keeps the ids it is told.
  multiplexer::BasicClient::DropObserver observer() {
    return [this](std::uint64_t id, multiplexer::DropReason) {
      std::lock_guard<std::mutex> lock(mutex);
      ids.insert(id);
    };
  }
  // A copy of the ids told so far.
  std::set<std::uint64_t> seen() {
    std::lock_guard<std::mutex> lock(mutex);
    return ids;
  }
  std::mutex mutex;
  std::set<std::uint64_t> ids;
};

// What the drop lines in `log` name, of the tests' types.
std::set<Named> named(const std::string& log) {
  static const std::regex line(R"(message dropped: [^\n]*; id (\d+), type (\d+))");
  std::set<Named> names;
  for (std::sregex_iterator match(log.begin(), log.end(), line), end; match != end; ++match) {
    const Named name(std::stoull((*match)[1].str()), static_cast<std::uint32_t>(std::stoul((*match)[2].str())));
    if (name.second >= FIRST_TYPE) {
      names.insert(name);
    }
  }
  return names;
}

// The ids of `names`.
std::set<std::uint64_t> ids_of(const std::set<Named>& names) {
  std::set<std::uint64_t> ids;
  for (const Named& name : names) {
    ids.insert(name.first);
  }
  return ids;
}

// A message of `type` with an id and a sender, as a send takes it whole.
MultiplexerMessage whole(multiplexer::Client& client, std::uint32_t type) {
  MultiplexerMessage msg;
  msg.set_id(client.random64());
  msg.set_sender(client.instance_id());
  msg.set_type(type);
  return msg;
}

}  // namespace

// The SyncClient's sends: a whole message, to one connection and to ALL,
// and a frame made of a serialized message with its id and type.
TEST(DropNames, EverySyncSendNamesItsMessage) {
  Told told;  // before the client, whose observer writes into it
  std::set<Named> sent;
  ::testing::internal::CaptureStderr();
  {
    multiplexer::Client client(multiplexer::peers::WEBSITE);  // never connected
    client.set_drop_observer(told.observer());
    const MultiplexerMessage one = whole(client, FIRST_TYPE);
    client.queue(one, 0);
    const MultiplexerMessage all = whole(client, FIRST_TYPE + 1);
    client.queue_all(all, 0);
    const MultiplexerMessage framed = whole(client, FIRST_TYPE + 2);
    std::string serialized = framed.SerializeAsString();
    client.queue(std::shared_ptr<const RawMessage>(new RawMessage(&serialized, framed.id(), framed.type())), 0);
    sent = {{one.id(), one.type()}, {all.id(), all.type()}, {framed.id(), framed.type()}};
    client.shutdown(0);
  }
  const std::string log = ::testing::internal::GetCapturedStderr();
  EXPECT_EQ(sent, named(log)) << log;
  EXPECT_EQ(ids_of(sent), told.seen());
}

// ThreadedClient's sends: a whole message, held for a connection until the
// shutdown, and every serialized form, given its message's id and type,
// with no time to wait.
TEST(DropNames, EveryThreadedSendNamesItsMessage) {
  Told told;  // before the client, whose observer writes into it
  std::set<Named> sent;
  ::testing::internal::CaptureStderr();
  {
    ThreadedClient client(multiplexer::peers::WEBSITE);  // never connected
    client.set_drop_observer(told.observer());
    std::uint32_t type = FIRST_TYPE;
    // A message of a type of its own, among those sent.
    const auto next = [&client, &type, &sent] {
      const MultiplexerMessage msg = client.new_message(type++, "");
      sent.insert({msg.id(), msg.type()});
      return msg;
    };
    client.send(next());
    const MultiplexerMessage one = next();
    client.send_serialized(one.SerializeAsString(), one.id(), one.type(), multiplexer::LanePtr(), 0);
    const MultiplexerMessage all = next();
    client.send_all_serialized(all.SerializeAsString(), all.id(), all.type(), 0);
    const MultiplexerMessage waited = next();
    EXPECT_EQ(0u, client.send_serialized_and_wait(waited.SerializeAsString(), waited.id(), waited.type(), true, 0));
    const MultiplexerMessage called = next();
    client.send_serialized_with_callback(called.SerializeAsString(), called.id(), called.type(), false, 0,
                                         [](unsigned int) {});
    const MultiplexerMessage notified = next();
    client.send_serialized_and_notify(notified.SerializeAsString(), notified.id(), notified.type(), false, 0,
                                      [](unsigned int, bool) {});
    client.shutdown(0);  // what is still held is dropped, and the io thread ends
  }
  const std::string log = ::testing::internal::GetCapturedStderr();
  EXPECT_EQ(6u, sent.size());
  EXPECT_EQ(sent, named(log)) << log;
  EXPECT_EQ(ids_of(sent), told.seen());
}
