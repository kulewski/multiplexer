// A ThreadedClient's plain send made at its thread's end, from the
// destructor of a thread_local the thread made before its first plain
// send: C++ destroys the thread's spare body (threaded_client_plain.cc)
// first, and the send makes a body of its own rather than use the spare
// after its end, which under AddressSanitizer is a use after free and
// without it may corrupt the heap. The message goes out, after those the
// thread sent before.
#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <string>
#include <thread>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

namespace {

using multiplexer::MultiplexerMessage;
using multiplexer::ThreadedClient;
using multiplexer::testing::InProcessMultiplexer;
namespace peers = multiplexer::peers;
namespace types = multiplexer::types;

const float BOUND = 10;  // seconds a test waits at most, to detect a failure

// An event of TEST_EVENT, `payload` its message, given its id by the client.
MultiplexerMessage event(const std::string& payload) {
  MultiplexerMessage msg;
  msg.set_type(types::TEST_EVENT);
  msg.set_message(payload);
  return msg;
}

// A per-thread buffer that sends what it holds as its thread ends, as a
// per-thread log flushing then would.
struct FlushedAtExit {
  ~FlushedAtExit() {
    if (client) {
      client->send(event(std::string(200, 'f')));
      sent.set_value();
    }
  }
  ThreadedClient* client = nullptr;
  std::promise<void> sent;
};

thread_local FlushedAtExit flushed_at_exit;

TEST(PlainSendAtExit, APlainSendFromAThreadLocalsDestructorGoesOut) {
  InProcessMultiplexer mx;
  multiplexer::SyncClient receiver(peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(receiver.connect("127.0.0.1", mx.port, BOUND));
  ThreadedClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  multiplexer::BasicClient::Delivery batching;
  batching.batch = true;
  batching.max_delay = 3600;  // the flush lets the batch go
  batching.timeout = -1;
  sender.set_delivery(types::TEST_EVENT, batching);
  std::future<void> sent;
  std::thread thread([&sender, &sent] {
    flushed_at_exit.client = &sender;  // made before the spare body
    sent = flushed_at_exit.sent.get_future();
    // The spare left holding a body handed back, which its end frees: two
    // sends, a batch of two let go, whose bodies come back at the io
    // thread's next turn (a batch of one goes as the message itself, its
    // body in its frame), then another, whose spare one of them becomes.
    sender.send(event(std::string(200, 'p')));
    sender.send(event(std::string(200, 'p')));
    sender.flush_all(BOUND);
    sender.flush_all(BOUND);
    sender.send(event(std::string(200, 'q')));
  });
  thread.join();
  ASSERT_EQ(std::future_status::ready, sent.wait_for(std::chrono::seconds(0)));
  ASSERT_TRUE(sender.flush_all(BOUND));
  std::string arrived;
  for (int index = 0; index < 4; ++index) {
    std::shared_ptr<MultiplexerMessage> next = receiver.receive_message(BOUND).first;
    ASSERT_TRUE(next);
    arrived += next->message().substr(0, 1);
  }
  EXPECT_EQ("ppqf", arrived);
}

}  // namespace
