// Confirmed sends (BasicClient::Delivery::confirm, basic_client_confirm.cc):
// a producer numbers the messages of a confirmed batch and keeps each until
// a consumer's library confirms it, its send's end and a flush waiting for
// that; it sends again, keeping ids and numbers, what was refused, what
// nobody took, what the connection that ended had written, and what
// nothing confirmed for retry_after, and gives up on what failed or ran
// past its batch's deadline, telling the drop observer; a type confirmed
// and not batched goes a message at a time. A consumer's library hands
// out each message with its number, a message sent again included, and
// gathers what it owes into one CONFIRMATION: a SyncClient once the
// program's receive waits, a ThreadedClient once on_message had what one
// read brought in hand, a server class as its handler returns or its
// Request goes.
// Counted, not timed: a raw peer on the far side plays the other library
// by hand, reading the frames as they went on the wire and writing the
// frames it would; the one test of retry_after waits on the resend itself;
// every wait is bounded only to detect a failure.
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "multiplexer/Multiplexer.pb.h"
#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/backend/base_threaded_multiplexer_server.h"
#include "multiplexer/client.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/io/raw_message.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

namespace {

using multiplexer::Confirmation;
using multiplexer::Confirmed;
using multiplexer::DropReason;
using multiplexer::IncomingMessage;
using multiplexer::MessageBatch;
using multiplexer::MultiplexerMessage;
using multiplexer::RawMessage;
using multiplexer::SyncClient;
using multiplexer::ThreadedClient;
using multiplexer::testing::InProcessMultiplexer;
namespace peers = multiplexer::peers;
namespace types = multiplexer::types;

const int BOUND = 30;      // seconds a wait may take before the test calls it stuck: a failure detector only
const float NEVER = 3600;  // a max_delay or retry_after no test lasts
const std::uint64_t PRODUCER = 0xc0f1;  // the raw peers' instance ids
const std::uint64_t CONSUMER = 0xc0f2;

std::string wire_bytes(const RawMessage& frame) {
  std::string bytes;
  for (const asio::const_buffer& part : frame.get_message_buffer()) {
    bytes.append(static_cast<const char*>(part.data()), part.size());
  }
  return bytes;
}

// Reads exactly `size` bytes into `at`: false when the stream ended, failed
// or stayed silent for BOUND seconds.
bool read_exactly(int fd, void* at, std::size_t size) {
  char* into = static_cast<char*>(at);
  while (size) {
    const ssize_t got = ::read(fd, into, size);
    if (got == 0 || (got < 0 && errno != EINTR)) {
      return false;
    }
    if (got > 0) {
      into += got;
      size -= static_cast<std::size_t>(got);
    }
  }
  return true;
}

// A peer that speaks the wire format by hand, connected and welcomed as
// `peer_type`: the other side's library, played frame by frame.
class RawPeer {
 public:
  RawPeer(unsigned short port, std::uint32_t peer_type, std::uint64_t id) : id_(id) {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    timeval bound = {BOUND, 0};
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &bound, sizeof bound);
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (::connect(fd_, reinterpret_cast<sockaddr*>(&address), sizeof address) != 0 ||
        !write(*multiplexer::impl::create_welcome_message(peer_type, id))) {
      throw std::runtime_error("no connection to the multiplexer");
    }
    MultiplexerMessage welcome;
    if (!next(&welcome) || welcome.type() != types::CONNECTION_WELCOME) {
      throw std::runtime_error("no welcome from the multiplexer");
    }
  }
  ~RawPeer() { ::close(fd_); }

  // The next frame but a heartbeat, parsed: false when none came.
  bool next(MultiplexerMessage* msg) {
    for (;;) {
      RawMessage frame;
      if (!read_exactly(fd_, frame.get_header_buffer().data(), frame.get_header_buffer().size()) ||
          !frame.unpack_header() ||
          !read_exactly(fd_, frame.get_body_buffer().data(), frame.get_body_buffer().size()) ||
          !msg->ParseFromString(frame.get_message())) {
        return false;
      }
      if (msg->type() != types::HEARTBIT) {
        return true;
      }
    }
  }
  bool write(const RawMessage& frame) {
    const std::string bytes = wire_bytes(frame);
    return ::write(fd_, bytes.data(), bytes.size()) == static_cast<ssize_t>(bytes.size());
  }
  bool send(const MultiplexerMessage& msg) {
    std::unique_ptr<RawMessage> frame(RawMessage::FromMessage(msg));
    return write(*frame);
  }
  std::uint64_t id() const { return id_; }

 private:
  int fd_;
  std::uint64_t id_;
};

// The numbers (first, count) pairs stand for.
template <typename Pairs>
std::vector<std::uint64_t> numbers_of(const Pairs& pairs) {
  std::vector<std::uint64_t> numbers;
  for (int pair = 0; pair + 1 < pairs.size(); pair += 2) {
    for (std::uint64_t number = pairs.Get(pair); number < pairs.Get(pair) + pairs.Get(pair + 1); ++number) {
      numbers.push_back(number);
    }
  }
  return numbers;
}

std::vector<std::uint64_t> range(std::uint64_t first, std::uint64_t count) {
  std::vector<std::uint64_t> numbers;
  for (std::uint64_t number = first; number < first + count; ++number) {
    numbers.push_back(number);
  }
  return numbers;
}

// A confirmed batch as the consumer's side reads it off the wire.
struct Batch {
  std::uint64_t id = 0;
  std::vector<MultiplexerMessage> messages;
  std::vector<std::uint64_t> numbers;
  std::vector<std::uint64_t> ids() const {
    std::vector<std::uint64_t> ids;
    for (const MultiplexerMessage& msg : messages) {
      ids.push_back(msg.id());
    }
    return ids;
  }
};

// The next frame `consumer` reads, which must be a batch: false otherwise.
bool next_batch(RawPeer& consumer, Batch* batch) {
  MultiplexerMessage envelope;
  MessageBatch inside;
  if (!consumer.next(&envelope) || !envelope.has_batch() || !inside.ParseFromString(envelope.batch())) {
    return false;
  }
  batch->id = envelope.id();
  batch->messages.clear();
  for (const std::string& body : inside.message()) {
    batch->messages.emplace_back();
    if (!batch->messages.back().ParseFromString(body)) {
      return false;
    }
  }
  batch->numbers = numbers_of(envelope.confirm());
  return true;
}

// A CONFIRMATION from `consumer` to `producer`, each list (first, count) pairs.
void confirm(RawPeer& consumer, std::uint64_t producer, const std::vector<std::uint64_t>& processed,
             const std::vector<std::uint64_t>& refused = {}, const std::vector<std::uint64_t>& failed = {}) {
  Confirmation confirmation;
  confirmation.mutable_processed()->Add(processed.begin(), processed.end());
  confirmation.mutable_refused()->Add(refused.begin(), refused.end());
  confirmation.mutable_failed()->Add(failed.begin(), failed.end());
  MultiplexerMessage msg;
  static std::uint64_t ids = 0xc0f1c0f100000000ULL;
  msg.set_id(++ids);
  msg.set_sender(consumer.id());
  msg.set_to(producer);
  msg.set_type(types::CONFIRMATION);
  confirmation.SerializeToString(msg.mutable_message());
  ASSERT_TRUE(consumer.send(msg));
}

// The next CONFIRMATION `producer` reads: false when another frame came.
bool next_confirmation(RawPeer& producer, Confirmation* confirmation) {
  MultiplexerMessage msg;
  return producer.next(&msg) && msg.type() == types::CONFIRMATION && msg.to() == producer.id() &&
         confirmation->ParseFromString(msg.message());
}

// A batch of `count` events of TEST_EVENT_ANY by hand from `producer`, the
// first numbered `first`, each its payload "event <index>"; the messages'
// ids are `id_base` on.
std::vector<MultiplexerMessage> send_batch(RawPeer& producer, std::uint64_t first, int count,
                                           std::uint64_t id_base = 0x1000) {
  std::vector<MultiplexerMessage> messages;
  MessageBatch inside;
  for (int index = 0; index < count; ++index) {
    MultiplexerMessage msg;
    msg.set_id(id_base + static_cast<std::uint64_t>(index));
    msg.set_sender(producer.id());
    msg.set_type(types::TEST_EVENT_ANY);
    msg.set_message("event " + std::to_string(index));
    inside.add_message(msg.SerializeAsString());
    messages.push_back(msg);
  }
  static std::uint64_t envelopes = 0xe0e0e0e000000000ULL;
  MultiplexerMessage envelope;
  envelope.set_id(++envelopes);
  envelope.set_sender(producer.id());
  envelope.set_type(types::TEST_EVENT_ANY);
  inside.SerializeToString(envelope.mutable_batch());
  envelope.add_confirm(first);
  envelope.add_confirm(static_cast<std::uint64_t>(count));
  EXPECT_TRUE(producer.send(envelope));
  return messages;
}

// Confirmed batching of `records` messages, closed by count or a flush,
// its batches given up on never.
SyncClient::Delivery confirmed(unsigned int records, float retry_after = NEVER) {
  SyncClient::Delivery delivery;
  delivery.batch = true;
  delivery.max_records = records;
  delivery.max_delay = NEVER;
  delivery.confirm = true;
  delivery.retry_after = retry_after;
  delivery.timeout = -1;
  return delivery;
}

// What a ThreadedClient producer's sends and drop observer heard, from
// its io thread.
struct Heard {
  std::mutex mutex;
  std::condition_variable changed;
  std::vector<unsigned int> ends;
  std::vector<std::pair<std::uint64_t, DropReason>> drops;
  multiplexer::BasicClient::SendCallback end() {
    return [this](unsigned int written) {
      std::lock_guard<std::mutex> lock(mutex);
      ends.push_back(written);
      changed.notify_all();
    };
  }
  multiplexer::BasicClient::DropObserver observer() {
    return [this](std::uint64_t id, DropReason reason) {
      std::lock_guard<std::mutex> lock(mutex);
      drops.emplace_back(id, reason);
      changed.notify_all();
    };
  }
  std::vector<unsigned int> ends_once(std::size_t count) {
    std::unique_lock<std::mutex> lock(mutex);
    changed.wait_for(lock, std::chrono::seconds(BOUND), [&] { return ends.size() >= count; });
    return ends;
  }
};

// An event of TEST_EVENT_ANY from `client`, `number` in its payload.
template <typename Client>
MultiplexerMessage event(Client& client, int number) {
  MultiplexerMessage msg = client.new_message(types::TEST_EVENT_ANY, "event " + std::to_string(number));
  return msg;
}

MultiplexerMessage sync_event(SyncClient& client, int number) {
  MultiplexerMessage msg;
  msg.set_id(client.random64());
  msg.set_sender(client.instance_id());
  msg.set_type(types::TEST_EVENT_ANY);
  msg.set_message("event " + std::to_string(number));
  return msg;
}

// The producer's side.

TEST(ConfirmedSends, ABatchNumbersItsMessagesAndTheirSendsEndAtTheConfirmation) {
  InProcessMultiplexer mx;
  RawPeer consumer(mx.port, peers::TEST_EVENT_BACKEND, CONSUMER);
  Heard heard;
  ThreadedClient producer(peers::TEST_EVENT_CLIENT);
  producer.set_drop_observer(heard.observer());
  ASSERT_TRUE(producer.connect("127.0.0.1", mx.port, BOUND));
  producer.set_delivery(types::TEST_EVENT_ANY, confirmed(4));
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 4; ++number) {
    MultiplexerMessage msg = event(producer, number);
    sent.push_back(msg.id());
    producer.send(msg, multiplexer::LanePtr(), heard.end());
  }
  Batch batch;
  ASSERT_TRUE(next_batch(consumer, &batch));
  EXPECT_EQ(sent, batch.ids());
  EXPECT_EQ(range(1, 4), batch.numbers);
  EXPECT_EQ(4u, producer.unconfirmed()) << "written, kept until confirmed";
  EXPECT_FALSE(producer.flush_all(0)) << "a flush waits for the confirmation";
  {
    std::lock_guard<std::mutex> lock(heard.mutex);
    EXPECT_TRUE(heard.ends.empty()) << "no send ends at its write";
  }
  confirm(consumer, producer.instance_id(), {1, 4});
  ASSERT_TRUE(producer.flush_all(BOUND));
  EXPECT_EQ((std::vector<unsigned int>{1, 1, 1, 1}), heard.ends_once(4));
  EXPECT_EQ(0u, producer.unconfirmed());
  EXPECT_EQ(0u, producer.sent_again());
  EXPECT_EQ(0u, producer.dropped());
}

// With no delay and the connection free, a message goes at once, as
// without batching, but in an envelope of its own that numbers it.
TEST(ConfirmedSends, AMessageThatWouldGoAloneGoesAsABatchOfOne) {
  InProcessMultiplexer mx;
  RawPeer consumer(mx.port, peers::TEST_EVENT_BACKEND, CONSUMER);
  ThreadedClient producer(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(producer.connect("127.0.0.1", mx.port, BOUND));
  SyncClient::Delivery batching = confirmed(100);
  batching.max_delay = 0;
  producer.set_delivery(types::TEST_EVENT_ANY, batching);
  MultiplexerMessage msg = event(producer, 0);
  producer.send(msg);
  Batch batch;
  ASSERT_TRUE(next_batch(consumer, &batch));
  EXPECT_EQ(std::vector<std::uint64_t>{msg.id()}, batch.ids());
  EXPECT_EQ(range(1, 1), batch.numbers);
  confirm(consumer, producer.instance_id(), {1, 1});
  EXPECT_TRUE(producer.flush_all(BOUND));
  EXPECT_EQ(0u, producer.unconfirmed());
}

TEST(ConfirmedSends, ARefusedMessageGoesAgainAtOnceWithItsIdAndNumber) {
  InProcessMultiplexer mx;
  RawPeer consumer(mx.port, peers::TEST_EVENT_BACKEND, CONSUMER);
  Heard heard;
  ThreadedClient producer(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(producer.connect("127.0.0.1", mx.port, BOUND));
  producer.set_delivery(types::TEST_EVENT_ANY, confirmed(4));
  for (int number = 0; number < 4; ++number) {
    producer.send(event(producer, number), multiplexer::LanePtr(), heard.end());
  }
  Batch first;
  ASSERT_TRUE(next_batch(consumer, &first));
  confirm(consumer, producer.instance_id(), {1, 2, 4, 1}, {3, 1});
  Batch again;
  ASSERT_TRUE(next_batch(consumer, &again));
  EXPECT_NE(first.id, again.id) << "a batch of its own";
  EXPECT_EQ(std::vector<std::uint64_t>{first.messages[2].id()}, again.ids());
  EXPECT_EQ(first.messages[2].message(), again.messages[0].message());
  EXPECT_EQ(range(3, 1), again.numbers);
  EXPECT_EQ(1u, producer.sent_again());
  EXPECT_EQ(1u, producer.unconfirmed());
  confirm(consumer, producer.instance_id(), {3, 1});
  ASSERT_TRUE(producer.flush_all(BOUND));
  EXPECT_EQ((std::vector<unsigned int>{1, 1, 1, 1}), heard.ends_once(4));
}

// The flush closes the batch itself, so that it begins before any
// confirmation can come.
TEST(ConfirmedSends, AFailedMessageIsGivenUpOnAndTold) {
  InProcessMultiplexer mx;
  RawPeer consumer(mx.port, peers::TEST_EVENT_BACKEND, CONSUMER);
  SyncClient producer(peers::TEST_EVENT_CLIENT);
  std::vector<std::pair<std::uint64_t, DropReason>> drops;
  producer.set_drop_observer([&drops](std::uint64_t id, DropReason reason) { drops.emplace_back(id, reason); });
  ASSERT_TRUE(producer.connect("127.0.0.1", mx.port, BOUND));
  producer.set_delivery(types::TEST_EVENT_ANY, confirmed(10));
  std::vector<unsigned int> ends;
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 4; ++number) {
    MultiplexerMessage msg = sync_event(producer, number);
    sent.push_back(msg.id());
    producer.queue(msg, BOUND, multiplexer::LanePtr(), [&ends](unsigned int written) { ends.push_back(written); });
  }
  std::thread far_side([&] {
    Batch batch;
    if (next_batch(consumer, &batch) && batch.ids() == sent) {
      confirm(consumer, producer.instance_id(), {1, 3}, {}, {4, 1});
    }
  });
  EXPECT_FALSE(producer.flush_all(BOUND)) << "one was given up on";
  far_side.join();
  EXPECT_EQ((std::vector<unsigned int>{1, 1, 1, 0}), ends);
  ASSERT_EQ(1u, drops.size());
  EXPECT_EQ(sent[3], drops[0].first);
  EXPECT_EQ(DropReason::FAILED, drops[0].second);
  EXPECT_EQ(0u, producer.unconfirmed());
  EXPECT_EQ(0u, producer.sent_again());
}

// Messages that hear no end, no `done` and no tracker, are resolved a run
// at a time: ranges across words of the flags, a range confirmed twice, a
// refusal in the middle, every message counted once.
TEST(ConfirmedSends, MessagesWithoutEndsAreResolvedARunAtATime) {
  InProcessMultiplexer mx;
  RawPeer consumer(mx.port, peers::TEST_EVENT_BACKEND, CONSUMER);
  ThreadedClient producer(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(producer.connect("127.0.0.1", mx.port, BOUND));
  producer.set_delivery(types::TEST_EVENT_ANY, confirmed(200));
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 200; ++number) {
    MultiplexerMessage msg = event(producer, number);
    sent.push_back(msg.id());
    producer.send(msg);
  }
  Batch batch;
  ASSERT_TRUE(next_batch(consumer, &batch));
  ASSERT_EQ(range(1, 200), batch.numbers);
  confirm(consumer, producer.instance_id(), {1, 70, 71, 29, 50, 40}, {100, 1}, {});  // 1-99, 50-89 again, 100 refused
  Batch again;
  ASSERT_TRUE(next_batch(consumer, &again));
  EXPECT_EQ(std::vector<std::uint64_t>{sent[99]}, again.ids());
  EXPECT_EQ(range(100, 1), again.numbers);
  confirm(consumer, producer.instance_id(), {101, 100, 100, 1, 1, 200});  // the rest, then 100, then all again
  ASSERT_TRUE(producer.flush_all(BOUND));
  EXPECT_EQ(0u, producer.unconfirmed());
  EXPECT_EQ(1u, producer.sent_again());
  EXPECT_EQ(0u, producer.dropped());
}

// A confirmation the producer waits for no longer, a number confirmed
// twice, never given or past the last there is, changes nothing.
TEST(ConfirmedSends, ANumberNotWaitedForIsPassedBy) {
  InProcessMultiplexer mx;
  RawPeer consumer(mx.port, peers::TEST_EVENT_BACKEND, CONSUMER);
  Heard heard;
  ThreadedClient producer(peers::TEST_EVENT_CLIENT);
  producer.set_drop_observer(heard.observer());
  ASSERT_TRUE(producer.connect("127.0.0.1", mx.port, BOUND));
  producer.set_delivery(types::TEST_EVENT_ANY, confirmed(2));
  for (int number = 0; number < 2; ++number) {
    producer.send(event(producer, number), multiplexer::LanePtr(), heard.end());
  }
  Batch batch;
  ASSERT_TRUE(next_batch(consumer, &batch));
  confirm(consumer, producer.instance_id(), {1, 1, 100, 5, ~0ULL, 2}, {7, 0}, {50, 1});
  confirm(consumer, producer.instance_id(), {1, 1}, {1, 1}, {1, 1});
  confirm(consumer, producer.instance_id(), {2, 1});
  ASSERT_TRUE(producer.flush_all(BOUND));
  EXPECT_EQ((std::vector<unsigned int>{1, 1}), heard.ends_once(2));
  EXPECT_EQ(0u, producer.sent_again()) << "a refusal of a number confirmed is passed by";
  EXPECT_EQ(0u, producer.dropped());
}

// Nobody to take the batch: the multiplexer's DELIVERY_ERROR, which the
// program never sees, sends it again after a back-off, until a consumer
// takes it.
TEST(ConfirmedSends, ABatchNobodyTookGoesAgainOnceSomebodyTakesIt) {
  InProcessMultiplexer mx;
  std::atomic<int> handed{0};
  ThreadedClient producer(peers::TEST_EVENT_CLIENT, [&handed](const IncomingMessage&) { ++handed; });
  ASSERT_TRUE(producer.connect("127.0.0.1", mx.port, BOUND));
  producer.set_delivery(types::TEST_EVENT_ANY, confirmed(3));
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 3; ++number) {
    MultiplexerMessage msg = event(producer, number);
    sent.push_back(msg.id());
    producer.send(msg);
  }
  for (int waited = 0; waited < BOUND * 100 && producer.sent_again() < 3; ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));  // a failure detector's bound: the back-off sends
  }
  ASSERT_GE(producer.sent_again(), 3u) << "sent again after the DELIVERY_ERROR";
  RawPeer consumer(mx.port, peers::TEST_EVENT_BACKEND, CONSUMER);
  Batch batch;
  ASSERT_TRUE(next_batch(consumer, &batch));
  EXPECT_EQ(sent, batch.ids());
  EXPECT_EQ(range(1, 3), batch.numbers);
  confirm(consumer, producer.instance_id(), {1, 3});
  ASSERT_TRUE(producer.flush_all(BOUND));
  EXPECT_EQ(0, handed.load()) << "no DELIVERY_ERROR reaches on_message";
}

// A multiplexer that dies takes the batch it was written to with it: the
// batch goes again at once, through the connection left.
TEST(ConfirmedSends, WhatTheEndedConnectionWroteGoesAgainThroughAnother) {
  std::unique_ptr<InProcessMultiplexer> first(new InProcessMultiplexer());
  InProcessMultiplexer second;
  std::unique_ptr<RawPeer> lost(new RawPeer(first->port, peers::TEST_EVENT_BACKEND, CONSUMER));
  ThreadedClient producer(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(producer.connect("127.0.0.1", first->port, BOUND));
  producer.set_delivery(types::TEST_EVENT_ANY, confirmed(3));
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 3; ++number) {
    MultiplexerMessage msg = event(producer, number);
    sent.push_back(msg.id());
    producer.send(msg);
  }
  Batch batch;
  ASSERT_TRUE(next_batch(*lost, &batch));
  RawPeer consumer(second.port, peers::TEST_EVENT_BACKEND, CONSUMER + 1);
  ASSERT_TRUE(producer.connect("127.0.0.1", second.port, BOUND));
  lost.reset();
  first.reset();
  Batch again;
  ASSERT_TRUE(next_batch(consumer, &again));
  EXPECT_EQ(sent, again.ids());
  EXPECT_EQ(range(1, 3), again.numbers);
  EXPECT_EQ(3u, producer.sent_again());
  confirm(consumer, producer.instance_id(), {1, 3});
  EXPECT_TRUE(producer.flush_all(BOUND));
}

// Nothing confirmed for retry_after: what is unresolved goes again, each
// send still ending once.
TEST(ConfirmedSends, WhatNothingConfirmedForRetryAfterGoesAgain) {
  InProcessMultiplexer mx;
  RawPeer consumer(mx.port, peers::TEST_EVENT_BACKEND, CONSUMER);
  Heard heard;
  ThreadedClient producer(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(producer.connect("127.0.0.1", mx.port, BOUND));
  producer.set_delivery(types::TEST_EVENT_ANY, confirmed(3, 0.5f));
  for (int number = 0; number < 3; ++number) {
    producer.send(event(producer, number), multiplexer::LanePtr(), heard.end());
  }
  Batch batch;
  ASSERT_TRUE(next_batch(consumer, &batch));
  Batch again;
  ASSERT_TRUE(next_batch(consumer, &again));  // the silence's resend
  EXPECT_EQ(batch.ids(), again.ids());
  EXPECT_EQ(range(1, 3), again.numbers);
  EXPECT_GE(producer.sent_again(), 3u);
  confirm(consumer, producer.instance_id(), {1, 3});
  ASSERT_TRUE(producer.flush_all(BOUND));
  EXPECT_EQ((std::vector<unsigned int>{1, 1, 1}), heard.ends_once(3)) << "once each";
}

// A message nobody confirms is given up on at its batch's deadline, the
// batch's timeout from its first message, and the flush waiting for it
// says so; the sends' own timeouts are not used.
TEST(ConfirmedSends, AMessageIsGivenUpOnAtItsBatchsDeadline) {
  InProcessMultiplexer mx;
  RawPeer consumer(mx.port, peers::TEST_EVENT_BACKEND, CONSUMER);  // reads nothing, confirms nothing
  SyncClient producer(peers::TEST_EVENT_CLIENT);
  std::vector<std::pair<std::uint64_t, DropReason>> drops;
  producer.set_drop_observer([&drops](std::uint64_t id, DropReason reason) { drops.emplace_back(id, reason); });
  ASSERT_TRUE(producer.connect("127.0.0.1", mx.port, BOUND));
  SyncClient::Delivery delivery = confirmed(3);
  delivery.max_delay = 0.1f;  // goes full, by its count, before that
  delivery.timeout = 0.3f;
  producer.set_delivery(types::TEST_EVENT_ANY, delivery);
  std::vector<unsigned int> ends;
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 3; ++number) {
    MultiplexerMessage msg = sync_event(producer, number);
    sent.push_back(msg.id());
    producer.queue(msg, -1, multiplexer::LanePtr(), [&ends](unsigned int written) { ends.push_back(written); });
  }
  EXPECT_FALSE(producer.flush_all(BOUND)) << "given up on";
  EXPECT_EQ((std::vector<unsigned int>{0, 0, 0}), ends);
  ASSERT_EQ(3u, drops.size());
  for (std::size_t index = 0; index < drops.size(); ++index) {
    EXPECT_EQ(sent[index], drops[index].first);
    EXPECT_EQ(DropReason::NOT_CONFIRMED, drops[index].second);
  }
  EXPECT_EQ(0u, producer.unconfirmed());
}

// A message of a confirmed batch that never went, held for a connection,
// is told given up on once: by its frame's drop, at the shutdown, not by
// the frame and again by the confirmations given up.
TEST(ConfirmedSends, AMessageThatNeverWentIsToldOnceAtTheShutdown) {
  SyncClient producer(peers::TEST_EVENT_CLIENT);  // connected to nothing: each batch is held for a connection
  std::vector<std::pair<std::uint64_t, DropReason>> drops;
  producer.set_drop_observer([&drops](std::uint64_t id, DropReason reason) { drops.emplace_back(id, reason); });
  producer.set_delivery(types::TEST_EVENT_ANY, confirmed(2));
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 2; ++number) {
    MultiplexerMessage msg = sync_event(producer, number);
    sent.push_back(msg.id());
    producer.queue(msg, BOUND);
  }
  EXPECT_EQ(2u, producer.unconfirmed());
  producer.shutdown();
  EXPECT_EQ(2u, producer.dropped());
  ASSERT_EQ(2u, drops.size()) << "each told once";
  for (std::size_t index = 0; index < drops.size(); ++index) {
    EXPECT_EQ(sent[index], drops[index].first);
    EXPECT_EQ(DropReason::SHUT_DOWN, drops[index].second);
  }
}

// One held for a connection past its deadline is told once too, whichever
// of its frame's deadline and its batch's comes first, both being the same.
TEST(ConfirmedSends, AMessageHeldPastItsDeadlineIsToldOnce) {
  SyncClient producer(peers::TEST_EVENT_CLIENT);
  std::vector<std::pair<std::uint64_t, DropReason>> drops;
  producer.set_drop_observer([&drops](std::uint64_t id, DropReason reason) { drops.emplace_back(id, reason); });
  // A listener that never welcomes: the connection stays on its way, and
  // the messages are held for it until their deadline.
  const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in address = {};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socklen_t length = sizeof address;
  ASSERT_EQ(0, ::bind(listener, reinterpret_cast<sockaddr*>(&address), length));
  ASSERT_EQ(0, ::listen(listener, 1));
  ASSERT_EQ(0, ::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length));
  SyncClient::Delivery delivery = confirmed(2);
  delivery.max_delay = 0.1f;  // goes full, by its count, before that
  delivery.timeout = 0.2f;
  producer.set_delivery(types::TEST_EVENT_ANY, delivery);
  producer.connect("127.0.0.1", ntohs(address.sin_port), 0);
  std::vector<unsigned int> ends;
  for (int number = 0; number < 2; ++number) {
    producer.queue(sync_event(producer, number), BOUND, multiplexer::LanePtr(),
                   [&ends](unsigned int written) { ends.push_back(written); });
  }
  EXPECT_FALSE(producer.flush_all(BOUND));
  EXPECT_EQ((std::vector<unsigned int>{0, 0}), ends);
  producer.shutdown();
  ::close(listener);
  EXPECT_EQ(2u, producer.dropped()) << "each told once";
  ASSERT_EQ(2u, drops.size());
  for (const std::pair<std::uint64_t, DropReason>& drop : drops) {
    EXPECT_TRUE(drop.second == DropReason::NO_CONNECTION || drop.second == DropReason::NOT_CONFIRMED);
  }
}

// Once every confirmed message is resolved, no timer of the confirmed
// sends waits: a SyncClient left with nothing connected and nothing to
// wait for has no work in its loop.
TEST(ConfirmedSends, NoTimerWaitsOnceEveryMessageIsResolved) {
  InProcessMultiplexer mx;
  SyncClient producer(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(producer.connect("127.0.0.1", mx.port, BOUND));
  RawPeer consumer(mx.port, peers::TEST_EVENT_BACKEND, CONSUMER);
  producer.set_delivery(types::TEST_EVENT_ANY, confirmed(1));
  producer.queue(sync_event(producer, 0), BOUND);
  std::thread far_side([&] {
    Batch batch;
    if (next_batch(consumer, &batch)) {
      confirm(consumer, producer.instance_id(), {1, 1});
    }
  });
  EXPECT_TRUE(producer.flush_all(BOUND));
  far_side.join();
  EXPECT_FALSE(producer.confirm_timer_armed());
}

// The SyncClient producer: the same, confirmations heard as its calls run
// the loop.
TEST(ConfirmedSends, ASyncClientsFlushWaitsForTheConfirmation) {
  InProcessMultiplexer mx;
  SyncClient producer(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(producer.connect("127.0.0.1", mx.port, BOUND));
  RawPeer consumer(mx.port, peers::TEST_EVENT_BACKEND, CONSUMER);
  producer.set_delivery(types::TEST_EVENT_ANY, confirmed(5));
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 5; ++number) {
    MultiplexerMessage msg = sync_event(producer, number);
    sent.push_back(msg.id());
    producer.queue(msg);
  }
  std::thread far_side([&] {
    Batch batch;
    if (next_batch(consumer, &batch) && batch.ids() == sent) {
      confirm(consumer, producer.instance_id(), {1, 5});
    }
  });
  EXPECT_TRUE(producer.flush_all(BOUND));
  far_side.join();
  EXPECT_EQ(0u, producer.unconfirmed());
}

// The consumer's side.

TEST(ConfirmedSends, ASyncClientConsumerConfirmsWhatItProcessedInOneConfirmation) {
  InProcessMultiplexer mx;
  SyncClient consumer(peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(consumer.connect("127.0.0.1", mx.port, BOUND));
  RawPeer producer(mx.port, peers::TEST_EVENT_CLIENT, PRODUCER);
  const std::vector<MultiplexerMessage> sent = send_batch(producer, 100, 50);
  for (std::size_t index = 0; index < sent.size(); ++index) {
    std::shared_ptr<MultiplexerMessage> msg = consumer.receive_message(BOUND).first;
    EXPECT_EQ(sent[index].id(), msg->id());
    EXPECT_EQ(100 + index, msg->confirmation());
    consumer.processed(*msg);
  }
  EXPECT_EQ(50u, consumer.confirmations_owed()) << "fewer than CONFIRMATIONS_GATHERED_MAX, kept while in hand";
  EXPECT_THROW(consumer.receive_message(0), SyncClient::OperationTimedOut);  // a receive that waits sends them
  EXPECT_EQ(0u, consumer.confirmations_owed());
  Confirmation confirmation;
  ASSERT_TRUE(next_confirmation(producer, &confirmation));
  EXPECT_EQ(range(100, 50), numbers_of(confirmation.processed())) << "every one, in one CONFIRMATION";
  EXPECT_EQ(0, confirmation.refused_size());
  EXPECT_EQ(0, confirmation.failed_size());
}

TEST(ConfirmedSends, AMessageSentAgainIsHandedOutAgain) {
  InProcessMultiplexer mx;
  SyncClient consumer(peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(consumer.connect("127.0.0.1", mx.port, BOUND));
  RawPeer producer(mx.port, peers::TEST_EVENT_CLIENT, PRODUCER);
  const std::vector<MultiplexerMessage> sent = send_batch(producer, 1, 3);
  send_batch(producer, 1, 3);  // the same messages, ids and numbers, as a resend
  for (int copy = 0; copy < 2; ++copy) {
    for (std::size_t index = 0; index < sent.size(); ++index) {
      std::shared_ptr<MultiplexerMessage> msg = consumer.receive_message(BOUND).first;
      EXPECT_EQ(sent[index].id(), msg->id()) << "copy " << copy;
      EXPECT_EQ(1 + index, msg->confirmation());
    }
  }
}

// A message that does not come from a confirmed batch carries no number,
// whatever its sender put there.
TEST(ConfirmedSends, OnlyAConfirmedBatchsMessageCarriesANumber) {
  InProcessMultiplexer mx;
  SyncClient consumer(peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(consumer.connect("127.0.0.1", mx.port, BOUND));
  RawPeer producer(mx.port, peers::TEST_EVENT_CLIENT, PRODUCER);
  MultiplexerMessage alone;
  alone.set_id(0x77);
  alone.set_sender(producer.id());
  alone.set_type(types::TEST_EVENT_ANY);
  alone.set_confirmation(5);
  ASSERT_TRUE(producer.send(alone));
  std::shared_ptr<MultiplexerMessage> msg = consumer.receive_message(BOUND).first;
  EXPECT_EQ(0x77u, msg->id());
  EXPECT_FALSE(msg->has_confirmation());
  consumer.processed(*msg);
  EXPECT_EQ(0u, consumer.confirmations_owed());
}

TEST(ConfirmedSends, AThreadedClientConfirmsWhatOnMessageReturnedFrom) {
  InProcessMultiplexer mx;
  ThreadedClient consumer(peers::TEST_EVENT_BACKEND, [](const IncomingMessage& incoming) {
    if (incoming.third->message() == "event 4") {
      throw std::runtime_error("this one fails");
    }
  });
  ASSERT_TRUE(consumer.connect("127.0.0.1", mx.port, BOUND));
  RawPeer producer(mx.port, peers::TEST_EVENT_CLIENT, PRODUCER);
  send_batch(producer, 1, 10);
  Confirmation confirmation;
  ASSERT_TRUE(next_confirmation(producer, &confirmation));
  EXPECT_EQ((std::vector<std::uint64_t>{1, 2, 3, 4, 6, 7, 8, 9, 10}), numbers_of(confirmation.processed()));
  EXPECT_EQ(range(5, 1), numbers_of(confirmation.failed()));
  EXPECT_EQ(0, confirmation.refused_size());
}

// A program that confirms its messages itself, from another thread.
TEST(ConfirmedSends, AThreadedClientsProgramMayConfirmFromAnyThread) {
  InProcessMultiplexer mx;
  std::mutex mutex;
  std::condition_variable arrived;
  std::vector<MultiplexerMessage> handed;
  ThreadedClient consumer(peers::TEST_EVENT_BACKEND, [&](const IncomingMessage& incoming) {
    std::lock_guard<std::mutex> lock(mutex);
    handed.push_back(*incoming.third);
    arrived.notify_all();
  });
  consumer.confirm_after_on_message(false);
  ASSERT_TRUE(consumer.connect("127.0.0.1", mx.port, BOUND));
  RawPeer producer(mx.port, peers::TEST_EVENT_CLIENT, PRODUCER);
  send_batch(producer, 1, 4);
  std::vector<MultiplexerMessage> taken;
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(arrived.wait_for(lock, std::chrono::seconds(BOUND), [&] { return handed.size() == 4; }));
    taken = handed;
  }
  EXPECT_EQ(0u, consumer.confirmations_owed()) << "nothing confirmed by on_message's return";
  consumer.processed(taken[0]);
  consumer.confirm(taken[1], Confirmed::REFUSED);
  consumer.processed(taken[2]);
  consumer.confirm(taken[3], Confirmed::FAILED);
  Confirmation confirmation;
  std::vector<std::uint64_t> processed, refused, failed;
  while (processed.size() + refused.size() + failed.size() < 4) {
    ASSERT_TRUE(next_confirmation(producer, &confirmation));
    for (std::uint64_t number : numbers_of(confirmation.processed())) {
      processed.push_back(number);
    }
    for (std::uint64_t number : numbers_of(confirmation.refused())) {
      refused.push_back(number);
    }
    for (std::uint64_t number : numbers_of(confirmation.failed())) {
      failed.push_back(number);
    }
  }
  EXPECT_EQ((std::vector<std::uint64_t>{1, 3}), processed);
  EXPECT_EQ(range(2, 1), refused);
  EXPECT_EQ(range(4, 1), failed);
}

// The server classes.

// Waits for `serving`, which a server's periodic_task sets once it serves:
// a failure detector's bound.
void wait_serving(const std::atomic<bool>& serving) {
  for (int waited = 0; waited < BOUND * 100 && !serving.load(); ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_TRUE(serving.load());
}

// What CONFIRMATIONs `producer` reads say, until they name `count` numbers.
void confirmations_until(RawPeer& producer, std::size_t count, std::vector<std::uint64_t>* processed,
                         std::vector<std::uint64_t>* failed) {
  while (processed->size() + failed->size() < count) {
    Confirmation confirmation;
    ASSERT_TRUE(next_confirmation(producer, &confirmation));
    for (std::uint64_t number : numbers_of(confirmation.processed())) {
      processed->push_back(number);
    }
    for (std::uint64_t number : numbers_of(confirmation.failed())) {
      failed->push_back(number);
    }
    EXPECT_EQ(0, confirmation.refused_size());
  }
  std::sort(processed->begin(), processed->end());
}

class Plain : public multiplexer::backend::BaseMultiplexerServer {
 public:
  explicit Plain(const multiplexer::backend::MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, peers::TEST_EVENT_BACKEND) {}
  std::atomic<bool> serving{false};
  std::atomic<bool> leave{false};

 protected:
  void handle_message(MultiplexerMessage& mxmsg) override {
    no_response();
    if (mxmsg.message() == "event 1") {
      throw std::runtime_error("this one fails");
    }
  }
  bool on_handler_exception(const std::exception&) override { return true; }
  void periodic_task() override {
    serving = true;
    if (leave.load()) {
      working = false;
    }
  }
};

TEST(ConfirmedSends, BaseMultiplexerServerConfirmsWhatItsHandlerReturnedFrom) {
  InProcessMultiplexer mx;
  Plain backend({{"127.0.0.1", mx.port}});
  std::thread serving([&backend] { backend.serve_forever(0.05f); });
  wait_serving(backend.serving);
  RawPeer producer(mx.port, peers::TEST_EVENT_CLIENT, PRODUCER);
  send_batch(producer, 1, 3);
  std::vector<std::uint64_t> processed, failed;
  confirmations_until(producer, 3, &processed, &failed);
  backend.leave = true;
  serving.join();
  EXPECT_EQ((std::vector<std::uint64_t>{1, 3}), processed);
  EXPECT_EQ(range(2, 1), failed);
}

class Threaded : public multiplexer::backend::BaseThreadedMultiplexerServer {
 public:
  explicit Threaded(const multiplexer::backend::MultiplexerAddresses& addresses)
      : BaseThreadedMultiplexerServer(addresses, peers::TEST_EVENT_BACKEND, options()) {}
  ~Threaded() override { close(); }
  std::atomic<bool> serving{false};
  std::atomic<bool> leave{false};
  std::mutex mutex;
  std::vector<multiplexer::backend::RequestPtr> kept;

 protected:
  static Options options() {
    Options options;
    options.workers = 2;
    return options;
  }
  void handle_message(const multiplexer::backend::RequestPtr& request) override {
    request->no_response();
    if (request->mxmsg().message() == "event 1") {
      throw std::runtime_error("this one fails");
    }
    if (request->mxmsg().message() == "event 2") {
      std::lock_guard<std::mutex> lock(mutex);
      kept.push_back(request);  // confirmed when let go of
    }
  }
  bool on_handler_exception(const std::exception&) override { return true; }
  void periodic_task() override {
    serving = true;
    if (leave.load()) {
      working = false;
    }
  }
};

TEST(ConfirmedSends, BaseThreadedMultiplexerServerConfirmsWhenTheRequestGoes) {
  InProcessMultiplexer mx;
  Threaded backend({{"127.0.0.1", mx.port}});
  std::thread serving([&backend] { backend.serve_forever(0.05f); });
  wait_serving(backend.serving);
  RawPeer producer(mx.port, peers::TEST_EVENT_CLIENT, PRODUCER);
  send_batch(producer, 1, 4);
  std::vector<std::uint64_t> processed, failed;
  confirmations_until(producer, 3, &processed, &failed);
  EXPECT_EQ((std::vector<std::uint64_t>{1, 4}), processed) << "every one but the request kept";
  EXPECT_EQ(range(2, 1), failed);
  {
    std::lock_guard<std::mutex> lock(backend.mutex);
    ASSERT_EQ(1u, backend.kept.size());
    backend.kept.clear();  // its message confirmed as it goes
  }
  confirmations_until(producer, 4, &processed, &failed);
  EXPECT_EQ((std::vector<std::uint64_t>{1, 3, 4}), processed);
  backend.leave = true;
  serving.join();
}

// A type confirmed and not batched: each message goes at once, in an
// envelope of its own that numbers it, and is kept until confirmed.
TEST(ConfirmedSends, WithoutBatchEachMessageGoesAloneAndIsConfirmed) {
  InProcessMultiplexer mx;
  SyncClient producer(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(producer.connect("127.0.0.1", mx.port, BOUND));
  RawPeer consumer(mx.port, peers::TEST_EVENT_BACKEND, CONSUMER);
  SyncClient::Delivery delivery;
  delivery.confirm = true;
  delivery.retry_after = NEVER;  // a confirmation late by seconds is no resend here
  delivery.timeout = -1;
  producer.set_delivery(types::TEST_EVENT_ANY, delivery);
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 3; ++number) {
    MultiplexerMessage msg = sync_event(producer, number);
    sent.push_back(msg.id());
    producer.queue(msg);
  }
  EXPECT_EQ(0u, producer.batched_messages()) << "none waits in a batch";
  EXPECT_EQ(3u, producer.unconfirmed());
  std::atomic<bool> alone{true};
  std::thread far_side([&] {
    for (std::uint64_t number = 1; number <= 3; ++number) {
      Batch batch;
      if (!next_batch(consumer, &batch) || batch.messages.size() != 1 || batch.numbers != range(number, 1) ||
          batch.ids()[0] != sent[number - 1]) {
        alone = false;
        return;
      }
      confirm(consumer, producer.instance_id(), {number, 1});
    }
  });
  EXPECT_TRUE(producer.flush_all(BOUND));
  far_side.join();
  EXPECT_TRUE(alone) << "each message in an envelope of its own, numbered, in order";
  EXPECT_EQ(0u, producer.unconfirmed());
  EXPECT_EQ(0u, producer.sent_again());
}

// A ThreadedClient, whose on_message runs as messages arrive, confirms
// what one read brought in one CONFIRMATION: twenty confirmed batches of
// one message each, from a producer that does not batch, which a
// multiplexer the test plays writes in one write, where each batch cost a
// CONFIRMATION of its own.
TEST(ConfirmedSends, WhatOneReadBroughtIsConfirmedTogether) {
  const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in address = {};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socklen_t length = sizeof address;
  ASSERT_EQ(0, ::bind(listener, reinterpret_cast<sockaddr*>(&address), length));
  ASSERT_EQ(0, ::listen(listener, 1));
  ASSERT_EQ(0, ::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length));
  std::atomic<int> handled{0};
  ThreadedClient consumer(peers::TEST_EVENT_BACKEND, [&handled](const IncomingMessage&) { ++handled; });
  consumer.connect("127.0.0.1", ntohs(address.sin_port), 0);  // goes on as the test answers it
  const int multiplexer = ::accept(listener, nullptr, nullptr);
  ASSERT_GE(multiplexer, 0);
  timeval bound = {BOUND, 0};
  ::setsockopt(multiplexer, SOL_SOCKET, SO_RCVTIMEO, &bound, sizeof bound);
  // A frame the consumer wrote, but heartbeats: false when none came.
  const auto next = [multiplexer](MultiplexerMessage* msg) {
    for (;;) {
      RawMessage frame;
      if (!read_exactly(multiplexer, frame.get_header_buffer().data(), frame.get_header_buffer().size()) ||
          !frame.unpack_header() ||
          !read_exactly(multiplexer, frame.get_body_buffer().data(), frame.get_body_buffer().size()) ||
          !msg->ParseFromString(frame.get_message())) {
        return false;
      }
      if (msg->type() != types::HEARTBIT) {
        return true;
      }
    }
  };
  MultiplexerMessage theirs;
  ASSERT_TRUE(next(&theirs));
  ASSERT_EQ(types::CONNECTION_WELCOME, theirs.type());
  // The welcome and the twenty batches, in one write.
  std::string bytes = wire_bytes(*multiplexer::impl::create_welcome_message(peers::MULTIPLEXER, 0x5119));
  const int count = 20;
  for (int index = 0; index < count; ++index) {
    MultiplexerMessage msg;
    msg.set_id(0x2000 + static_cast<std::uint64_t>(index));
    msg.set_sender(PRODUCER);
    msg.set_type(types::TEST_EVENT_ANY);
    msg.set_message("event " + std::to_string(index));
    MessageBatch inside;
    inside.add_message(msg.SerializeAsString());
    MultiplexerMessage envelope;
    envelope.set_id(0xe1e1e1e100000000ULL + static_cast<std::uint64_t>(index));
    envelope.set_sender(PRODUCER);
    envelope.set_type(types::TEST_EVENT_ANY);
    inside.SerializeToString(envelope.mutable_batch());
    envelope.add_confirm(static_cast<std::uint64_t>(index) + 1);
    envelope.add_confirm(1);
    std::unique_ptr<RawMessage> frame(RawMessage::FromMessage(envelope));
    bytes += wire_bytes(*frame);
  }
  ASSERT_EQ(static_cast<ssize_t>(bytes.size()), ::write(multiplexer, bytes.data(), bytes.size()));
  std::vector<std::uint64_t> numbers;
  int confirmations = 0;
  while (numbers.size() < static_cast<std::size_t>(count)) {
    MultiplexerMessage msg;
    ASSERT_TRUE(next(&msg)) << "confirmed " << numbers.size();
    if (msg.type() != types::CONFIRMATION) {
      continue;
    }
    Confirmation confirmation;
    ASSERT_TRUE(confirmation.ParseFromString(msg.message()));
    EXPECT_EQ(PRODUCER, msg.to());
    ++confirmations;
    for (std::uint64_t number : numbers_of(confirmation.processed())) {
      numbers.push_back(number);
    }
  }
  EXPECT_EQ(range(1, count), numbers);
  EXPECT_EQ(1, confirmations) << "one for what one read brought";
  EXPECT_EQ(count, handled.load());
  ::close(multiplexer);  // first: a shutdown would wait a while for an end that never closes
  consumer.shutdown(0);
  ::close(listener);
}

// Producer and consumer libraries together, the consumer a SyncClient.
TEST(ConfirmedSends, TheLibrariesAgree) {
  InProcessMultiplexer mx;
  SyncClient consumer(peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(consumer.connect("127.0.0.1", mx.port, BOUND));
  Heard heard;
  ThreadedClient producer(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(producer.connect("127.0.0.1", mx.port, BOUND));
  producer.set_delivery(types::TEST_EVENT_ANY, confirmed(25));
  const int count = 100;
  for (int number = 0; number < count; ++number) {
    producer.send(event(producer, number), multiplexer::LanePtr(), heard.end());
  }
  for (int number = 0; number < count; ++number) {
    std::shared_ptr<MultiplexerMessage> msg = consumer.receive_message(BOUND).first;
    EXPECT_EQ("event " + std::to_string(number), msg->message());
    consumer.processed(*msg);
  }
  EXPECT_THROW(consumer.receive_message(0), SyncClient::OperationTimedOut);  // has nothing more in hand
  EXPECT_TRUE(producer.flush_all(BOUND));
  EXPECT_EQ(std::vector<unsigned int>(count, 1), heard.ends_once(count));
  EXPECT_EQ(0u, producer.unconfirmed());
  EXPECT_EQ(0u, producer.sent_again());
}

}  // namespace
