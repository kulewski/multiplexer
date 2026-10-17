// A client's batching (BasicClient::set_batching): the events queued of a
// batched type go several in one message, full batches as they fill and
// the rest at a flush; the receiving library hands each out as it was
// sent, with a frame of its own; the keys of a type batch apart, and one
// key's setting goes ahead of its type's; a send that does not batch lets
// the batch go first; a batch's message is measured and kept within
// max_bytes; with no delay, a message goes alone while the connection has
// nothing to write and those sent while it is busy go together once it is
// not, or once it dies, or once another connection comes up; a batch given
// up on is told for each message inside; the settings are checked; and
// what does not belong in a batch that arrives is dropped. Counted, not
// timed: a batch closes here by its count, by a flush or by the
// connection's write, the delays set far longer than any test; a raw peer
// of the receiving type reads what the multiplexer forwards as it went on
// the wire; a multiplexer that reads nothing keeps a connection busy for
// as long as a test needs; every wait is bounded only to detect a failure.
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <asio/io_service.hpp>
#include <asio/post.hpp>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "lib/seconds.h"
#include "multiplexer/Multiplexer.pb.h"
#include "multiplexer/client.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/io/raw_message.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

namespace {

using multiplexer::IncomingMessage;
using multiplexer::MessageBatch;
using multiplexer::MultiplexerMessage;
using multiplexer::RawMessage;
using multiplexer::SyncClient;
using multiplexer::ThreadedClient;
using multiplexer::testing::InProcessMultiplexer;
namespace peers = multiplexer::peers;
namespace types = multiplexer::types;

const int BOUND = 30;               // seconds a wait may take before the test calls it stuck: a failure detector only
const float NEVER = 3600;           // a max_delay no test lasts: its batches close by count, a flush or a write
const std::uint64_t PEER = 0xba7c;  // the raw peers' instance ids start here

// The bytes of `frame` as it goes on the wire, header and body.
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

// A peer that speaks the wire format by hand, to see the frames the
// multiplexer forwards as they went: connected and welcomed as `peer_type`.
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
  std::uint64_t id() const { return id_; }

 private:
  int fd_;
  std::uint64_t id_;
};

// A multiplexer that welcomes one client and then reads nothing, its
// receive buffer the kernel's least: what the client writes fills the
// sockets between them and stays in flight, the connection busy for as
// long as the test needs; die() resets the connection.
class SilentMultiplexer {
 public:
  SilentMultiplexer() {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
    const int least = 1;  // an accepted socket inherits it
    ::setsockopt(listener_, SOL_SOCKET, SO_RCVBUF, &least, sizeof least);
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof address;
    if (::bind(listener_, reinterpret_cast<sockaddr*>(&address), length) != 0 || ::listen(listener_, 1) != 0 ||
        ::getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
      ::close(listener_);
      throw std::runtime_error("no socket to listen on");
    }
    port = ntohs(address.sin_port);
    // The handshake, as the client connects: its welcome read, ours sent.
    handshake_ = std::thread([this] {
      fd_ = ::accept(listener_, nullptr, nullptr);
      if (fd_ < 0) {
        return;
      }
      timeval bound = {BOUND, 0};
      ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &bound, sizeof bound);
      RawMessage welcome;
      if (read_exactly(fd_, welcome.get_header_buffer().data(), welcome.get_header_buffer().size()) &&
          welcome.unpack_header() &&
          read_exactly(fd_, welcome.get_body_buffer().data(), welcome.get_body_buffer().size())) {
        const std::string ours = wire_bytes(*multiplexer::impl::create_welcome_message(peers::MULTIPLEXER, 0x5117));
        if (::write(fd_, ours.data(), ours.size()) != static_cast<ssize_t>(ours.size())) {
          ADD_FAILURE() << "the silent multiplexer's welcome was not written";
        }
      }
    });
  }
  ~SilentMultiplexer() {
    ::shutdown(listener_, SHUT_RDWR);  // an accept still waiting returns
    die();
    ::close(listener_);
  }
  // From now on reads what the client writes, whole frames, as a
  // multiplexer that came back to life would, until a message of `type`:
  // false when none came, BOUND seconds at most for each read.
  bool read_until(std::uint32_t type, MultiplexerMessage* msg) {
    if (handshake_.joinable()) {
      handshake_.join();
    }
    for (;;) {
      RawMessage frame;
      if (fd_ < 0 || !read_exactly(fd_, frame.get_header_buffer().data(), frame.get_header_buffer().size()) ||
          !frame.unpack_header() ||
          !read_exactly(fd_, frame.get_body_buffer().data(), frame.get_body_buffer().size()) ||
          !msg->ParseFromString(frame.get_message())) {
        return false;
      }
      if (msg->type() == type) {
        return true;
      }
    }
  }
  // The connection is reset, what the client wrote and this end never
  // read thrown away.
  void die() {
    if (handshake_.joinable()) {
      handshake_.join();
    }
    if (fd_ >= 0) {
      const linger reset = {1, 0};
      ::setsockopt(fd_, SOL_SOCKET, SO_LINGER, &reset, sizeof reset);
      ::close(fd_);
      fd_ = -1;
    }
  }

  unsigned short port = 0;

 private:
  int listener_ = -1;
  int fd_ = -1;  // the handshake's thread sets it, read once that thread is joined
  std::thread handshake_;
};

// Fills `sender`'s one connection, to a SilentMultiplexer, through a pinned
// lane, lost with the connection rather than handed over or held: frames
// that fill the sockets, and as many more as the connection's queue holds,
// so that it has no room and messages wait for it.
void fill(ThreadedClient& sender) {
  const multiplexer::LanePtr pinned = std::make_shared<multiplexer::Lane>(true);
  const std::string large(multiplexer::testing::fill_size(), 'f');
  for (int frame = 0; frame < multiplexer::testing::FILL_FRAMES; ++frame) {
    sender.send(sender.new_message(types::TEST_UNROUTED, large), pinned);
  }
  for (unsigned int frame = 0;
       frame <= multiplexer::ConnectionsManager<multiplexer::BasicClient>::DEFAULT_OUT_QUEUE_SIZE; ++frame) {
    sender.send(sender.new_message(types::TEST_UNROUTED, "f"), pinned);
  }
}

// An event of TEST_EVENT, `number` in its payload and workflow, `key` when
// given, its id fresh from `client`.
MultiplexerMessage event(SyncClient& client, int number, const std::string* key = nullptr) {
  MultiplexerMessage msg;
  msg.set_id(client.random64());
  msg.set_sender(client.instance_id());
  msg.set_type(types::TEST_EVENT);
  msg.set_message("event " + std::to_string(number));
  msg.set_workflow("flow " + std::to_string(number));
  if (key) {
    msg.set_key(*key);
  }
  return msg;
}

// What one frame the receiver read carried: the messages inside a batch,
// or the message itself.
struct Frame {
  bool batch = false;
  std::size_t bytes = 0;  // the frame's body, as it went on the wire
  bool keyed = false;
  std::string key;
  std::vector<MultiplexerMessage> messages;
};

// The frames `receiver` reads until they hold `count` messages.
std::vector<Frame> frames_until(RawPeer& receiver, std::size_t count) {
  std::vector<Frame> frames;
  std::size_t messages = 0;
  while (messages < count) {
    MultiplexerMessage msg;
    if (!receiver.next(&msg)) {
      ADD_FAILURE() << "the receiver read " << messages << " message(s) of " << count;
      break;
    }
    Frame frame;
    frame.batch = msg.has_batch();
    frame.bytes = msg.ByteSizeLong();
    frame.keyed = msg.has_key();
    frame.key = msg.key();
    if (frame.batch) {
      MessageBatch batch;
      EXPECT_TRUE(batch.ParseFromString(msg.batch()));
      for (const std::string& body : batch.message()) {
        MultiplexerMessage inside;
        EXPECT_TRUE(inside.ParseFromString(body));
        frame.messages.push_back(inside);
      }
    } else {
      frame.messages.push_back(msg);
    }
    messages += frame.messages.size();
    frames.push_back(frame);
  }
  return frames;
}

// The batching that closes by count or by flush, never by time here.
SyncClient::Batching by_count(unsigned int records) {
  SyncClient::Batching batching;
  batching.max_records = records;
  batching.max_delay = NEVER;
  return batching;
}

TEST(Batching, QueuedEventsGoInBatchesAndAFlushSendsTheRest) {
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  SyncClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  sender.set_batching(types::TEST_EVENT, by_count(10));
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 25; ++number) {
    MultiplexerMessage msg = event(sender, number);
    sent.push_back(msg.id());
    ASSERT_TRUE(sender.queue(msg));
  }
  EXPECT_EQ(5u, sender.batched_messages()) << "two full batches went, five wait";
  ASSERT_TRUE(sender.flush_all(BOUND));
  EXPECT_EQ(0u, sender.batched_messages());
  std::vector<Frame> frames = frames_until(receiver, 25);
  std::vector<std::size_t> sizes;
  std::vector<std::uint64_t> received;
  for (const Frame& frame : frames) {
    EXPECT_TRUE(frame.batch);
    EXPECT_FALSE(frame.keyed);
    sizes.push_back(frame.messages.size());
    for (const MultiplexerMessage& msg : frame.messages) {
      received.push_back(msg.id());
      EXPECT_EQ(types::TEST_EVENT, msg.type());
      EXPECT_EQ(sender.instance_id(), msg.sender());
    }
  }
  EXPECT_EQ((std::vector<std::size_t>{10, 10, 5}), sizes);
  EXPECT_EQ(sent, received);
}

TEST(Batching, AReceiverGetsEachMessageAsItWasSent) {
  InProcessMultiplexer mx;
  SyncClient receiver(peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(receiver.connect("127.0.0.1", mx.port, BOUND));
  SyncClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  sender.set_batching(types::TEST_EVENT, by_count(7));
  std::vector<MultiplexerMessage> sent;
  for (int number = 0; number < 20; ++number) {
    sent.push_back(event(sender, number));
    ASSERT_TRUE(sender.queue(sent.back()));
  }
  ASSERT_TRUE(sender.flush_all(BOUND));
  for (const MultiplexerMessage& expected : sent) {
    IncomingMessage incoming = receiver.read_raw_message(BOUND);
    const MultiplexerMessage& msg = *incoming.third;
    EXPECT_EQ(expected.id(), msg.id());
    EXPECT_EQ(expected.message(), msg.message());
    EXPECT_EQ(expected.workflow(), msg.workflow());
    EXPECT_EQ(expected.sender(), msg.sender());
    EXPECT_FALSE(msg.has_batch());
    // The frame of its own a reader of bytes, the Python client, parses.
    MultiplexerMessage from_bytes;
    ASSERT_TRUE(from_bytes.ParseFromString(incoming.first->get_message()));
    EXPECT_EQ(expected.id(), from_bytes.id());
    EXPECT_EQ(expected.message(), from_bytes.message());
  }
}

TEST(Batching, TheKeysOfATypeBatchApart) {
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  SyncClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  sender.set_batching(types::TEST_EVENT, by_count(5));
  const std::string a = "a", b = "b";
  std::vector<std::uint64_t> sent_a, sent_b;
  for (int number = 0; number < 10; ++number) {
    MultiplexerMessage first = event(sender, number, &a), second = event(sender, number, &b);
    sent_a.push_back(first.id());
    sent_b.push_back(second.id());
    ASSERT_TRUE(sender.queue(first));
    ASSERT_TRUE(sender.queue(second));
  }
  ASSERT_TRUE(sender.flush_all(BOUND));
  std::vector<std::uint64_t> received_a, received_b;
  for (const Frame& frame : frames_until(receiver, 20)) {
    EXPECT_TRUE(frame.batch);
    EXPECT_TRUE(frame.keyed);
    EXPECT_EQ(5u, frame.messages.size());
    for (const MultiplexerMessage& msg : frame.messages) {
      EXPECT_EQ(frame.key, msg.key()) << "a batch holds one key";
      (msg.key() == a ? received_a : received_b).push_back(msg.id());
    }
  }
  EXPECT_EQ(sent_a, received_a);
  EXPECT_EQ(sent_b, received_b);
}

TEST(Batching, AKeysOwnSettingGoesAheadOfItsTypesAndOtherKeysGoAlone) {
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  SyncClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  const std::string a = "a", b = "b";
  sender.set_batching(types::TEST_EVENT, a, by_count(2));
  // a0 and a1 fill a batch; a2 waits until b0, which does not batch, lets it go first.
  std::vector<std::uint64_t> sent;
  for (const std::pair<const std::string*, int>& each :
       std::vector<std::pair<const std::string*, int>>{{&a, 0}, {&a, 1}, {&a, 2}, {&b, 0}, {&b, 1}}) {
    MultiplexerMessage msg = event(sender, each.second, each.first);
    sent.push_back(msg.id());
    ASSERT_TRUE(sender.queue(msg));
  }
  ASSERT_TRUE(sender.flush_all(BOUND));
  std::vector<Frame> frames = frames_until(receiver, 5);
  ASSERT_EQ(4u, frames.size());
  EXPECT_TRUE(frames[0].batch);
  EXPECT_EQ(2u, frames[0].messages.size());
  for (std::size_t index = 1; index < frames.size(); ++index) {
    EXPECT_FALSE(frames[index].batch) << "a batch of one goes as the message itself; b is not batched";
  }
  std::vector<std::uint64_t> received;
  for (const Frame& frame : frames) {
    for (const MultiplexerMessage& msg : frame.messages) {
      received.push_back(msg.id());
    }
  }
  EXPECT_EQ(sent, received);
}

TEST(Batching, ASendThatDoesNotBatchLetsTheBatchGoFirst) {
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  SyncClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  sender.set_batching(types::TEST_EVENT, by_count(100));
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 3; ++number) {
    MultiplexerMessage msg = event(sender, number);
    sent.push_back(msg.id());
    ASSERT_TRUE(sender.queue(msg));
  }
  EXPECT_EQ(3u, sender.batched_messages());
  MultiplexerMessage flushed = event(sender, 3);
  sender.send(flushed, BOUND);  // a flushing send does not batch
  std::vector<Frame> frames = frames_until(receiver, 4);
  ASSERT_EQ(2u, frames.size());
  EXPECT_TRUE(frames[0].batch);
  ASSERT_EQ(3u, frames[0].messages.size());
  for (std::size_t index = 0; index < 3; ++index) {
    EXPECT_EQ(sent[index], frames[0].messages[index].id());
  }
  EXPECT_FALSE(frames[1].batch);
  EXPECT_EQ(flushed.id(), frames[1].messages[0].id());
}

// A batching client makes the frame of a plain event without its checksum
// (RawMessage::Unsealed): one that goes in a batch never gets one, only its
// body read; one that goes alone, by a send that does not batch or as a
// batch of one, gets it as it goes, and the multiplexer, which checks every
// frame's checksum before it routes it, delivers it.
TEST(Batching, AMessageInABatchGetsNoChecksumOfItsOwn) {
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  SyncClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  sender.set_batching(types::TEST_EVENT, by_count(100));
  const auto frame_of = [&sender](int number) {
    return multiplexer::frame_stamped(
        event(sender, number), sender.instance_id(), [&sender] { return sender.random64(); }, /*batching=*/true);
  };
  std::vector<std::shared_ptr<const RawMessage>> batched;
  std::vector<std::string> bodies;
  for (int number = 0; number < 3; ++number) {
    batched.push_back(frame_of(number));
    bodies.push_back(batched.back()->get_message());
    ASSERT_TRUE(sender.queue(batched.back()));
  }
  const std::shared_ptr<const RawMessage> scheduled = frame_of(3);
  sender.flush(sender.schedule_one(scheduled), BOUND);  // it does not batch: the batch goes first
  const std::shared_ptr<const RawMessage> one = frame_of(4);
  ASSERT_TRUE(sender.queue(one));
  ASSERT_TRUE(sender.flush_all(BOUND));  // a batch of one goes as the message itself
  const std::vector<Frame> frames = frames_until(receiver, 5);
  ASSERT_EQ(3u, frames.size());
  EXPECT_TRUE(frames[0].batch);
  EXPECT_EQ(3u, frames[0].messages.size());
  EXPECT_FALSE(frames[1].batch);
  EXPECT_FALSE(frames[2].batch);
  for (std::size_t index = 0; index < batched.size(); ++index) {
    EXPECT_NE(RawMessage::WRITING, batched[index]->usability()) << "a message in a batch had its own checksum computed";
    // Held here too, its body was copied into the batch, not moved out
    // (RawMessage::TakeBody).
    EXPECT_EQ(bodies[index], batched[index]->get_message());
  }
  EXPECT_EQ(RawMessage::WRITING, scheduled->usability());
  EXPECT_EQ(RawMessage::WRITING, one->usability());
}

// A synchronous client's queue() runs its loop first only where the message
// chooses a connection or something is due (BasicClient::send's
// poll_first): one that only joins a batch with a delay leaves the loop
// alone, a system call spared, while the one that fills its batch runs it
// before the batch chooses a connection, as a message sent alone does, and
// so does the first queue() after a batch's delay is over, whose timer only
// the loop runs. The loop is the test's own, so a handler posted to it says
// whether it ran.
TEST(Batching, AQueuedMessageThatOnlyJoinsABatchRunsNoLoop) {
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  const std::shared_ptr<asio::io_service> loop = std::make_shared<asio::io_service>();
  SyncClient sender(loop, peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  sender.set_batching(types::TEST_EVENT, by_count(3));
  int ran = 0;
  const auto post = [&] { asio::post(*loop, [&ran] { ++ran; }); };
  post();
  ASSERT_TRUE(sender.queue(event(sender, 0)));
  ASSERT_TRUE(sender.queue(event(sender, 1)));
  EXPECT_EQ(0, ran) << "a message that only joined its batch ran the loop";
  ASSERT_TRUE(sender.queue(event(sender, 2)));  // fills the batch, which goes
  EXPECT_EQ(1, ran);
  post();
  MultiplexerMessage alone = event(sender, 3);
  alone.set_type(types::PING);  // not batched: it goes alone, as any send did
  ASSERT_TRUE(sender.queue(alone));
  EXPECT_EQ(2, ran);
  std::vector<Frame> frames = frames_until(receiver, 3);
  ASSERT_EQ(1u, frames.size());
  EXPECT_EQ(3u, frames[0].messages.size());

  // A delay over: the next queue() runs the loop, whose timer lets the
  // batch go. The wait only needs to be longer than the delay.
  SyncClient::Batching timed;
  timed.max_delay = 0.001f;
  sender.set_batching(types::TEST_EVENT, timed);
  post();
  ASSERT_TRUE(sender.queue(event(sender, 4)));
  EXPECT_EQ(2, ran);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  ASSERT_TRUE(sender.queue(event(sender, 5)));
  EXPECT_EQ(3, ran);
  frames = frames_until(receiver, 1);
  ASSERT_EQ(1u, frames.size());
  EXPECT_EQ(sender.instance_id(), frames[0].messages[0].sender());
}

// frame_stamped() appends what a message lacks, the id and the sender, to
// its own bytes rather than setting them in a copy: the frame reads as the
// message with them, its other fields as they were, and a message that
// lacks nothing is framed as it is.
TEST(Batching, AFrameIsStampedWithWhatItsMessageLacks) {
  MultiplexerMessage msg;
  msg.set_type(types::TEST_EVENT);
  msg.set_message(std::string(1000, 'p'));
  msg.set_key("k");
  msg.set_workflow("w");
  std::uint64_t fresh = 41;
  const auto stamped = [&](const MultiplexerMessage& message) {
    const std::shared_ptr<const RawMessage> raw =
        multiplexer::frame_stamped(message, 77, [&fresh] { return ++fresh; }, /*batching=*/true);
    MultiplexerMessage parsed;
    EXPECT_TRUE(parsed.ParseFromString(raw->get_message()));
    EXPECT_EQ(parsed.id(), raw->id());
    EXPECT_EQ(parsed.type(), raw->type());
    EXPECT_EQ(RawMessage::Batchable::YES, raw->batchable());
    EXPECT_EQ("k", raw->batch_key());
    return parsed;
  };
  MultiplexerMessage expected = msg;
  expected.set_id(42);
  expected.set_sender(77);
  EXPECT_EQ(expected.SerializeAsString(), stamped(msg).SerializeAsString());
  MultiplexerMessage with_id = msg;
  with_id.set_id(7);
  expected.set_id(7);
  EXPECT_EQ(expected.SerializeAsString(), stamped(with_id).SerializeAsString());
  MultiplexerMessage with_sender = msg;
  with_sender.set_sender(9);
  expected.set_id(43);
  expected.set_sender(9);
  EXPECT_EQ(expected.SerializeAsString(), stamped(with_sender).SerializeAsString());
  EXPECT_EQ(43u, fresh);
  MultiplexerMessage whole = msg;
  whole.set_id(5);
  whole.set_sender(6);
  EXPECT_EQ(whole.SerializeAsString(),
            multiplexer::frame_stamped(whole, 77, [] { return std::uint64_t(1); })->get_message());
}

TEST(Batching, ABatchsMessageStaysWithinMaxBytes) {
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  SyncClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  SyncClient::Batching batching = by_count(1000);
  batching.max_bytes = 1000;
  sender.set_batching(types::TEST_EVENT, batching);
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 50; ++number) {
    MultiplexerMessage msg = event(sender, number);
    msg.set_message(std::string(100, 'p'));
    sent.push_back(msg.id());
    ASSERT_TRUE(sender.queue(msg));
  }
  ASSERT_TRUE(sender.flush_all(BOUND));
  std::vector<std::uint64_t> received;
  std::vector<Frame> frames = frames_until(receiver, 50);
  for (const Frame& frame : frames) {
    EXPECT_LE(frame.bytes, 1000u) << "measured as it went on the wire";
    for (const MultiplexerMessage& msg : frame.messages) {
      received.push_back(msg.id());
    }
  }
  EXPECT_LE(frames.size(), 10u) << "several in each";
  EXPECT_EQ(sent, received);
}

// A message cut from a batch whose message would be over max_bytes goes on
// whole. Each of these is 196 bytes: five fit in 1,000, so the sixth lets
// the batch go and every message in it had its body moved into it, its
// frame held by nothing else (RawMessage::TakeBody); the envelope adds
// three bytes for each and its own fields, over 1,000, so the last is cut
// and must get its body back to go next.
TEST(Batching, AMessageCutFromABatchGoesOnWhole) {
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  SyncClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  SyncClient::Batching batching = by_count(1000);
  batching.max_bytes = 1000;
  sender.set_batching(types::TEST_EVENT, batching);
  std::vector<MultiplexerMessage> sent;
  for (int number = 0; number < 12; ++number) {
    MultiplexerMessage msg = event(sender, number);
    for (std::size_t payload = 150; msg.ByteSizeLong() < 196; ++payload) {
      msg.set_message(std::string(payload, static_cast<char>('a' + number)));
    }
    ASSERT_EQ(196u, msg.ByteSizeLong());
    sent.push_back(msg);
    ASSERT_TRUE(sender.queue(msg));
  }
  ASSERT_TRUE(sender.flush_all(BOUND));
  std::vector<MultiplexerMessage> received;
  std::vector<std::size_t> sizes;
  for (const Frame& frame : frames_until(receiver, sent.size())) {
    EXPECT_LE(frame.bytes, 1000u);
    sizes.push_back(frame.messages.size());
    received.insert(received.end(), frame.messages.begin(), frame.messages.end());
  }
  ASSERT_EQ(sent.size(), received.size());
  for (std::size_t index = 0; index < sent.size(); ++index) {
    EXPECT_EQ(sent[index].SerializeAsString(), received[index].SerializeAsString()) << "message " << index;
  }
  EXPECT_EQ(4u, sizes[0]) << "the fifth cut from the first batch";
}

// With no delay, the first message goes alone, the connection having
// nothing to write, and the next nine, sent while its write is under way,
// go together once it is done. The sender's io thread is held in a
// callback while the ten sends are posted behind it, so that they all run
// before the first write's handler does; a heartbeat written just then
// would make the first wait too, so the count says at most two frames.
TEST(Batching, WithNoDelayMessagesSentWhileTheConnectionIsBusyGoTogether) {
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  std::promise<void> entered, release;
  std::shared_future<void> released = release.get_future().share();
  bool held = false;
  ThreadedClient sender(peers::TEST_ACTIVE_CLIENT, [&](const IncomingMessage&) {
    if (!held) {
      held = true;
      entered.set_value();
      released.wait();
    }
  });
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  ThreadedClient::Batching batching;
  batching.max_records = 100;
  batching.max_delay = 0;
  sender.set_batching(types::TEST_EVENT, batching);
  SyncClient poke(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(poke.connect("127.0.0.1", mx.port, BOUND));
  MultiplexerMessage hold = event(poke, 0);
  hold.set_to(sender.instance_id());
  poke.send(hold, BOUND);
  ASSERT_EQ(std::future_status::ready, entered.get_future().wait_for(std::chrono::seconds(BOUND)));
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 10; ++number) {
    MultiplexerMessage msg = sender.new_message(types::TEST_EVENT, "event " + std::to_string(number));
    sent.push_back(msg.id());
    sender.send(msg);
  }
  release.set_value();
  std::vector<Frame> frames = frames_until(receiver, 10);
  ASSERT_LE(frames.size(), 2u);
  EXPECT_TRUE(frames.back().batch);
  EXPECT_GE(frames.back().messages.size(), 9u);
  std::vector<std::uint64_t> received;
  for (const Frame& frame : frames) {
    for (const MultiplexerMessage& msg : frame.messages) {
      received.push_back(msg.id());
    }
  }
  EXPECT_EQ(sent, received);
  sender.shutdown();
}

TEST(Batching, WithNoDelayABatchGoesWhenTheBusyConnectionDies) {
  // Nothing would ever be written on the busy connection again: once it
  // dies, the batch that waited for it goes, held for the next connection,
  // the only message held, what filled the connection lost with it.
  SilentMultiplexer silent;
  std::promise<void> told;  // the first drop: the connection's handover is over
  bool first = true;
  ThreadedClient sender(peers::TEST_ACTIVE_CLIENT);
  sender.set_drop_observer([&](std::uint64_t, multiplexer::DropReason) {
    if (first) {
      first = false;
      told.set_value();
    }
  });
  ASSERT_TRUE(sender.connect("127.0.0.1", silent.port, BOUND));
  fill(sender);
  ThreadedClient::Batching batching;
  batching.max_delay = 0;
  sender.set_batching(types::TEST_EVENT, batching);
  for (int number = 0; number < 3; ++number) {
    sender.send(sender.new_message(types::TEST_EVENT, "event " + std::to_string(number)));
  }
  ASSERT_EQ(0u, sender.held_for_a_connection());  // after the io thread placed every send before it
  silent.die();
  ASSERT_EQ(std::future_status::ready, told.get_future().wait_for(std::chrono::seconds(BOUND)));
  EXPECT_EQ(1u, sender.held_for_a_connection()) << "the batch, held for the next connection";
  sender.shutdown(0);
}

TEST(Batching, ABatchAConnectionLetGoGoesOnThatConnection) {
  // Two connections, both busy, their far ends reading nothing: the batch
  // waits. The second's end starts reading, and once that connection has
  // written everything, the batch goes on it, where the round robin would
  // have put it on the first, behind writes that never end.
  SilentMultiplexer first, second;
  ThreadedClient sender(peers::TEST_ACTIVE_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", first.port, BOUND));
  ASSERT_TRUE(sender.connect("127.0.0.1", second.port, BOUND));
  const std::string large(multiplexer::testing::fill_size(), 'f');
  for (int frame = 0; frame < 2 * multiplexer::testing::FILL_FRAMES; ++frame) {
    sender.send(sender.new_message(types::TEST_UNROUTED, large));  // each connection in turn: both full
  }
  ThreadedClient::Batching batching;
  batching.max_delay = 0;
  sender.set_batching(types::TEST_EVENT, batching);
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 3; ++number) {
    MultiplexerMessage msg = sender.new_message(types::TEST_EVENT, "event " + std::to_string(number));
    sent.push_back(msg.id());
    sender.send(msg);
  }
  ASSERT_EQ(0u, sender.held_for_a_connection());  // after the io thread placed every send before it
  MultiplexerMessage batch;
  ASSERT_TRUE(second.read_until(types::TEST_EVENT, &batch)) << "the batch went elsewhere";
  ASSERT_TRUE(batch.has_batch());
  MessageBatch inside;
  ASSERT_TRUE(inside.ParseFromString(batch.batch()));
  std::vector<std::uint64_t> received;
  for (const std::string& body : inside.message()) {
    MultiplexerMessage msg;
    ASSERT_TRUE(msg.ParseFromString(body));
    received.push_back(msg.id());
  }
  EXPECT_EQ(sent, received);
  sender.shutdown(0);
}

TEST(Batching, WithNoDelayABatchGoesWhenAConnectionComesUp) {
  // The only connection stays busy and full: the batch that waits for it
  // goes at the registration of a second, which has nothing to write.
  SilentMultiplexer silent;
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  ThreadedClient sender(peers::TEST_ACTIVE_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", silent.port, BOUND));
  fill(sender);
  ThreadedClient::Batching batching;
  batching.max_delay = 0;
  sender.set_batching(types::TEST_EVENT, batching);
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 3; ++number) {
    MultiplexerMessage msg = sender.new_message(types::TEST_EVENT, "event " + std::to_string(number));
    sent.push_back(msg.id());
    sender.send(msg);
  }
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  std::vector<Frame> frames = frames_until(receiver, sent.size());
  ASSERT_EQ(1u, frames.size());
  EXPECT_TRUE(frames[0].batch);
  std::vector<std::uint64_t> received;
  for (const MultiplexerMessage& msg : frames[0].messages) {
    received.push_back(msg.id());
  }
  EXPECT_EQ(sent, received);
  sender.shutdown(0);
}

TEST(Batching, ABatchThatGoesLetsTheBatchesWaitingForAConnectionGoFirst) {
  // Key "a" batches with no delay, key "b" by three. The connection is
  // busy writing a message that does not batch, so "a"'s batch waits for
  // it to have nothing left to write; "b"'s fills meanwhile and goes on its
  // own, and "a"'s, older, goes before it, where it waited for the
  // connection behind "b"'s and every batch after, which a busy stream
  // could hold up without end. The io thread is held while the sends are
  // posted, so that they are placed before the first write's end.
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  std::promise<void> entered, release;
  std::shared_future<void> released = release.get_future().share();
  bool held = false;
  ThreadedClient sender(peers::TEST_ACTIVE_CLIENT, [&](const IncomingMessage&) {
    if (!held) {
      held = true;
      entered.set_value();
      released.wait();
    }
  });
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  ThreadedClient::Batching no_delay;
  no_delay.max_delay = 0;
  sender.set_batching(types::TEST_EVENT, "a", no_delay);
  sender.set_batching(types::TEST_EVENT, "b", by_count(3));
  SyncClient poke(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(poke.connect("127.0.0.1", mx.port, BOUND));
  MultiplexerMessage hold = event(poke, 0);
  hold.set_to(sender.instance_id());
  poke.send(hold, BOUND);
  ASSERT_EQ(std::future_status::ready, entered.get_future().wait_for(std::chrono::seconds(BOUND)));
  sender.send(sender.new_message(types::TEST_UNROUTED, "busy"));  // does not batch: the connection busy
  MultiplexerMessage a = sender.new_message(types::TEST_EVENT, "a");
  a.set_key("a");
  sender.send(a);
  std::vector<std::uint64_t> b_ids;
  for (int number = 0; number < 3; ++number) {
    MultiplexerMessage b = sender.new_message(types::TEST_EVENT, "b" + std::to_string(number));
    b.set_key("b");
    b_ids.push_back(b.id());
    sender.send(b);
  }
  release.set_value();
  std::vector<Frame> frames = frames_until(receiver, 4);
  ASSERT_EQ(2u, frames.size());
  ASSERT_EQ(1u, frames[0].messages.size());
  EXPECT_EQ(a.id(), frames[0].messages[0].id()) << "the older batch first";
  EXPECT_TRUE(frames[1].batch);
  std::vector<std::uint64_t> got;
  for (const MultiplexerMessage& msg : frames[1].messages) {
    got.push_back(msg.id());
  }
  EXPECT_EQ(b_ids, got);
  sender.shutdown();
}

TEST(Batching, WithNoDelayABatchGoesAtItsFirstDeadline) {
  // A batch with no delay waits for room: its messages' timeouts run, and
  // it goes when the first is up, at the latest, as a message that waited
  // for room on a full connection would. The connection's far end reads
  // nothing and its queue is full, so the batch is given up on then, and
  // said, where it waited for a connection that never had nothing left to
  // write.
  SilentMultiplexer silent;
  std::promise<void> told;
  std::vector<std::uint64_t> drops;
  std::mutex mutex;
  ThreadedClient sender(peers::TEST_ACTIVE_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", silent.port, BOUND));
  fill(sender);
  ThreadedClient::Batching batching;
  batching.max_delay = 0;
  sender.set_batching(types::TEST_EVENT, batching);
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 2; ++number) {
    MultiplexerMessage msg = sender.new_message(types::TEST_EVENT, "event " + std::to_string(number));
    sent.push_back(msg.id());
    sender.send_serialized(msg.SerializeAsString(), msg.id(), msg.type(), multiplexer::LanePtr(), 0.2f,
                           [&](unsigned int written) {
                             std::lock_guard<std::mutex> lock(mutex);
                             drops.push_back(written);
                             if (drops.size() == 2) {
                               told.set_value();
                             }
                           });
  }
  ASSERT_EQ(std::future_status::ready, told.get_future().wait_for(std::chrono::seconds(BOUND)))
      << "the batch still waits";
  EXPECT_EQ((std::vector<std::uint64_t>{0, 0}), drops) << "given up on, each message";
  EXPECT_EQ(2u, sender.dropped());
  sender.shutdown(0);
}

TEST(Batching, ABatchGoesAtItsDelay) {
  // No flush and no other send: the batch goes once its delay is over.
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  ThreadedClient sender(peers::TEST_ACTIVE_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  ThreadedClient::Batching batching;
  batching.max_delay = 0.05f;
  sender.set_batching(types::TEST_EVENT, batching);
  std::vector<std::uint64_t> sent;
  for (int number = 0; number < 3; ++number) {
    MultiplexerMessage msg = sender.new_message(types::TEST_EVENT, "event " + std::to_string(number));
    sent.push_back(msg.id());
    sender.send(msg);
  }
  std::vector<Frame> frames = frames_until(receiver, 3);
  ASSERT_EQ(1u, frames.size());
  EXPECT_TRUE(frames[0].batch);
  std::vector<std::uint64_t> got;
  for (const MultiplexerMessage& msg : frames[0].messages) {
    got.push_back(msg.id());
  }
  EXPECT_EQ(sent, got);
  sender.shutdown();
}

TEST(Batching, ASendWithNoTimeToWaitGoesAlone) {
  // A timeout of 0 is a send that waits for nothing: in a batch it would
  // leave the whole batch no time, so it goes alone, the batch first.
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  SyncClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  sender.set_batching(types::TEST_EVENT, by_count(100));
  MultiplexerMessage first = event(sender, 1), now = event(sender, 2), last = event(sender, 3);
  sender.queue(first);
  sender.queue(now, 0);
  sender.queue(last);
  ASSERT_TRUE(sender.flush_all(BOUND));
  std::vector<Frame> frames = frames_until(receiver, 3);
  ASSERT_EQ(3u, frames.size());
  EXPECT_EQ(first.id(), frames[0].messages[0].id());
  EXPECT_EQ(now.id(), frames[1].messages[0].id());
  EXPECT_FALSE(frames[1].batch);
  EXPECT_EQ(last.id(), frames[2].messages[0].id());
}

TEST(Batching, BytesOfAnotherTypeOrWithTooLongAKeyGoAlone) {
  // A frame made of bytes the program gave is read before it may batch:
  // one whose message is of another type than the frame says, or has a key
  // over MAX_KEY_SIZE, goes alone, as without batching, where the second
  // made the batch's own message fail and lost the first.
  asio::io_service io_service;
  SyncClient sender(io_service, peers::TEST_EVENT_CLIENT);
  sender.set_batching(types::TEST_EVENT, by_count(100));
  const std::string long_key(multiplexer::MAX_KEY_SIZE + 1, 'k');
  for (int number = 0; number < 2; ++number) {
    MultiplexerMessage keyed = event(sender, number, &long_key);
    std::shared_ptr<const RawMessage> frame(new RawMessage(keyed.SerializeAsString(), keyed.id(), keyed.type()));
    EXPECT_NO_THROW(sender.queue(frame, BOUND));
    MultiplexerMessage other = event(sender, number);
    other.set_type(types::TEST_EVENT_ANY);
    frame.reset(new RawMessage(other.SerializeAsString(), other.id(), types::TEST_EVENT));
    EXPECT_NO_THROW(sender.queue(frame, BOUND));
  }
  EXPECT_EQ(0u, sender.batched_messages());
  EXPECT_EQ(4u, sender.held_for_a_connection()) << "each alone, held: nothing connected";
}

TEST(Batching, ABatchGivenUpOnIsToldForEachMessage) {
  // Nothing connected: the full batch is held for a connection, one
  // message that holds three, and the flush says not all was written; the
  // shutdown gives up on it, and the drop observer, the count and each
  // message's tracker and `done` hear of each message.
  asio::io_service io_service;
  SyncClient sender(io_service, peers::TEST_EVENT_CLIENT);  // never connected: nothing can take it
  std::vector<std::pair<std::uint64_t, multiplexer::DropReason>> told;
  sender.set_drop_observer(
      [&told](std::uint64_t id, multiplexer::DropReason reason) { told.emplace_back(id, reason); });
  sender.set_batching(types::TEST_EVENT, by_count(3));
  std::vector<std::uint64_t> sent;
  std::vector<SyncClient::ScheduledMessageTracker> trackers;
  std::vector<unsigned int> heard;
  for (int number = 0; number < 3; ++number) {
    MultiplexerMessage msg = event(sender, number);
    sent.push_back(msg.id());
    trackers.push_back(
        sender.queue(msg, BOUND, multiplexer::LanePtr(), [&heard](unsigned int written) { heard.push_back(written); }));
  }
  EXPECT_EQ(1u, sender.held_for_a_connection()) << "one batch, held";
  EXPECT_FALSE(sender.flush_all(0)) << "held, nothing connected";
  EXPECT_TRUE(told.empty());
  sender.shutdown(0);
  io_service.poll();  // whatever pass is left to tell drops and end sends
  ASSERT_EQ(3u, told.size());
  for (std::size_t index = 0; index < 3; ++index) {
    EXPECT_EQ(sent[index], told[index].first);
    EXPECT_EQ(multiplexer::DropReason::SHUT_DOWN, told[index].second);
    ASSERT_TRUE(trackers[index]);
    EXPECT_TRUE(trackers[index].is_lost());
  }
  EXPECT_EQ(3u, sender.dropped());
  EXPECT_EQ((std::vector<unsigned int>{0, 0, 0}), heard);
}

TEST(Batching, AMessagesTimeoutCountsFromWhenItsBatchGoes) {
  // A message's time in its batch is the batching's, not a wait for room
  // or for a connection: its timeout, a millisecond here, counts from when
  // the batch goes, so that a batch that goes after it, nothing connected,
  // is held for a connection rather than given up on at once.
  asio::io_service io_service;
  SyncClient client(io_service, peers::TEST_EVENT_CLIENT);
  client.set_batching(types::TEST_EVENT, by_count(100));
  client.queue(event(client, 0), 0.001f);
  client.queue(event(client, 1), 0.001f);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));  // past the timeout: however late it wakes, still past
  client.flush_all(0);  // lets the batch go; with nothing connected, it ends at once
  EXPECT_EQ(0u, client.dropped());
  EXPECT_EQ(1u, client.held_for_a_connection()) << "the batch, held";
}

TEST(Batching, ABatchThatWentBeforeItsDelayLeavesNoTimer) {
  // A synchronous client's wait takes an armed timer for work: one left
  // armed for a batch already gone kept its waits busy until the delay was
  // over, an hour here.
  asio::io_service io_service;
  SyncClient client(io_service, peers::TEST_EVENT_CLIENT);
  client.set_batching(types::TEST_EVENT, by_count(2));
  client.queue(event(client, 0), BOUND);
  EXPECT_TRUE(client.batch_timer_armed()) << "armed for the delay";
  client.queue(event(client, 1), BOUND);  // full: gone, held for a connection
  EXPECT_FALSE(client.batch_timer_armed()) << "stopped with the last batch that had a delay";
  client.queue(event(client, 2), BOUND);
  client.flush_all(0);  // nothing connected: the flush lets the batch go, held, and ends
  EXPECT_FALSE(client.batch_timer_armed()) << "and at a flush";
}

TEST(Batching, ADeliveryErrorForABatchIsToldForEachMessage) {
  // Nobody takes TEST_EVENT: the multiplexer sends the batch back whole in
  // its DELIVERY_ERROR, and the sender's library tells each message of its
  // own failure, as the multiplexer would have for the message alone, where
  // the program heard of a batch it never sent. A DELIVERY_ERROR without
  // its original, left out for its size, goes on as it is.
  InProcessMultiplexer mx;
  SyncClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  sender.set_batching(types::TEST_EVENT, by_count(3));
  std::vector<MultiplexerMessage> sent;
  for (int number = 0; number < 3; ++number) {
    sent.push_back(event(sender, number));
    sender.queue(sent.back());  // the third fills the batch: it goes
  }
  for (const MultiplexerMessage& msg : sent) {
    IncomingMessage got = sender.read_raw_message(BOUND);
    const MultiplexerMessage& error = *got.third;
    ASSERT_EQ(types::DELIVERY_ERROR, error.type());
    EXPECT_EQ(msg.id(), error.references());
    EXPECT_EQ(msg.workflow(), error.workflow());
    multiplexer::DeliveryError report;
    ASSERT_TRUE(report.ParseFromString(error.message()));
    EXPECT_EQ(msg.id(), report.packet_id());
    ASSERT_EQ(1, report.failed_type_size());
    EXPECT_EQ(peers::TEST_EVENT_BACKEND, report.failed_type(0));
    EXPECT_EQ(msg.id(), report.original_message().id());
    EXPECT_EQ(msg.message(), report.original_message().message());
    MultiplexerMessage read;  // what Python reads: the frame's bytes
    ASSERT_TRUE(read.ParseFromString(got.first->get_message()));
    EXPECT_EQ(error.id(), read.id());
  }
  RawPeer reporter(mx.port, peers::TEST_EVENT_CLIENT, PEER + 9);
  multiplexer::DeliveryError omitted;
  omitted.set_packet_id(77);
  omitted.set_original_message_omitted(true);
  MultiplexerMessage told;
  told.set_id(4242);
  told.set_sender(reporter.id());
  told.set_to(sender.instance_id());
  told.set_type(types::DELIVERY_ERROR);
  told.set_references(77);
  omitted.SerializeToString(told.mutable_message());
  ASSERT_TRUE(reporter.write(*std::unique_ptr<RawMessage>(RawMessage::FromMessage(told))));
  EXPECT_EQ(4242u, sender.read_raw_message(BOUND).third->id());
}

TEST(Batching, TheSettingsAreChecked) {
  asio::io_service io_service;
  SyncClient client(io_service, peers::TEST_EVENT_CLIENT);
  SyncClient::Batching batching;
  EXPECT_THROW(client.set_batching(types::PING, batching), std::invalid_argument);
  EXPECT_THROW(client.set_batching(types::TEST_EVENT, std::string(multiplexer::MAX_KEY_SIZE + 1, 'k'), batching),
               std::invalid_argument);
  for (unsigned int records : {0u, multiplexer::MAX_BATCH_RECORDS + 1}) {
    SyncClient::Batching wrong;
    wrong.max_records = records;
    EXPECT_THROW(client.set_batching(types::TEST_EVENT, wrong), std::invalid_argument);
  }
  for (std::size_t bytes : {std::size_t(0), std::size_t(multiplexer::MAX_MESSAGE_SIZE) + 1}) {
    SyncClient::Batching wrong;
    wrong.max_bytes = bytes;
    EXPECT_THROW(client.set_batching(types::TEST_EVENT, wrong), std::invalid_argument);
  }
  for (float delay :
       {-1.0f, static_cast<float>(2 * mx::MAX_SECONDS), std::numeric_limits<float>::infinity(), std::nanf("")}) {
    SyncClient::Batching wrong;
    wrong.max_delay = delay;
    EXPECT_THROW(client.set_batching(types::TEST_EVENT, wrong), std::invalid_argument);
  }
  SyncClient::Batching longest;
  longest.max_records = multiplexer::MAX_BATCH_RECORDS;
  longest.max_delay = static_cast<float>(mx::MAX_SECONDS);
  EXPECT_NO_THROW(client.set_batching(types::TEST_EVENT, longest));
  EXPECT_NO_THROW(client.set_batching(types::TEST_EVENT, batching));
  EXPECT_NO_THROW(client.clear_batching(types::TEST_EVENT));
}

// A batch from a peer that is no library: the receiver takes the plain
// events of the batch's type and key in it and drops the rest, a message of
// another type, one addressed with `to`, one without an id.
TEST(Batching, WhatDoesNotBelongInABatchThatArrivesIsDropped) {
  InProcessMultiplexer mx;
  SyncClient receiver(peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(receiver.connect("127.0.0.1", mx.port, BOUND));
  RawPeer sender(mx.port, peers::TEST_EVENT_CLIENT, PEER + 1);
  MultiplexerMessage good;
  good.set_id(1001);
  good.set_sender(sender.id());
  good.set_type(types::TEST_EVENT);
  good.set_message("good");
  MultiplexerMessage other_type = good, addressed = good, no_id = good, also_good = good;
  other_type.set_id(1002);
  other_type.set_type(types::TEST_EVENT_ANY);
  addressed.set_id(1003);
  addressed.set_to(receiver.instance_id());
  no_id.clear_id();
  also_good.set_id(1005);
  MessageBatch batch;
  for (const MultiplexerMessage* msg : {&good, &other_type, &addressed, &no_id, &also_good}) {
    batch.add_message(msg->SerializeAsString());
  }
  MultiplexerMessage envelope;
  envelope.set_id(1000);
  envelope.set_sender(sender.id());
  envelope.set_type(types::TEST_EVENT);
  batch.SerializeToString(envelope.mutable_batch());
  std::unique_ptr<RawMessage> frame(RawMessage::FromMessage(envelope));
  ASSERT_TRUE(sender.write(*frame));
  EXPECT_EQ(1001u, receiver.read_raw_message(BOUND).third->id());
  EXPECT_EQ(1005u, receiver.read_raw_message(BOUND).third->id());
  // One message more than MAX_BATCH_RECORDS: the batch is dropped whole,
  // and the message after it comes next.
  MessageBatch over;
  for (unsigned int number = 0; number <= multiplexer::MAX_BATCH_RECORDS; ++number) {
    good.set_id(2000 + number);
    over.add_message(good.SerializeAsString());
  }
  envelope.set_id(1999);
  over.SerializeToString(envelope.mutable_batch());
  ASSERT_TRUE(sender.write(*std::unique_ptr<RawMessage>(RawMessage::FromMessage(envelope))));
  good.set_id(3000);
  ASSERT_TRUE(sender.write(*std::unique_ptr<RawMessage>(RawMessage::FromMessage(good))));
  EXPECT_EQ(3000u, receiver.read_raw_message(BOUND).third->id());
  // A message seen before is dropped, inside a batch as alone: 3000 again,
  // and twice 3001 in one batch, then a marker.
  MessageBatch repeats;
  for (std::uint64_t id : {3000u, 3001u, 3001u}) {
    good.set_id(id);
    repeats.add_message(good.SerializeAsString());
  }
  envelope.set_id(2999);
  repeats.SerializeToString(envelope.mutable_batch());
  ASSERT_TRUE(sender.write(*std::unique_ptr<RawMessage>(RawMessage::FromMessage(envelope))));
  good.set_id(3002);
  ASSERT_TRUE(sender.write(*std::unique_ptr<RawMessage>(RawMessage::FromMessage(good))));
  EXPECT_EQ(3001u, receiver.read_raw_message(BOUND).third->id());
  EXPECT_EQ(3002u, receiver.read_raw_message(BOUND).third->id());
}

TEST(Batching, AMessageOutOfABatchCanBeSentOn) {
  // A relay sends on, as it is, what it received: a message that came in a
  // batch is framed then, where its connection refused to write it and
  // wrote nothing more.
  InProcessMultiplexer mx;
  RawPeer wire(mx.port, peers::TEST_EVENT_BACKEND, PEER);
  SyncClient relay(peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(relay.connect("127.0.0.1", mx.port, BOUND));
  SyncClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, BOUND));
  sender.set_batching(types::TEST_EVENT, by_count(2));
  MultiplexerMessage first = event(sender, 1), second = event(sender, 2);
  sender.queue(first);
  sender.queue(second);
  ASSERT_TRUE(sender.flush_all(BOUND));
  ASSERT_EQ(1u, frames_until(wire, 2).size()) << "one batch on the wire";
  IncomingMessage got = relay.read_raw_message(BOUND);
  ASSERT_EQ(first.id(), got.third->id());
  SyncClient::ScheduledMessageTracker onward = relay.queue(got.first);
  relay.flush(onward, BOUND);
  EXPECT_FALSE(onward.in_queue()) << "written";
  MultiplexerMessage resent;
  ASSERT_TRUE(wire.next(&resent));
  EXPECT_EQ(first.id(), resent.id()) << "sent on, as it was";
  EXPECT_EQ(first.message(), resent.message());
}

}  // namespace
