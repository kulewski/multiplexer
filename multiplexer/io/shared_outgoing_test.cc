// SharedOutgoing, the outgoing side of a connection that several event
// loops write to: who writes, in what order, what is handed to the
// connection's own loop, and how a frame under way is settled when the
// channel ends. Every write is the test's own function, written whole,
// part of it, nothing because the socket is full, or a failure, so each
// path is taken on purpose; counted, with no clock. A frame refused for a
// full queue is told apart from one refused by a channel that ended, and a
// close once flushed ends the connection's life with the channel.
// send_rest(), the sendmsg(2) behind the real writes, is checked on a
// socket pair.
#include "multiplexer/io/shared_outgoing.h"

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "lib/functors.h"
#include "multiplexer/Multiplexer.pb.h"

namespace {

using multiplexer::FORCED_FRAMES_PAST_FULL_QUEUE;
using multiplexer::MultiplexerMessage;
using multiplexer::RawMessage;

typedef std::shared_ptr<const RawMessage> Frame;
typedef multiplexer::SharedOutgoing<Frame, mx::ReferencingFunctor<Frame>> Outgoing;
typedef Outgoing::Ending Ending;
typedef Outgoing::Queued Queued;

// A frame of its own id, with `payload`.
Frame frame(std::uint64_t id, const std::string& payload = "payload") {
  MultiplexerMessage message;
  message.set_id(id);
  message.set_type(1);
  message.set_message(payload);
  return Frame(RawMessage::FromMessage(message));
}

std::size_t length(const RawMessage& frame) { return frame.get_header_length() + frame.get_body_length(); }

// Another thread's id, for a writer that is not this one.
std::thread::id another_thread() {
  std::thread::id other;
  std::thread([&other] { other = std::this_thread::get_id(); }).join();
  return other;
}

// A write of what is asked for, whole, its frame's id noted in `ids`.
struct WriteWhole {
  std::vector<std::uint64_t>* ids;
  ssize_t operator()(int, const RawMessage& frame, std::size_t offset) const {
    ids->push_back(frame.id());
    return static_cast<ssize_t>(length(frame) - offset);
  }
};

// A write into a full socket: nothing, EAGAIN.
ssize_t would_block(int, const RawMessage&, std::size_t) {
  errno = EAGAIN;
  return -1;
}

const std::thread::id SELF = std::this_thread::get_id();

TEST(SharedOutgoing, TheFirstToQueueWritesAndTheOthersQueueBehind) {
  Outgoing outgoing;
  outgoing.set_limit(10);
  EXPECT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(1), false, false, SELF));
  EXPECT_EQ(Queued::QUEUED, outgoing.enqueue(frame(2), false, false, another_thread()));
  std::vector<std::uint64_t> written;
  EXPECT_EQ(Ending::IDLE, outgoing.write_through(SELF, WriteWhole{&written}));
  EXPECT_EQ((std::vector<std::uint64_t>{1, 2}), written);
  // Free again: the next to queue writes.
  EXPECT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(3), false, false, another_thread()));
}

// A write that would block leaves the frame, and what queues behind it,
// to the own loop, from where the write stopped; a frame queued meanwhile
// waits for it, and once it emptied the queue, the next to queue writes.
TEST(SharedOutgoing, AWriteThatWouldBlockHandsTheRestToTheOwnLoop) {
  Outgoing outgoing;
  outgoing.set_limit(10);
  ASSERT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(1), false, false, SELF));
  EXPECT_EQ(Ending::HAND_OVER, outgoing.write_through(SELF, &would_block));
  EXPECT_EQ(Queued::QUEUED, outgoing.enqueue(frame(2), false, false, another_thread()));
  Frame front;
  std::size_t offset = 99;
  ASSERT_TRUE(outgoing.owner_next(&front, &offset));
  EXPECT_EQ(1u, front->id());
  EXPECT_EQ(0u, offset);
  EXPECT_EQ(Ending::MORE, outgoing.owner_written(true));
  ASSERT_TRUE(outgoing.owner_next(&front, &offset));
  EXPECT_EQ(2u, front->id());
  EXPECT_EQ(Ending::IDLE, outgoing.owner_written(true));
  EXPECT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(3), false, false, SELF));
}

// Part of a frame written: the own loop writes the rest, from there.
TEST(SharedOutgoing, PartOfAFrameWrittenLeavesTheRestFromWhereItStopped) {
  Outgoing outgoing;
  outgoing.set_limit(10);
  ASSERT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(1), false, false, SELF));
  EXPECT_EQ(Ending::HAND_OVER, outgoing.write_through(SELF, [](int, const RawMessage&, std::size_t) { return 5; }));
  Frame front;
  std::size_t offset = 0;
  ASSERT_TRUE(outgoing.owner_next(&front, &offset));
  EXPECT_EQ(5u, offset);
  EXPECT_EQ(Ending::IDLE, outgoing.owner_written(true));
  // The next frame starts at its beginning.
  ASSERT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(2), false, false, SELF));
  std::vector<std::size_t> offsets;
  outgoing.write_through(SELF, [&offsets](int, const RawMessage& written, std::size_t from) {
    offsets.push_back(from);
    return static_cast<ssize_t>(length(written) - from);
  });
  EXPECT_EQ(std::vector<std::size_t>{0}, offsets);
}

// A writer of another loop writes WRITE_THROUGH_FRAMES frames at most, and
// leaves the rest to the own loop.
TEST(SharedOutgoing, AfterSoManyFramesTheOwnLoopTakesOver) {
  Outgoing outgoing;
  outgoing.set_limit(1000);
  ASSERT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(0), false, false, SELF));
  for (unsigned int id = 1; id < Outgoing::WRITE_THROUGH_FRAMES + 2; ++id) {
    ASSERT_EQ(Queued::QUEUED, outgoing.enqueue(frame(id), false, false, SELF));
  }
  std::vector<std::uint64_t> written;
  EXPECT_EQ(Ending::HAND_OVER, outgoing.write_through(SELF, WriteWhole{&written}));
  EXPECT_EQ(Outgoing::WRITE_THROUGH_FRAMES, written.size());
  Frame front;
  std::size_t offset = 0;
  ASSERT_TRUE(outgoing.owner_next(&front, &offset));
  EXPECT_EQ(Outgoing::WRITE_THROUGH_FRAMES, front->id());
}

// A connection's own frame goes right behind the frame being written.
TEST(SharedOutgoing, AsapGoesRightBehindTheFrameBeingWritten) {
  Outgoing outgoing;
  outgoing.set_limit(10);
  ASSERT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(1), false, false, SELF));
  ASSERT_EQ(Queued::QUEUED, outgoing.enqueue(frame(2), false, false, SELF));
  ASSERT_EQ(Queued::QUEUED, outgoing.enqueue(frame(3), true, /*asap=*/true, SELF));
  std::vector<std::uint64_t> written;
  outgoing.write_through(SELF, WriteWhole{&written});
  EXPECT_EQ((std::vector<std::uint64_t>{1, 3, 2}), written);
}

// A full queue refuses a frame; a forced one passes by
// FORCED_FRAMES_PAST_FULL_QUEUE at most; full() says so for the routing.
TEST(SharedOutgoing, AFullQueueDropsButForcedFramesPassBySome) {
  Outgoing outgoing;
  outgoing.set_limit(2);
  ASSERT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(1), false, false, SELF));
  ASSERT_EQ(Queued::QUEUED, outgoing.enqueue(frame(2), false, false, SELF));
  EXPECT_TRUE(outgoing.full());
  EXPECT_EQ(Queued::FULL, outgoing.enqueue(frame(3), false, false, SELF));
  for (unsigned int forced = 0; forced < FORCED_FRAMES_PAST_FULL_QUEUE; ++forced) {
    ASSERT_EQ(Queued::QUEUED, outgoing.enqueue(frame(4 + forced), /*force=*/true, false, SELF));
  }
  EXPECT_EQ(Queued::FULL, outgoing.enqueue(frame(100), true, false, SELF));
  outgoing.set_limit(1000);
  EXPECT_FALSE(outgoing.full());
}

// The channel ends while a writer of another loop writes the front: the
// end takes the queue, the front among it, under way; the writer, back,
// says whether its write was whole, for the frame set aside.
TEST(SharedOutgoing, EndedWhileAWriteThroughWasUnderWayTheWriterSettlesIt) {
  for (const bool whole : {true, false}) {
    Outgoing outgoing;
    outgoing.set_limit(10);
    ASSERT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(1), false, false, SELF));
    ASSERT_EQ(Queued::QUEUED, outgoing.enqueue(frame(2), false, false, SELF));
    bool under_way = false;
    std::size_t taken = 0;
    const Ending ending = outgoing.write_through(SELF, [&](int, const RawMessage& written, std::size_t) {
      taken = outgoing.take(&under_way).size();  // the own loop, meanwhile
      return static_cast<ssize_t>(whole ? length(written) : 3);
    });
    EXPECT_EQ(whole ? Ending::WRITTEN : Ending::LOST, ending);
    EXPECT_TRUE(under_way);
    EXPECT_EQ(2u, taken);
    EXPECT_EQ(Queued::ENDED, outgoing.enqueue(frame(3), true, true, SELF));
  }
}

// The channel ends after a loop took the writing on and before it began:
// its frame is lost, settled by that writer, who writes nothing.
TEST(SharedOutgoing, EndedBeforeAClaimedWriteBeganLosesItsFrame) {
  Outgoing outgoing;
  outgoing.set_limit(10);
  ASSERT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(1), false, false, SELF));
  bool under_way = false;
  EXPECT_EQ(1u, outgoing.take(&under_way).size());
  EXPECT_TRUE(under_way);
  int writes = 0;
  EXPECT_EQ(Ending::LOST, outgoing.write_through(SELF, [&writes](int, const RawMessage&, std::size_t) {
    ++writes;
    return 0;
  }));
  EXPECT_EQ(0, writes);
}

// The channel ends while the own loop's write is under way: that write
// settles; one handed over but not begun has nothing to settle.
TEST(SharedOutgoing, EndedUnderTheOwnLoopsWriteThatWriteSettles) {
  Outgoing outgoing;
  outgoing.set_limit(10);
  ASSERT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(1), false, false, SELF));
  ASSERT_EQ(Ending::HAND_OVER, outgoing.write_through(SELF, &would_block));
  Frame front;
  std::size_t offset = 0;
  ASSERT_TRUE(outgoing.owner_next(&front, &offset));
  bool under_way = false;
  outgoing.take(&under_way);
  EXPECT_TRUE(under_way);
  EXPECT_EQ(Ending::WRITTEN, outgoing.owner_written(true));

  Outgoing handed;
  handed.set_limit(10);
  ASSERT_EQ(Queued::CLAIMED, handed.enqueue(frame(1), false, false, SELF));
  ASSERT_EQ(Ending::HAND_OVER, handed.write_through(SELF, &would_block));
  handed.take(&under_way);
  EXPECT_FALSE(under_way);
  EXPECT_FALSE(handed.owner_next(&front, &offset));
}

// The own loop's write that fails fails the connection's writing.
TEST(SharedOutgoing, TheOwnLoopsFailedWriteFails) {
  Outgoing outgoing;
  outgoing.set_limit(10);
  ASSERT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(1), false, false, SELF));
  ASSERT_EQ(Ending::HAND_OVER, outgoing.write_through(SELF, [](int, const RawMessage&, std::size_t) {
    errno = EPIPE;
    return -1;
  }));
  Frame front;
  std::size_t offset = 0;
  ASSERT_TRUE(outgoing.owner_next(&front, &offset));
  EXPECT_EQ(Ending::FAILED, outgoing.owner_written(false));
}

// Closing once flushed: at once when nothing is queued, the channel and
// the connection's life ended in the same step; else when a writer empties
// the queue, which ends both, the connection living until then. A frame
// queued after is refused as ENDED, not as a full queue.
TEST(SharedOutgoing, ClosesOnceFlushed) {
  std::atomic<bool> idle_living{true};
  Outgoing idle;
  idle.start(-1, &idle_living);
  idle.set_limit(10);
  EXPECT_TRUE(idle.close_when_flushed());
  EXPECT_FALSE(idle_living.load());
  EXPECT_EQ(Queued::ENDED, idle.enqueue(frame(1), true, true, SELF));

  std::atomic<bool> busy_living{true};
  Outgoing busy;
  busy.start(-1, &busy_living);
  busy.set_limit(10);
  ASSERT_EQ(Queued::CLAIMED, busy.enqueue(frame(1), false, false, SELF));
  EXPECT_FALSE(busy.close_when_flushed());
  EXPECT_TRUE(busy_living.load()) << "until what is queued is written";
  ASSERT_EQ(Queued::QUEUED, busy.enqueue(frame(2), false, false, SELF));
  std::vector<std::uint64_t> written;
  EXPECT_EQ(Ending::CLOSE, busy.write_through(SELF, WriteWhole{&written}));
  EXPECT_EQ((std::vector<std::uint64_t>{1, 2}), written);
  EXPECT_FALSE(busy_living.load()) << "ended with the channel, before the own loop closes it";
  EXPECT_EQ(Queued::ENDED, busy.enqueue(frame(3), true, true, SELF));
}

// A channel the own loop ended, the connection shut down, refuses a frame
// as ENDED, a full queue as FULL: the routing tells a peer gone from one
// with no room.
TEST(SharedOutgoing, TellsAnEndedChannelFromAFullQueue) {
  Outgoing outgoing;
  outgoing.set_limit(1);
  ASSERT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(1), false, false, SELF));
  EXPECT_EQ(Queued::FULL, outgoing.enqueue(frame(2), false, false, SELF));
  bool under_way = false;
  outgoing.take(&under_way);
  EXPECT_EQ(Queued::ENDED, outgoing.enqueue(frame(3), false, false, SELF));
  EXPECT_EQ(Queued::ENDED, outgoing.enqueue(frame(4), true, true, SELF));
}

// The heartbeat's question while frames are queued is owed back by the
// writer that empties the queue; asked of an empty queue, it is answered.
TEST(SharedOutgoing, OwesTheHeartbeatItsCallOnceTheQueueEmpties) {
  Outgoing outgoing;
  outgoing.set_limit(10);
  const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  EXPECT_TRUE(outgoing.quiet(now));
  EXPECT_EQ(now, outgoing.quiet_since());
  ASSERT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(1), false, false, SELF));
  EXPECT_FALSE(outgoing.quiet(now));
  std::vector<std::uint64_t> written;
  EXPECT_EQ(Ending::HEARTBEAT, outgoing.write_through(SELF, WriteWhole{&written}));
  EXPECT_LE(now, outgoing.quiet_since());
  // Owed once.
  ASSERT_EQ(Queued::CLAIMED, outgoing.enqueue(frame(2), false, false, SELF));
  EXPECT_EQ(Ending::IDLE, outgoing.write_through(SELF, WriteWhole{&written}));
}

// send_rest() writes what is left of a frame past its offset, header and
// body, in one sendmsg(2), and says EAGAIN when the socket has no room.
TEST(SharedOutgoing, SendRestWritesTheRestOfAFrame) {
  int sockets[2];
  ASSERT_EQ(0, ::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets));
  const Frame sent = frame(7, std::string(100, 'x'));
  std::string whole;
  for (const asio::const_buffer& buffer : sent->get_message_buffer()) {
    whole.append(static_cast<const char*>(buffer.data()), buffer.size());
  }
  const std::size_t offset = 3;
  ASSERT_EQ(static_cast<ssize_t>(whole.size() - offset), multiplexer::send_rest(sockets[0], *sent, offset));
  std::string received(whole.size() - offset, '\0');
  ASSERT_EQ(static_cast<ssize_t>(received.size()), ::read(sockets[1], &received[0], received.size()));
  EXPECT_EQ(whole.substr(offset), received);

  const int small = 4096;
  ::setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &small, sizeof small);
  ::setsockopt(sockets[1], SOL_SOCKET, SO_RCVBUF, &small, sizeof small);
  const Frame large = frame(8, std::string(1 << 20, 'y'));
  ssize_t written = multiplexer::send_rest(sockets[0], *large, 0);
  ASSERT_GT(written, 0);
  EXPECT_LT(static_cast<std::size_t>(written), length(*large));
  written = multiplexer::send_rest(sockets[0], *large, static_cast<std::size_t>(written));
  EXPECT_EQ(-1, written);
  EXPECT_EQ(EAGAIN, errno);
  ::close(sockets[0]);
  ::close(sockets[1]);
}

}  // namespace
