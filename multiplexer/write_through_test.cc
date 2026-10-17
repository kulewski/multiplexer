// Writing through, a connection shared between event loops as a
// multiplexer on several shares them (Connection::share,
// multiplexer/io/shared_outgoing.h), on a real socket: a write that would
// block, or writes part of a frame, leaves the rest to the connection's own
// loop, which writes it, and what queues behind it, whole and in order; a
// connection shut down between another loop's claim of the writing and its
// write, or under its own loop's write, loses those frames and counts them
// lost; one closed once flushed writes everything queued first, and once
// another loop's writer has emptied its queue it is gone for every loop's
// routing before its own loop closes it; writers on several threads keep
// each one's order, every frame whole. Counted, not
// timed: the own loop is a thread the test hands work to and waits for, the
// peer a socket the test reads, and a frame of LARGE bytes cannot fit the
// socket buffers the test sets: the connection's send buffer small, the
// peer's receive buffer a few times loopback's segment, so that TCP never
// waits for a window too small to send a segment into.
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <asio/executor_work_guard.hpp>
#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "lib/functors.h"
#include "lib/mutex.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/io/connection.h"
#include "multiplexer/multiplexer.constants.h"

namespace multiplexer {

class WriteThroughManager;

// A multiplexer's traits, its queue entries bare frames, and its
// connections shared between loops when asked.
template <>
struct ConnectionsManagerTraits<WriteThroughManager> : public DefaultConnectionsManagerTraits {
  static constexpr bool WRITES_THROUGH = true;
  struct MessagesBufferTraits : public DefaultConnectionsManagerTraits::MessagesBufferTraits {
    typedef std::shared_ptr<const RawMessage> value_type;
    typedef mx::ReferencingFunctor<value_type> ToRawMessagePointerConverter;
    typedef mx::ReferencingFunctor<value_type> ToBufferRepresentationConverter;
    typedef mx::ConstructingFunctor<bool, value_type> SchedulingResultFunctor;
  };
  typedef multiplexer::Connection<WriteThroughManager> Connection;
};

// The manager of one connection: it registers the peer as ConnectionsManager
// does, says when, counts what the connection gives up, and routes nothing.
class WriteThroughManager : public ConnectionsManager<WriteThroughManager> {
 public:
  explicit WriteThroughManager(asio::io_service& io_service) : ConnectionsManager(io_service) {}
  std::shared_ptr<const RawMessage> get_welcome_message() { return nullptr; }
  void handle_message(const Connection::pointer&, const std::shared_ptr<const RawMessage>&,
                      const std::shared_ptr<MultiplexerMessage>&) {}
  void after_connection_registration(Connection::pointer, const WelcomeMessage&) { registered.set_value(); }
  void handle_orphaned_outgoing_messages(Connection*, Connection::MessagesBuffer& queue) MX_EXCLUDES(mutex_) {
    mx::MutexLock lock(mutex_);
    orphaned_ += queue.size();
    given_up_.notify_all();
  }

  // The frames the connection gave up so far.
  std::size_t orphaned() MX_EXCLUDES(mutex_) {
    mx::MutexLock lock(mutex_);
    return orphaned_;
  }
  // Whether the connection gave up `count` frames within the failure
  // detector's wait.
  bool wait_for_orphaned(std::size_t count) MX_EXCLUDES(mutex_) {
    mx::UniqueLock lock(mutex_);
    const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (orphaned_ < count) {
      if (given_up_.wait_until(lock, deadline) == std::cv_status::timeout) {
        return orphaned_ >= count;
      }
    }
    return true;
  }

  std::promise<void> registered;

 private:
  mx::Mutex mutex_;
  std::condition_variable_any given_up_;
  std::size_t orphaned_ MX_GUARDED_BY(mutex_) = 0;  // given up, on the own loop
};

}  // namespace multiplexer

namespace {

using multiplexer::MultiplexerMessage;
using multiplexer::RawMessage;
using multiplexer::WriteThroughManager;
typedef WriteThroughManager::Connection Connection;
typedef Connection::Shared::Queued Queued;

const int WAIT_MS = 30000;             // a failure detector only
const int SEND_BUFFER = 4096;          // the connection's, as the test sets it (the kernel doubles it)
const int RECEIVE_BUFFER = 256 << 10;  // the peer's, the same
const std::size_t LARGE = 4 << 20;     // more than both buffers hold together
const std::uint64_t PEER_ID = 0x5eedc0de;

// A frame of `id`, `writer` and `sequence` in its workflow, and a payload
// of `size` bytes.
std::shared_ptr<const RawMessage> frame(std::uint64_t id, std::size_t size, int writer = 0, int sequence = 0) {
  MultiplexerMessage message;
  message.set_id(id);
  message.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  message.set_workflow(std::to_string(writer) + ":" + std::to_string(sequence));
  message.set_message(std::string(size, static_cast<char>('a' + id % 26)));
  return std::shared_ptr<const RawMessage>(RawMessage::FromMessage(message));
}

// Reads exactly `size` bytes into `at`: 0, -1 at the end of the stream,
// ETIMEDOUT, or the errno.
int read_exactly(int fd, void* at, std::size_t size) {
  char* into = static_cast<char*>(at);
  while (size) {
    pollfd readable{fd, POLLIN, 0};
    const int ready = ::poll(&readable, 1, WAIT_MS);
    if (ready == 0) {
      return ETIMEDOUT;
    }
    const ssize_t got = ready < 0 ? -1 : ::recv(fd, into, size, 0);
    if (got == 0) {
      return -1;
    }
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      return errno;
    }
    into += got;
    size -= static_cast<std::size_t>(got);
  }
  return 0;
}

// The next frame on `fd`, checked and parsed: 0, -1 at the end of the
// stream, EPROTO for a frame that is not whole, ETIMEDOUT, or the errno.
int read_message(int fd, MultiplexerMessage* message) {
  RawMessage read;
  if (int error = read_exactly(fd, read.get_header_buffer().data(), read.get_header_buffer().size())) {
    return error;
  }
  if (!read.unpack_header()) {
    return EPROTO;
  }
  if (int error = read_exactly(fd, read.get_body_buffer().data(), read.get_body_buffer().size())) {
    return error;
  }
  return read.verify() && message->ParseFromString(read.get_message()) ? 0 : EPROTO;
}

// Bytes on `fd` until its end: how many, or -1 for an error first.
long read_to_the_end(int fd) {
  long total = 0;
  char buffer[65536];
  for (;;) {
    pollfd readable{fd, POLLIN, 0};
    if (::poll(&readable, 1, WAIT_MS) <= 0) {
      return -1;
    }
    const ssize_t got = ::recv(fd, buffer, sizeof buffer, 0);
    if (got == 0) {
      return total;
    }
    if (got < 0 && errno != EINTR) {
      return -1;
    }
    total += got > 0 ? got : 0;
  }
}

// One shared connection and its peer: the connection's own loop on a
// thread of its own, the test's other threads the other loops, the peer a
// plain socket that reads only when the test says, the buffers as the file
// comment says. The peer has sent its welcome, and the connection
// registered it.
struct SharedConnection {
  SharedConnection() : work(asio::make_work_guard(io_service)) {
    manager = std::make_shared<WriteThroughManager>(io_service);
    connection = Connection::Create(io_service, manager);
    connection->share();
    asio::ip::tcp::acceptor acceptor(io_service, asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    peer = ::socket(AF_INET, SOCK_STREAM, 0);
    ::setsockopt(peer, SOL_SOCKET, SO_RCVBUF, &RECEIVE_BUFFER, sizeof RECEIVE_BUFFER);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(acceptor.local_endpoint().port());
    ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
    EXPECT_EQ(0, ::connect(peer, reinterpret_cast<sockaddr*>(&address), sizeof address));
    acceptor.accept(connection->socket());
    connection->socket().set_option(asio::socket_base::send_buffer_size(SEND_BUFFER));
    loop = std::thread([this] { io_service.run(); });
    on_own_loop([this] {
      connection->bind_io_thread_to_current();
      connection->start_only_read();
    });
    const std::shared_ptr<const RawMessage> welcome =
        multiplexer::impl::create_welcome_message(multiplexer::peers::PYTHON_TEST_SERVER, PEER_ID);
    for (const asio::const_buffer& part : welcome->get_message_buffer()) {
      EXPECT_EQ(static_cast<ssize_t>(part.size()), ::send(peer, part.data(), part.size(), MSG_NOSIGNAL));
    }
    manager->registered.get_future().wait();
    connection->set_outgoing_queue_max_size(100000);  // nothing dropped for want of room
  }
  ~SharedConnection() {
    on_own_loop([this] {
      if (!connection->shuts_down()) {
        connection->shutdown();
      }
    });
    ::close(peer);
    work.reset();
    loop.join();
  }

  // Runs `work` on the connection's own loop and waits for it, after
  // whatever that loop had to run before.
  void on_own_loop(const std::function<void()>& run) {
    std::promise<void> done;
    asio::post(io_service, [&run, &done] {
      run();
      done.set_value();
    });
    done.get_future().wait();
  }

  // Queues `raw` from another loop, a thread of its own, writing it
  // through when that thread took the writing on: what the queueing said.
  Queued from_another_loop(const std::shared_ptr<const RawMessage>& raw) {
    Queued queued = Queued::FULL;
    std::thread([&] {
      queued = connection->enqueue_shared(raw, false);
      if (queued == Queued::CLAIMED) {
        connection->write_claimed();
      }
    }).join();
    return queued;
  }

  asio::io_service io_service;
  asio::executor_work_guard<asio::io_service::executor_type> work;
  std::shared_ptr<WriteThroughManager> manager;
  Connection::pointer connection;
  int peer = -1;
  std::thread loop;
};

// Another loop's write of LARGE bytes stops at the full socket and leaves
// the rest to the own loop; what another loop queues meanwhile waits
// behind it, the writing not its to take; the peer reads both whole, in
// order. Once the own loop emptied the queue, the next to queue writes.
TEST(WriteThrough, AWriteThatWouldBlockLeavesTheRestToTheOwnLoop) {
  SharedConnection shared;
  EXPECT_EQ(Queued::CLAIMED, shared.from_another_loop(frame(1, LARGE)));
  EXPECT_EQ(Queued::QUEUED, shared.from_another_loop(frame(2, 100)));
  MultiplexerMessage read;
  ASSERT_EQ(0, read_message(shared.peer, &read));
  EXPECT_EQ(1u, read.id());
  EXPECT_EQ(LARGE, read.message().size());
  ASSERT_EQ(0, read_message(shared.peer, &read));
  EXPECT_EQ(2u, read.id());
  shared.on_own_loop([] {});  // after the end of its write, which the peer read
  EXPECT_EQ(Queued::CLAIMED, shared.from_another_loop(frame(3, 100)));
  ASSERT_EQ(0, read_message(shared.peer, &read));
  EXPECT_EQ(3u, read.id());
  EXPECT_EQ(0u, shared.manager->orphaned());
}

// The connection shut down after another loop took the writing on and
// before it wrote: that loop writes nothing, and its frame is lost,
// counted once the own loop settles it.
TEST(WriteThrough, ShutDownBetweenAClaimAndItsWriteLosesTheFrame) {
  SharedConnection shared;
  Queued queued = Queued::FULL;
  std::promise<void> taken;
  std::promise<void> shut;
  std::thread writer([&] {
    queued = shared.connection->enqueue_shared(frame(1, 100), false);
    taken.set_value();
    shut.get_future().wait();
    if (queued == Queued::CLAIMED) {
      shared.connection->write_claimed();
    }
  });
  taken.get_future().wait();
  shared.on_own_loop([&shared] { shared.connection->shutdown(); });
  shut.set_value();
  writer.join();
  EXPECT_EQ(Queued::CLAIMED, queued);
  EXPECT_TRUE(shared.manager->wait_for_orphaned(1));  // once the own loop settled what the writer posted
  shared.on_own_loop([] {});
  EXPECT_EQ(1u, shared.manager->orphaned());
  EXPECT_EQ(0, read_to_the_end(shared.peer));
}

// The connection shut down while its own loop's write is under way, the
// socket full: that write ends unfinished, and its frame is lost with the
// one queued behind it; the peer gets part of the first and no more.
TEST(WriteThrough, ShutDownUnderTheOwnLoopsWriteLosesItsFrame) {
  SharedConnection shared;
  ASSERT_EQ(Queued::CLAIMED, shared.from_another_loop(frame(1, LARGE)));
  ASSERT_EQ(Queued::QUEUED, shared.from_another_loop(frame(2, 100)));
  shared.on_own_loop([&shared] { shared.connection->shutdown(); });
  EXPECT_TRUE(shared.manager->wait_for_orphaned(2));  // once the write ended, as many steps on as it took
  shared.on_own_loop([] {});
  EXPECT_EQ(2u, shared.manager->orphaned());
  const long received = read_to_the_end(shared.peer);
  EXPECT_GT(received, 0);
  EXPECT_LT(static_cast<std::size_t>(received), LARGE);
}

// Closed once flushed while other loops' frames are queued: the own loop
// writes them all first, then the connection's end of the stream; what is
// queued after that is refused, the connection ended, and nothing is lost.
TEST(WriteThrough, ClosedOnceFlushedWritesEverythingQueuedFirst) {
  SharedConnection shared;
  ASSERT_EQ(Queued::CLAIMED, shared.from_another_loop(frame(1, LARGE)));
  ASSERT_EQ(Queued::QUEUED, shared.from_another_loop(frame(2, 100)));
  shared.on_own_loop([&shared] { shared.connection->close_when_flushed(5); });
  MultiplexerMessage read;
  ASSERT_EQ(0, read_message(shared.peer, &read));
  EXPECT_EQ(1u, read.id());
  ASSERT_EQ(0, read_message(shared.peer, &read));
  EXPECT_EQ(2u, read.id());
  EXPECT_EQ(-1, read_message(shared.peer, &read));  // the end of the stream
  EXPECT_EQ(Queued::ENDED, shared.from_another_loop(frame(3, 100)));
  EXPECT_EQ(0u, shared.manager->orphaned());
}

// Closed once flushed while another loop's write through holds the
// writing: the writer that empties the queue ends the channel and, in the
// same step, the connection's life, before the own loop runs the close it
// posts there. Routed to meanwhile, from any loop, the connection is not
// living, and a frame queued to it is refused as ENDED, a peer gone, never
// as a full queue: one loop's routing never finds a closing peer, which
// closes there at once.
TEST(WriteThrough, ClosedOnceFlushedByAnotherLoopsWriterIsGoneAtOnce) {
  SharedConnection shared;
  Queued queued = Queued::FULL;
  std::promise<void> taken;
  std::promise<void> write_now;
  std::thread writer([&] {
    queued = shared.connection->enqueue_shared(frame(1, 100), false);
    taken.set_value();
    write_now.get_future().wait();
    if (queued == Queued::CLAIMED) {
      shared.connection->write_claimed();  // empties the queue, the close owed: posted to the own loop
    }
  });
  taken.get_future().wait();
  ASSERT_EQ(Queued::CLAIMED, queued);
  // The drain's close on the own loop, which then stays busy, the close
  // the writer posts waiting behind it, until the test lets it go.
  std::promise<void> holding;
  std::promise<void> release;
  asio::post(shared.io_service, [&] {
    shared.connection->close_when_flushed(5);
    holding.set_value();
    release.get_future().wait();
  });
  holding.get_future().wait();
  write_now.set_value();
  writer.join();
  const bool living = shared.connection->living();
  const Queued routed = shared.connection->enqueue_shared(frame(2, 100), false);
  release.set_value();
  shared.on_own_loop([] {});
  EXPECT_FALSE(living) << "gone for the routing once its channel ended";
  EXPECT_EQ(Queued::ENDED, routed) << "refused as a peer gone, not as a full queue";
  MultiplexerMessage read;
  ASSERT_EQ(0, read_message(shared.peer, &read));
  EXPECT_EQ(1u, read.id());
  EXPECT_EQ(-1, read_message(shared.peer, &read));  // then the end of the stream
  EXPECT_EQ(0u, shared.manager->orphaned());
}

// Several loops route to one connection at once: each one's frames arrive
// in the order it queued them, every frame whole, none lost.
TEST(WriteThrough, WritersOnSeveralThreadsKeepEachOnesOrder) {
  SharedConnection shared;
  const int WRITERS = 4;
  const int FRAMES = 2000;  // each
  std::vector<std::thread> writers;
  std::atomic<int> refused{0};
  for (int writer = 0; writer < WRITERS; ++writer) {
    writers.emplace_back([&shared, &refused, writer] {
      for (int sequence = 0; sequence < FRAMES; ++sequence) {
        const Queued queued = shared.connection->enqueue_shared(
            frame(1 + writer * FRAMES + sequence, 64 + sequence % 512, writer, sequence), false);
        if (queued == Queued::FULL || queued == Queued::ENDED) {
          refused.fetch_add(1);
        }
        if (queued == Queued::CLAIMED) {
          shared.connection->write_claimed();
        }
      }
    });
  }
  std::vector<int> next(WRITERS, 0);
  for (int count = 0; count < WRITERS * FRAMES; ++count) {
    MultiplexerMessage read;
    ASSERT_EQ(0, read_message(shared.peer, &read)) << "after " << count << " frames";
    const std::string& workflow = read.workflow();
    const int writer = std::stoi(workflow.substr(0, workflow.find(':')));
    const int sequence = std::stoi(workflow.substr(workflow.find(':') + 1));
    ASSERT_EQ(next[writer], sequence) << "writer " << writer;
    ++next[writer];
  }
  for (std::thread& writer : writers) {
    writer.join();
  }
  EXPECT_EQ(0, refused.load());
  EXPECT_EQ(0u, shared.manager->orphaned());
}

}  // namespace
