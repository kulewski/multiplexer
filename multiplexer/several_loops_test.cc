// A multiplexer on several event loops (Server::set_threads) behaves as on
// one: events from senders on every loop reach receivers on every loop,
// each sender's in order, none lost; a newcomer that takes a live
// connection's instance id from another loop has the old connection's
// departure recorded before its own arrival, as at one loop, and gets what
// is addressed to the id, and the old connection's frames its own loop
// still held are dropped; a peer closing once flushed, its queue emptied
// by another loop's write, is gone for every loop's routing before its own
// loop closes it, recorded as reaching nobody rather than as a full queue;
// a stop ends every connection on every loop before the main loop's run()
// returns, its callback run on the main loop. Counted, not timed: the
// receivers count what they got, the recording is read back, the stop's
// callback says what it saw, and where the order of two loops' steps
// matters, one of them is a connection of the test's own, on a loop the
// test runs one handler at a time.
#include <arpa/inet.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "lib/protobuf/stream.h"
#include "multiplexer/client.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/server.h"

using multiplexer::MultiplexerMessage;
using multiplexer::PeerEvent;
using multiplexer::RawMessage;
using multiplexer::Record;
using multiplexer::RoutedMessage;
using multiplexer::testing::InProcessMultiplexer;
typedef multiplexer::Server::Connection::Shared::Queued Queued;

namespace {

const std::uint64_t CLAIMED_ID = 0x5eed10095eed1009ULL;  // the id the newcomer takes over
const std::uint64_t RECEIVER_ID = 0x5eedfeed5eedfeedULL;
const std::uint64_t CLOSING_ID = 0x5eedc105e5eedc10ULL;
const int WAIT_MS = 30000;  // a failure detector only

// A TCP connection to the multiplexer on `port`: the descriptor, or -1.
int connect_to(unsigned short port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in remote{};
  remote.sin_family = AF_INET;
  remote.sin_port = htons(port);
  ::inet_pton(AF_INET, "127.0.0.1", &remote.sin_addr);
  if (fd < 0 || ::connect(fd, reinterpret_cast<sockaddr*>(&remote), sizeof remote)) {
    if (fd >= 0) {
      ::close(fd);
    }
    return -1;
  }
  return fd;
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

// The next frame on `fd` that is not a heartbeat, parsed: 0, -1 at the end
// of the stream, ETIMEDOUT, or the errno.
int read_message(int fd, MultiplexerMessage* message) {
  for (;;) {
    RawMessage frame;
    if (int error = read_exactly(fd, frame.get_header_buffer().data(), frame.get_header_buffer().size())) {
      return error;
    }
    if (!frame.unpack_header()) {
      return EPROTO;
    }
    if (int error = read_exactly(fd, frame.get_body_buffer().data(), frame.get_body_buffer().size())) {
      return error;
    }
    if (!frame.verify() || !message->ParseFromString(frame.get_message())) {
      return EPROTO;
    }
    if (message->type() != multiplexer::types::HEARTBIT) {
      return 0;
    }
  }
}

// A peer of `id` on a connection of its own, its welcome sent and the
// multiplexer's read: the descriptor, or -1.
int welcomed_peer(unsigned short port, std::uint64_t id) {
  const int fd = connect_to(port);
  if (fd < 0) {
    return -1;
  }
  const std::shared_ptr<const RawMessage> welcome =
      multiplexer::impl::create_welcome_message(multiplexer::peers::PYTHON_TEST_SERVER, id);
  for (const asio::const_buffer& part : welcome->get_message_buffer()) {
    if (::send(fd, part.data(), part.size(), MSG_NOSIGNAL) != static_cast<ssize_t>(part.size())) {
      ::close(fd);
      return -1;
    }
  }
  MultiplexerMessage answer;
  if (read_message(fd, &answer) || answer.type() != multiplexer::types::CONNECTION_WELCOME) {
    ::close(fd);
    return -1;
  }
  return fd;
}

// `work` run on the multiplexer's main loop, waited for: at one loop the
// loop the multiplexer's state belongs to.
void on_main_loop(InProcessMultiplexer& mx, const std::function<void()>& work) {
  std::promise<void> done;
  mx.io_service.post([&] {
    work();
    done.set_value();
  });
  done.get_future().wait();
}

// Where a test's recording goes.
std::string recording_path(const std::string& name) {
  const char* directory = std::getenv("TEST_TMPDIR");
  return std::string(directory ? directory : "/tmp") + "/" + name + ".rec";
}

// Writes `frame`, its header and its body, to `fd`: 0 or the errno.
int send_frame(int fd, const RawMessage& frame) {
  for (const asio::const_buffer& part : frame.get_message_buffer()) {
    if (::send(fd, part.data(), part.size(), MSG_NOSIGNAL) != static_cast<ssize_t>(part.size())) {
      return errno;
    }
  }
  return 0;
}

// A frame `id` from the peer of `sender` addressed to `to`.
std::shared_ptr<const RawMessage> addressed(std::uint64_t id, std::uint64_t sender, std::uint64_t to) {
  MultiplexerMessage message;
  message.set_id(id);
  message.set_sender(sender);
  message.set_to(to);
  message.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  message.set_message("frame " + std::to_string(id));
  return std::shared_ptr<const RawMessage>(RawMessage::FromMessage(message));
}

// Whether the socket `fd` came to hold `bytes` unread within the failure
// detector's wait: what was written to its far end is all there.
bool holds(int fd, std::size_t bytes) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_MS);
  for (;;) {
    int unread = 0;
    if (::ioctl(fd, FIONREAD, &unread) == 0 && static_cast<std::size_t>(unread) >= bytes) {
      return true;
    }
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
    std::this_thread::yield();
  }
}

// The dispositions the recording at `path` gave the message `id`, in order.
std::vector<RoutedMessage::Disposition> dispositions(const std::string& path, std::uint64_t id) {
  mx::protobuf::FileMessageInputStream recorded(::open(path.c_str(), O_RDONLY), true);
  Record record;
  std::vector<RoutedMessage::Disposition> found;
  while (recorded.read(record)) {
    if (record.has_routed() && record.routed().id() == id) {
      found.push_back(record.routed().disposition());
    }
  }
  return found;
}

// A connection to the multiplexer that the test makes itself, on a loop of
// the test's own that the test's thread runs one handler at a time: the
// multiplexer its manager and the connection shared between loops, as any
// it accepts on several loops is, the peer a plain socket. The test
// decides when that loop runs, so that a step of the multiplexer's own
// loops can come between two of its. Registered as `type` and `id`.
struct TestLoopPeer {
  TestLoopPeer(InProcessMultiplexer& mx, std::uint32_t type, std::uint64_t id) {
    connection = multiplexer::Server::Connection::Create(loop, mx.server);
    connection->share();
    asio::ip::tcp::acceptor acceptor(loop, asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
    peer = connect_to(acceptor.local_endpoint().port());
    acceptor.accept(connection->socket());
    connection->start_only_read();
    EXPECT_EQ(0, send_frame(peer, *multiplexer::impl::create_welcome_message(type, id)));
    loop.run_one();  // the welcome read: registered, the multiplexer's welcome written
    MultiplexerMessage answer;
    EXPECT_EQ(0, read_message(peer, &answer));
    EXPECT_EQ(multiplexer::types::CONNECTION_WELCOME, answer.type());
  }
  ~TestLoopPeer() {
    if (!connection->shuts_down()) {
      connection->shutdown();
    }
    ::close(peer);
    loop.run();  // what its end left to run
  }

  // Runs the connection's loop, a handler at a time, until it has ended.
  void run_to_its_end() {
    while (!connection->shuts_down()) {
      loop.run_one();
    }
  }

  asio::io_service loop;
  multiplexer::Server::Connection::pointer connection;
  int peer = -1;
};

class OnLoops : public ::testing::TestWithParam<unsigned int> {};

// The newcomer takes the old connection's id from the next loop, the
// connections dealt out round robin: the recording says the old one left
// before the newcomer came, as at one loop, nothing of the old one after
// it, and what is addressed to the id reaches the newcomer.
TEST_P(OnLoops, ANewcomerTakesAnIdAfterTheOldConnectionLeft) {
  InProcessMultiplexer mx(0, GetParam());
  const std::string path = recording_path("newcomer_" + std::to_string(GetParam()));
  std::string error;
  bool recording = false;
  on_main_loop(mx, [&] { recording = mx.server->start_recording(path, "", 0, 0, 0, &error); });
  ASSERT_TRUE(recording) << error;
  const int old_peer = welcomed_peer(mx.port, CLAIMED_ID);
  ASSERT_GE(old_peer, 0);
  const int newcomer = welcomed_peer(mx.port, CLAIMED_ID);
  ASSERT_GE(newcomer, 0);
  MultiplexerMessage read;
  EXPECT_EQ(-1, read_message(old_peer, &read)) << "the old connection ended";

  multiplexer::Client sender(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(sender.wait_for_connection(sender.connect("127.0.0.1", mx.port, 5), 5));
  MultiplexerMessage marker;
  marker.set_id(sender.random64());
  marker.set_sender(sender.instance_id());
  marker.set_to(CLAIMED_ID);
  marker.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  marker.set_message("to whoever has the id");
  sender.send(marker, 5);
  ASSERT_EQ(0, read_message(newcomer, &read));
  EXPECT_EQ(marker.message(), read.message());
  sender.shutdown();
  on_main_loop(mx, [&] { mx.server->stop_recording("done"); });
  ::close(old_peer);
  ::close(newcomer);

  mx::protobuf::FileMessageInputStream recorded(::open(path.c_str(), O_RDONLY), true);
  Record record;
  std::vector<PeerEvent::Kind> events;
  while (recorded.read(record)) {
    if (record.has_peer() && record.peer().peer_id() == CLAIMED_ID) {
      events.push_back(record.peer().kind());
    }
  }
  EXPECT_EQ((std::vector<PeerEvent::Kind>{PeerEvent::CONNECTED, PeerEvent::DISCONNECTED, PeerEvent::CONNECTED}),
            events);
}

INSTANTIATE_TEST_SUITE_P(OneAndTwo, OnLoops, ::testing::Values(1u, 2u));

// Three senders and three receivers, dealt out over three loops: every
// receiver gets every event (whom: ALL), each sender's in the order sent.
// Each client lives on a thread of its own, the one it was made on.
TEST(SeveralLoops, RouteBetweenEveryLoopInEachSendersOrder) {
  const int PEERS = 3;
  const int EVENTS = 300;  // per sender: what each receiver gets fits its queue, read or not
  InProcessMultiplexer mx(0, 3);
  std::vector<std::thread> threads;
  std::vector<std::promise<void>> connected(PEERS);
  std::atomic<int> out_of_order{0};
  std::atomic<int> received{0};
  std::atomic<int> failed{0};
  for (int receiver = 0; receiver < PEERS; ++receiver) {
    threads.emplace_back([&, receiver] {
      multiplexer::Client client(multiplexer::peers::TEST_EVENT_BACKEND);
      const bool up = client.wait_for_connection(client.connect("127.0.0.1", mx.port, 5), 5);
      connected[receiver].set_value();
      if (!up) {
        failed.fetch_add(1);
        return;
      }
      std::vector<int> next(PEERS, 0);
      try {
        for (int count = 0; count < PEERS * EVENTS; ++count) {
          const std::shared_ptr<MultiplexerMessage> event = client.receive_message(WAIT_MS / 1000.0f).first;
          const std::string& workflow = event->workflow();
          const int sender = std::stoi(workflow.substr(0, workflow.find(':')));
          const int sequence = std::stoi(workflow.substr(workflow.find(':') + 1));
          if (next[sender] != sequence) {
            out_of_order.fetch_add(1);
          }
          next[sender] = sequence + 1;
          received.fetch_add(1);
        }
      } catch (const std::exception&) {
        failed.fetch_add(1);
      }
      client.shutdown();
    });
  }
  for (std::promise<void>& receiver : connected) {
    receiver.get_future().wait();
  }
  for (int sender = 0; sender < PEERS; ++sender) {
    threads.emplace_back([&, sender] {
      multiplexer::Client client(multiplexer::peers::TEST_EVENT_CLIENT);
      if (!client.wait_for_connection(client.connect("127.0.0.1", mx.port, 5), 5)) {
        failed.fetch_add(1);
        return;
      }
      for (int sequence = 0; sequence < EVENTS; ++sequence) {
        MultiplexerMessage event;
        event.set_id(client.random64());
        event.set_sender(client.instance_id());
        event.set_type(multiplexer::types::TEST_EVENT);
        event.set_workflow(std::to_string(sender) + ":" + std::to_string(sequence));
        event.set_message("event");
        client.queue(event);
      }
      client.flush_all(WAIT_MS / 1000.0f);
      client.shutdown();
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(0, failed.load());
  EXPECT_EQ(PEERS * PEERS * EVENTS, received.load());
  EXPECT_EQ(0, out_of_order.load());
}

// A stop ends the connections of every loop before the main loop's run()
// returns, the peers draining, and its callback runs on the main loop,
// where the multiplexer's own objects are.
TEST(SeveralLoops, StopEndsEveryLoopsConnectionsBeforeTheMainLoopReturns) {
  const int PEERS = 6;  // two on each of three loops
  asio::io_service io_service;
  multiplexer::Server::pointer server;
  std::promise<unsigned short> bound;
  std::thread::id main_loop;
  std::thread::id stopped_on;
  std::size_t accepted_when_stopped = 99;
  std::size_t accepted_after_run = 99;
  std::thread loop([&] {
    main_loop = std::this_thread::get_id();
    const char* srcdir = std::getenv("TEST_SRCDIR");
    server = multiplexer::Server::Create(io_service, "127.0.0.1", 0);
    server->set_rules_file(std::string(srcdir ? srcdir : ".") + (srcdir ? "/mx/" : "/") + "tests/testing.rules");
    std::string error;
    AssertMsg(server->load_rules(&error) == multiplexer::Server::RulesLoad::LOADED, error);
    server->set_threads(3);
    server->start();
    bound.set_value(server->local_port());
    io_service.run();
    accepted_after_run = server->accepted_count();
    server->finish_loops();
  });
  const unsigned short port = bound.get_future().get();
  std::vector<int> peers;
  for (int peer = 0; peer < PEERS; ++peer) {
    peers.push_back(welcomed_peer(port, 1000 + peer));
    ASSERT_GE(peers.back(), 0);
  }
  io_service.post([&] {
    server->stop(/*drain_seconds=*/5, [&] {
      stopped_on = std::this_thread::get_id();
      accepted_when_stopped = server->accepted_count();
    });
  });
  for (const int peer : peers) {
    MultiplexerMessage read;
    EXPECT_EQ(-1, read_message(peer, &read)) << "the multiplexer's end of the stream";
    ::close(peer);  // its own end, which the closing connection reads on to
  }
  loop.join();
  EXPECT_EQ(main_loop, stopped_on);
  EXPECT_EQ(0u, accepted_when_stopped);
  EXPECT_EQ(0u, accepted_after_run);
  server.reset();
}

// A newcomer from another loop takes the instance id of a connection whose
// own loop still holds frames it read in one go: the first was routed
// before the newcomer came, the rest are dropped, as at one loop, where the
// replacement ends the old connection at once and nothing it held is read.
// The receiver gets the newcomer's next frame, not the old connection's,
// and the recording nothing of the old connection's second frame.
TEST(SeveralLoops, AReplacedConnectionsFramesStillHeldAreDropped) {
  InProcessMultiplexer mx(0, 2);
  const std::string path = recording_path("replaced_frames");
  std::string error;
  bool recording = false;
  on_main_loop(mx, [&] { recording = mx.server->start_recording(path, "", 0, 0, 0, &error); });
  ASSERT_TRUE(recording) << error;
  const int receiver = welcomed_peer(mx.port, RECEIVER_ID);
  ASSERT_GE(receiver, 0);
  TestLoopPeer old(mx, multiplexer::peers::PYTHON_TEST_SERVER, CLAIMED_ID);
  // Two frames to the receiver, both in the old connection's socket before
  // its loop reads it, which then takes both in one read: the first routed,
  // the next one's step posted.
  ASSERT_EQ(0, send_frame(old.peer, *addressed(1, CLAIMED_ID, RECEIVER_ID)));
  ASSERT_EQ(0, send_frame(old.peer, *addressed(2, CLAIMED_ID, RECEIVER_ID)));
  ASSERT_TRUE(
      holds(old.connection->socket().native_handle(), 2 * (addressed(1, CLAIMED_ID, RECEIVER_ID)->get_header_length() +
                                                           addressed(1, CLAIMED_ID, RECEIVER_ID)->get_body_length())));
  old.loop.run_one();
  MultiplexerMessage read;
  ASSERT_EQ(0, read_message(receiver, &read));
  EXPECT_EQ(1u, read.id());
  // The newcomer replaces it from one of the multiplexer's own loops, the
  // old connection's end posted behind its next step.
  const int newcomer = welcomed_peer(mx.port, CLAIMED_ID);
  ASSERT_GE(newcomer, 0);
  old.run_to_its_end();
  ASSERT_EQ(0, send_frame(newcomer, *addressed(3, CLAIMED_ID, RECEIVER_ID)));
  ASSERT_EQ(0, read_message(receiver, &read));
  EXPECT_EQ(3u, read.id()) << "the newcomer's frame, the old connection's second dropped";
  on_main_loop(mx, [&] { mx.server->stop_recording("done"); });
  EXPECT_EQ(std::vector<RoutedMessage::Disposition>{}, dispositions(path, 2));
  ::close(receiver);
  ::close(newcomer);
}

// A peer closing once flushed, at a stop's drain say, whose queue another
// loop's write empties is gone for the routing of every loop from then on,
// as at one loop, where it closes at once: an event for its type, routed
// from another loop before the peer's own loop runs the close, is recorded
// as reaching nobody, not as refused by a full queue. The closing peer is
// the test's own, its loop run only once the event was routed.
TEST(SeveralLoops, APeerClosingOnceFlushedIsGoneForTheRouting) {
  InProcessMultiplexer mx(0, 2);
  const std::string path = recording_path("closing_peer");
  std::string error;
  bool recording = false;
  on_main_loop(mx, [&] { recording = mx.server->start_recording(path, "", 0, 0, 0, &error); });
  ASSERT_TRUE(recording) << error;
  TestLoopPeer closing(mx, multiplexer::peers::TEST_EVENT_BACKEND, CLOSING_ID);  // TEST_EVENT's one backend
  // Another loop takes the writing of a frame to it on; the drain's close,
  // on its own loop, finds the frame queued and is owed; the other loop's
  // write empties the queue, which ends the peer for the routing, and posts
  // the close to its own loop, which the test leaves be.
  const std::shared_ptr<const RawMessage> queued_first = addressed(1, CLAIMED_ID, CLOSING_ID);
  Queued queued = Queued::FULL;
  std::thread([&] { queued = closing.connection->enqueue_shared(queued_first, false); }).join();
  ASSERT_EQ(Queued::CLAIMED, queued);
  closing.connection->close_when_flushed(5);
  std::thread([&] { closing.connection->write_claimed(); }).join();
  // An event of the type, routed from another loop meanwhile, and a
  // marker its sender gets back after it.
  multiplexer::Client sender(multiplexer::peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.wait_for_connection(sender.connect("127.0.0.1", mx.port, 5), 5));
  MultiplexerMessage event;
  event.set_id(sender.random64());
  event.set_sender(sender.instance_id());
  event.set_type(multiplexer::types::TEST_EVENT);
  event.set_message("event");
  sender.send(event, 5);
  MultiplexerMessage marker;
  marker.set_id(sender.random64());
  marker.set_sender(sender.instance_id());
  marker.set_to(sender.instance_id());
  marker.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  marker.set_message("marker");
  sender.send(marker, 5);
  while (sender.receive_message(WAIT_MS / 1000.0f).first->message() != "marker") {
  }
  sender.shutdown();
  on_main_loop(mx, [&] { mx.server->stop_recording("done"); });
  EXPECT_EQ(std::vector<RoutedMessage::Disposition>{RoutedMessage::NO_RECIPIENT}, dispositions(path, event.id()));
  // The peer gets the frame queued first, and the end of the stream once
  // its own loop runs the close.
  MultiplexerMessage read;
  ASSERT_EQ(0, read_message(closing.peer, &read));
  EXPECT_EQ(1u, read.id());
  closing.loop.run_one();
  EXPECT_EQ(-1, read_message(closing.peer, &read));
}

}  // namespace
