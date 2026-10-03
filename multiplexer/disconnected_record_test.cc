// A peer's DISCONNECTED is recorded after its last frames. After a write to
// it fails, the multiplexer reads on to the peer's end and routes what it
// reads; those frames are recorded before the DISCONNECTED, where the
// DISCONNECTED was recorded at the failed write, before them. And a
// PEER_CONTROL among them changes nothing and records nothing, its peer
// gone from routing, where it recorded a ROUTING for the peer gone.
// Counted, not timed: the peer reads nothing, so the multiplexer's write to
// it waits, its buffers full; the multiplexer is frozen while the peer
// sends its last frames and resets, and once it runs again the failed
// write is handled before the read, as asio performs a descriptor's write
// before its read.
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
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

using multiplexer::MultiplexerMessage;
using multiplexer::PeerControl;
using multiplexer::PeerEvent;
using multiplexer::RawMessage;
using multiplexer::Record;
using multiplexer::testing::FILL_FRAMES;
using multiplexer::testing::fill_size;
using multiplexer::testing::InProcessMultiplexer;

namespace {

const std::uint64_t PEER_ID = 0x5eed000000d1500cULL;
const int LAST_FRAMES = 3;  // the messages the peer sends before it resets, a PEER_CONTROL among them
const int WAIT_MS = 30000;  // a failure detector only

// Holds a multiplexer's io thread, so that it reads nothing while the
// sockets stay open: a multiplexer frozen, until the end of the scope.
struct Freeze {
  explicit Freeze(InProcessMultiplexer& mx) {
    std::shared_future<void> gate = released.get_future().share();
    std::promise<void> frozen;
    mx.io_service.post([gate, &frozen] {
      frozen.set_value();
      gate.wait();
    });
    frozen.get_future().wait();
  }
  ~Freeze() { released.set_value(); }
  std::promise<void> released;
};

// `work` run on the multiplexer's io thread, waited for.
void on_io_thread(InProcessMultiplexer& mx, const std::function<void()>& work) {
  std::promise<void> done;
  mx.io_service.post([&] {
    work();
    done.set_value();
  });
  done.get_future().wait();
}

// The bytes of `frame`, its header and its body.
std::string frame_bytes(const RawMessage& frame) {
  std::string bytes;
  for (const asio::const_buffer& part : frame.get_message_buffer()) {
    bytes.append(static_cast<const char*>(part.data()), part.size());
  }
  return bytes;
}

// Writes `bytes` in one send(): whether all of it went. On a socket with
// TCP_NODELAY, what one send() takes leaves at once, where Nagle would hold
// a second piece back, and a reset throws away what was held.
bool write_all(int fd, const std::string& bytes) {
  return ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL) == static_cast<ssize_t>(bytes.size());
}

// Reads exactly `size` bytes within WAIT_MS: whether they came.
bool read_exactly(int fd, char* into, std::size_t size) {
  while (size) {
    pollfd readable{fd, POLLIN, 0};
    if (::poll(&readable, 1, WAIT_MS) <= 0) {
      return false;
    }
    const ssize_t got = ::recv(fd, into, size, 0);
    if (got <= 0) {
      return false;
    }
    into += got;
    size -= static_cast<std::size_t>(got);
  }
  return true;
}

}  // namespace

TEST(DisconnectedRecord, ComesAfterThePeersLastFrames) {
  InProcessMultiplexer mx;
  const char* directory = std::getenv("TEST_TMPDIR");
  const std::string path = std::string(directory ? directory : "/tmp") + "/disconnected_record_test.rec";
  ::unlink(path.c_str());
  std::string error;
  bool started = false;
  on_io_thread(mx, [&] { started = mx.server->start_recording(path, "", 0, 0, 0, &error); });
  ASSERT_TRUE(started) << error;

  // The peer: welcomed, then reading nothing.
  const int peer = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(mx.port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ASSERT_EQ(0, ::connect(peer, reinterpret_cast<sockaddr*>(&address), sizeof address));
  const int no_delay = 1;
  ASSERT_EQ(0, ::setsockopt(peer, IPPROTO_TCP, TCP_NODELAY, &no_delay, sizeof no_delay));
  ASSERT_TRUE(write_all(
      peer, frame_bytes(*multiplexer::impl::create_welcome_message(multiplexer::peers::TEST_TINY_QUEUE, PEER_ID))));
  RawMessage welcome;
  ASSERT_TRUE(read_exactly(peer, static_cast<char*>(welcome.get_header_buffer().data()), RawMessage::HEADER_LENGTH));
  ASSERT_TRUE(welcome.unpack_header());
  ASSERT_TRUE(read_exactly(peer, static_cast<char*>(welcome.get_body_buffer().data()), welcome.get_body_length()));

  // A sender fills the peer's buffers, however far the kernel grew them,
  // until the multiplexer's write to it waits, and sends a few more; its
  // message to itself comes back once every one was routed.
  multiplexer::Client sender(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(sender.wait_for_connection(sender.connect("127.0.0.1", mx.port, 5), 5));
  const std::string fill(fill_size(), 'f'), chunk(64 * 1024, 'x');
  for (int index = 0; index < FILL_FRAMES + 4; ++index) {
    MultiplexerMessage msg;
    msg.set_id(sender.random64());
    msg.set_sender(sender.instance_id());
    msg.set_to(PEER_ID);
    msg.set_type(multiplexer::types::TEST_EVENT);
    msg.set_message(index < FILL_FRAMES ? fill : chunk);
    sender.queue(msg);
  }
  MultiplexerMessage marker;
  marker.set_id(sender.random64());
  marker.set_sender(sender.instance_id());
  marker.set_to(sender.instance_id());
  marker.set_type(multiplexer::types::TEST_EVENT);
  marker.set_message("marker");
  sender.queue(marker);
  bool marked = false;
  while (!marked) {
    marked = sender.receive_message(WAIT_MS / 1000.0f).first->message() == "marker";
  }

  // The peer's last frames and its reset, while the multiplexer is frozen.
  std::string last_bytes;
  for (int index = 0; index < LAST_FRAMES; ++index) {
    MultiplexerMessage last;
    last.set_id(1000 + index);
    last.set_sender(PEER_ID);
    last.set_type(multiplexer::types::TEST_UNROUTED);
    last.set_message("last");
    last_bytes += frame_bytes(*std::unique_ptr<RawMessage>(RawMessage::FromMessage(last)));
    if (index == 0) {
      PeerControl control;
      control.mutable_routing()->set_any(false);  // a change: the welcome took every path
      MultiplexerMessage routing;
      routing.set_id(2000);
      routing.set_sender(PEER_ID);
      routing.set_type(multiplexer::types::PEER_CONTROL);
      control.SerializeToString(routing.mutable_message());
      last_bytes += frame_bytes(*std::unique_ptr<RawMessage>(RawMessage::FromMessage(routing)));
    }
  }
  {
    Freeze frozen(mx);
    ASSERT_TRUE(write_all(peer, last_bytes));
    const linger at_once{1, 0};
    ::setsockopt(peer, SOL_SOCKET, SO_LINGER, &at_once, sizeof at_once);
    ::close(peer);
  }
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_MS);
  std::size_t accepted = 2;
  while (accepted > 1 && std::chrono::steady_clock::now() < deadline) {
    on_io_thread(mx, [&] { accepted = mx.server->accepted_count(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(1u, accepted) << "the peer's connection closed, the sender's left";
  on_io_thread(mx, [&] { mx.server->stop_recording("done"); });
  sender.shutdown();

  mx::protobuf::FileMessageInputStream recorded(::open(path.c_str(), O_RDONLY), true);
  Record record;
  int index = 0, disconnected = -1, routing_records = 0;
  std::vector<int> last_frames;
  while (recorded.read(record)) {
    if (record.has_routed() && record.routed().sender() == PEER_ID &&
        record.routed().type() == multiplexer::types::TEST_UNROUTED) {
      last_frames.push_back(index);
    }
    if (record.has_peer() && record.peer().peer_id() == PEER_ID && record.peer().kind() == PeerEvent::DISCONNECTED) {
      disconnected = index;
    }
    if (record.has_peer() && record.peer().peer_id() == PEER_ID && record.peer().kind() == PeerEvent::ROUTING) {
      ++routing_records;
    }
    ++index;
  }
  ASSERT_EQ(LAST_FRAMES, static_cast<int>(last_frames.size())) << "the last frames were routed";
  ASSERT_GE(disconnected, 0);
  EXPECT_GT(disconnected, last_frames.back()) << "DISCONNECTED after the peer's last frames";
  EXPECT_EQ(0, routing_records) << "the PEER_CONTROL read on changed nothing";
}
