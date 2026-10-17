// A multiplexer reads a burst of frames in a read or two, not two reads a
// frame: a peer's hundred small events written at once cost its connection
// a read of the socket for as many as the read buffer holds and the read
// that finds the socket empty, where each frame cost a read for its
// header, one for its body and, after the last, the read that found the
// socket empty. Counted by this binary's own recv(2), every thread's,
// around the burst: the peer is the test's own socket, which it writes and
// reads with write(2) and read(2), so that the multiplexer's thread is the
// only one that calls recv; the burst ends with a frame the peer addresses
// to itself, which comes back once every frame before it was handled.
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "multiplexer/Multiplexer.pb.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/io/raw_message.h"
#include "multiplexer/multiplexer.constants.h"

namespace {

std::atomic<std::size_t> recvs{0};

}  // namespace

// Every recv of the binary, asio's included, counted, then made; a
// sanitizer's own interception of the call gives way to this one.
extern "C" ssize_t recv(int fd, void* buffer, size_t size, int flags) {
  recvs.fetch_add(1, std::memory_order_relaxed);
  return static_cast<ssize_t>(::syscall(SYS_recvfrom, fd, buffer, size, flags, nullptr, nullptr));
}

namespace {

using multiplexer::MultiplexerMessage;
using multiplexer::RawMessage;
using multiplexer::testing::InProcessMultiplexer;
namespace types = multiplexer::types;
namespace peers = multiplexer::peers;

const int EVENTS = 100;
const std::uint64_t PEER_ID = 0x7e57;

// The bytes of `frame` as it goes on the wire, header and body.
std::string wire_bytes(const RawMessage& frame) {
  std::string bytes;
  for (const asio::const_buffer& part : frame.get_message_buffer()) {
    bytes.append(static_cast<const char*>(part.data()), part.size());
  }
  return bytes;
}

// `msg` as a frame's bytes.
std::string wire_bytes(const MultiplexerMessage& msg) {
  std::unique_ptr<RawMessage> frame(RawMessage::FromMessage(msg));
  return wire_bytes(*frame);
}

// Reads exactly `size` bytes into `at` with read(2): false when the stream
// ended or failed first.
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

// The next frame on `fd`, parsed into `msg`: false when there was none.
bool read_message(int fd, MultiplexerMessage* msg) {
  RawMessage frame;
  return read_exactly(fd, frame.get_header_buffer().data(), frame.get_header_buffer().size()) &&
         frame.unpack_header() && read_exactly(fd, frame.get_body_buffer().data(), frame.get_body_buffer().size()) &&
         msg->ParseFromString(frame.get_message());
}

// Writes all of `bytes` with write(2), in one call where the kernel takes them.
bool write_all(int fd, const std::string& bytes) {
  for (std::size_t written = 0; written < bytes.size();) {
    const ssize_t put = ::write(fd, bytes.data() + written, bytes.size() - written);
    if (put < 0 && errno != EINTR) {
      return false;
    }
    if (put > 0) {
      written += static_cast<std::size_t>(put);
    }
  }
  return true;
}

TEST(ReadSyscalls, ABurstOfFramesIsReadInAReadOrTwo) {
  InProcessMultiplexer mx;
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);
  sockaddr_in address = {};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(mx.port);
  ASSERT_EQ(0, ::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof address));
  ASSERT_TRUE(write_all(fd, wire_bytes(*multiplexer::impl::create_welcome_message(peers::TEST_EVENT_CLIENT, PEER_ID))));
  MultiplexerMessage answer;
  ASSERT_TRUE(read_message(fd, &answer));
  ASSERT_EQ(types::CONNECTION_WELCOME, answer.type());

  // Events nobody receives, said to nobody, which the multiplexer drops,
  // then the marker, all in one write.
  std::string burst;
  for (int index = 0; index < EVENTS; ++index) {
    MultiplexerMessage event;
    event.set_id(1000 + index);
    event.set_sender(PEER_ID);
    event.set_type(types::TEST_UNROUTED);
    event.set_report_delivery_error(false);
    event.set_message("event");
    burst += wire_bytes(event);
  }
  MultiplexerMessage marker;
  marker.set_id(999);
  marker.set_sender(PEER_ID);
  marker.set_to(PEER_ID);
  marker.set_type(types::TEST_EVENT);
  marker.set_message("marker");
  burst += wire_bytes(marker);

  const std::size_t before = recvs.load();
  ASSERT_TRUE(write_all(fd, burst));
  MultiplexerMessage back;
  do {
    ASSERT_TRUE(read_message(fd, &back)) << "the connection ended before the marker came back";
  } while (back.id() != marker.id());
  const std::size_t reads = recvs.load() - before;
  ::close(fd);
  EXPECT_LT(reads, std::size_t(EVENTS / 4)) << "reads of the socket for " << EVENTS + 1 << " frames written at once";
}

}  // namespace
