// What routing a message costs the multiplexer in allocations: two, the
// frame it arrived in and that frame's body, which it forwards as they are,
// where the frame's two write buffers took a list of their own, a node each,
// as it arrived, and its write copied the list again, four allocations more.
// Counted by this binary's operator new on the multiplexer's own thread, an
// in-process multiplexer on one loop, around the routing of events from one
// raw peer to another, each read off the receiver's socket before the next
// is written, so that each is routed alone; the first events warm the
// connections up and are not counted. An asio that keeps one handler's
// memory a thread, where 1.22 and later keep two, allocates a read's anew
// for every frame: one more, its own.
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <asio/detail/thread_info_base.hpp>  // ASIO_RECYCLING_ALLOCATOR_CACHE_SIZE, where asio has it
#include <asio/post.hpp>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <memory>
#include <new>
#include <string>

#include "multiplexer/Multiplexer.pb.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/io/raw_message.h"
#include "multiplexer/multiplexer.constants.h"

namespace {

thread_local bool counting = false;
thread_local std::size_t allocations = 0;

}  // namespace

// Every allocation of the binary goes through these, every form but the
// aligned ones, which keep their own pair, so that what one form allocates
// any other may free; only the allocations of the thread that counts, while
// it counts, are counted. Never inlined, any of them: GCC at -O2, inlining a
// new or a delete into its caller, sees malloc() or free() behind it and
// calls the pair a mismatch (-Wmismatched-new-delete).
namespace {
void* counted(std::size_t size) noexcept {
  if (counting) {
    ++allocations;
  }
  return std::malloc(size ? size : 1);
}
}  // namespace
__attribute__((noinline)) void* operator new(std::size_t size) {
  if (void* memory = counted(size)) {
    return memory;
  }
  throw std::bad_alloc();
}
__attribute__((noinline)) void* operator new[](std::size_t size) {
  if (void* memory = counted(size)) {
    return memory;
  }
  throw std::bad_alloc();
}
__attribute__((noinline)) void* operator new(std::size_t size, const std::nothrow_t&) noexcept { return counted(size); }
__attribute__((noinline)) void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
  return counted(size);
}
__attribute__((noinline)) void operator delete(void* memory) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete[](void* memory) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete(void* memory, const std::nothrow_t&) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete[](void* memory, const std::nothrow_t&) noexcept { std::free(memory); }

namespace {

using multiplexer::MultiplexerMessage;
using multiplexer::RawMessage;
using multiplexer::testing::InProcessMultiplexer;
namespace types = multiplexer::types;
namespace peers = multiplexer::peers;

const int BOUND = 30;      // seconds a read may take before the test calls it stuck: a failure detector only
const int WARM_UP = 20;    // events routed before the count, which fill what is kept from one to the next
const int EVENTS = 200;    // events routed while counting
const int OCCASIONAL = 8;  // allocations of the loop's own that the count may meet besides: a timer, a log line
#if defined(ASIO_RECYCLING_ALLOCATOR_CACHE_SIZE)
const unsigned int PER_EVENT = 2;  // the frame and its body
#else
const unsigned int PER_EVENT = 3;  // and the read's handler, asio 1.18 recycling only one handler's memory a thread
#endif

// The bytes of `msg` as a frame on the wire, header and body.
std::string wire_bytes(const MultiplexerMessage& msg) {
  std::unique_ptr<RawMessage> frame(RawMessage::FromMessage(msg));
  std::string bytes;
  for (const asio::const_buffer& part : frame->get_message_buffer()) {
    bytes.append(static_cast<const char*>(part.data()), part.size());
  }
  return bytes;
}

// A peer speaking the wire format by hand, welcomed as `type`.
class RawPeer {
 public:
  RawPeer(unsigned short port, std::uint32_t type, std::uint64_t id) : fd_(::socket(AF_INET, SOCK_STREAM, 0)) {
    timeval bound = {BOUND, 0};
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &bound, sizeof bound);
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    welcomed_ = ::connect(fd_, reinterpret_cast<sockaddr*>(&address), sizeof address) == 0 &&
                write(*multiplexer::impl::create_welcome_message(type, id));
    MultiplexerMessage welcome;
    welcomed_ = welcomed_ && next(&welcome) && welcome.type() == types::CONNECTION_WELCOME;
  }
  ~RawPeer() { ::close(fd_); }
  bool welcomed() const { return welcomed_; }

  bool write(const RawMessage& frame) {
    std::string bytes;
    for (const asio::const_buffer& part : frame.get_message_buffer()) {
      bytes.append(static_cast<const char*>(part.data()), part.size());
    }
    return write_bytes(bytes);
  }
  bool write_bytes(const std::string& bytes) {
    for (std::size_t written = 0; written < bytes.size();) {
      const ssize_t put = ::write(fd_, bytes.data() + written, bytes.size() - written);
      if (put < 0 && errno != EINTR) {
        return false;
      }
      if (put > 0) {
        written += static_cast<std::size_t>(put);
      }
    }
    return true;
  }
  // The next frame but a heartbeat, parsed: false when none came.
  bool next(MultiplexerMessage* msg) {
    for (;;) {
      RawMessage frame;
      if (!read_exactly(frame.get_header_buffer().data(), frame.get_header_buffer().size()) || !frame.unpack_header() ||
          !read_exactly(frame.get_body_buffer().data(), frame.get_body_buffer().size()) ||
          !msg->ParseFromString(frame.get_message())) {
        return false;
      }
      if (msg->type() != types::HEARTBIT) {
        return true;
      }
    }
  }

 private:
  bool read_exactly(void* at, std::size_t size) {
    char* into = static_cast<char*>(at);
    while (size) {
      const ssize_t got = ::read(fd_, into, size);
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

  int fd_;
  bool welcomed_ = false;
};

// Runs `call` on the multiplexer's loop and waits for it.
template <typename Call>
void on_loop(InProcessMultiplexer& mx, Call call) {
  std::promise<void> ran;
  asio::post(mx.io_service, [&] {
    call();
    ran.set_value();
  });
  ran.get_future().wait();
}

TEST(RouteAllocations, ARoutedMessageCostsItsFrameAndItsBody) {
  InProcessMultiplexer mx;
  RawPeer receiver(mx.port, peers::TEST_EVENT_BACKEND, 0x7e51);
  RawPeer sender(mx.port, peers::TEST_EVENT_CLIENT, 0x7e52);
  ASSERT_TRUE(receiver.welcomed());
  ASSERT_TRUE(sender.welcomed());
  const std::string payload(200, 'p');  // past any string's own room: its body is an allocation of its own
  // One event, routed, read off the receiver before the next goes.
  const auto route = [&](int index) {
    MultiplexerMessage event;
    event.set_id(1000 + static_cast<std::uint64_t>(index));
    event.set_sender(0x7e52);
    event.set_type(types::TEST_EVENT);
    event.set_message(payload);
    MultiplexerMessage got;
    return sender.write_bytes(wire_bytes(event)) && receiver.next(&got) && got.id() == event.id();
  };
  for (int index = 0; index < WARM_UP; ++index) {
    ASSERT_TRUE(route(index));
  }
  on_loop(mx, [] {
    allocations = 0;
    counting = true;
  });
  for (int index = WARM_UP; index < WARM_UP + EVENTS; ++index) {
    ASSERT_TRUE(route(index));
  }
  std::size_t counted = 0;
  on_loop(mx, [&counted] {
    counting = false;
    counted = allocations;
  });
  EXPECT_GE(counted, PER_EVENT * EVENTS) << "the frame and its body, each event";
  EXPECT_LE(counted, PER_EVENT * EVENTS + OCCASIONAL) << "nothing more for a routed event";
}

}  // namespace
