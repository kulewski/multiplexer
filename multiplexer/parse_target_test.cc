// The multiplexer parses every frame into one message it keeps, whose
// strings keep their capacity from one frame to the next
// (Server::parse_target): routing a large event asks its thread for one
// block the size of the frame, the frame's body, where it asked for two,
// the second the parse's copy of the payload, made and freed per frame.
// After a frame whose strings, its payload, batch, key and workflow, hold
// more than Server::KEPT_PARSE_BYTES, or that had override rules, the kept
// message is let go, so that an odd frame's memory goes, and the next
// frame's parse allocates again. Counted, not timed: this binary's operator new counts the blocks
// of at least LARGE bytes the multiplexer's thread allocates while a
// handler posted to that thread says so.
#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <new>
#include <string>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/server.h"

namespace {

const std::size_t LARGE = 128 * 1024;  // a block this size or more is counted

// Whether this thread's allocations are counted, and the large ones so far.
thread_local bool counting = false;
std::atomic<std::uint64_t> large_blocks{0};

// Every allocation of the program, counted on a thread that says so, or
// null; malloc's, so that every form of delete below, free, matches every
// form of new, as a sanitizer checks.
void* counted_allocation(std::size_t size) noexcept {
  if (counting && size >= LARGE) {
    large_blocks.fetch_add(1, std::memory_order_relaxed);
  }
  return std::malloc(size ? size : 1);
}

}  // namespace

__attribute__((noinline)) void* operator new(std::size_t size) {
  if (void* memory = counted_allocation(size)) {
    return memory;
  }
  throw std::bad_alloc();
}
__attribute__((noinline)) void* operator new[](std::size_t size) { return operator new(size); }
__attribute__((noinline)) void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
  return counted_allocation(size);
}
__attribute__((noinline)) void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
  return counted_allocation(size);
}
// Never inlined, any of them: GCC at -O2, inlining a new or a delete into
// its caller, sees malloc() or free() behind it and calls the pair a
// mismatch (-Wmismatched-new-delete), not knowing that every form here is
// malloc's.
__attribute__((noinline)) void operator delete(void* memory) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete[](void* memory) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete(void* memory, const std::nothrow_t&) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete[](void* memory, const std::nothrow_t&) noexcept { std::free(memory); }

using multiplexer::Client;
using multiplexer::MultiplexerMessage;
using multiplexer::testing::InProcessMultiplexer;
namespace peers = multiplexer::peers;
namespace types = multiplexer::types;

namespace {

// Runs `call` on the multiplexer's thread and waits until it has.
template <typename Call>
void on_its_thread(InProcessMultiplexer& mx, Call call) {
  std::promise<void> done;
  mx.io_service.post([&call, &done] {
    call();
    done.set_value();
  });
  done.get_future().wait();
}

// A multiplexer, a receiver of TEST_EVENT and a sender of it.
struct Route {
  Route() : receiver(peers::TEST_EVENT_BACKEND), sender(peers::WEBSITE) {
    EXPECT_TRUE(receiver.wait_for_connection(receiver.connect("127.0.0.1", mx.port, 5), 5));
    EXPECT_TRUE(sender.wait_for_connection(sender.connect("127.0.0.1", mx.port, 5), 5));
  }

  // An event of `size` bytes, with a workflow of `workflow` bytes and
  // `rules` override rules naming its usual receiver, written and received.
  void send(std::size_t size, std::size_t workflow = 0, int rules = 0) {
    MultiplexerMessage msg;
    msg.set_id(sender.random64());
    msg.set_sender(sender.instance_id());
    msg.set_type(types::TEST_EVENT);
    msg.set_message(std::string(size, 'x'));
    if (workflow) {
      msg.set_workflow(std::string(workflow, 'w'));
    }
    for (int rule = 0; rule < rules; ++rule) {
      msg.add_override_rrules()->set_peer_type(peers::TEST_EVENT_BACKEND);
    }
    sender.send(msg, 30);
    EXPECT_EQ(msg.id(), receiver.receive_message(30).first->id());
  }

  // The large blocks the multiplexer's thread allocated while routing
  // `count` events of `size` bytes: counted from a handler before the
  // first send to one after the receiver had the last, by when the
  // routing handler has returned.
  std::uint64_t large_blocks_routing(unsigned int count, std::size_t size) {
    on_its_thread(mx, [] {
      large_blocks = 0;
      counting = true;
    });
    for (unsigned int index = 0; index < count; ++index) {
      send(size);
    }
    on_its_thread(mx, [] { counting = false; });
    return large_blocks.load();
  }

  InProcessMultiplexer mx;
  Client receiver;
  Client sender;
};

TEST(ParseTarget, ALargeFrameIsParsedIntoMemoryAlreadyThere) {
  Route route;
  route.send(256 * 1024);  // the kept message's payload grows to this once
  EXPECT_EQ(10u, route.large_blocks_routing(10, 256 * 1024)) << "one block per frame, its body, and no parse copy";
}

TEST(ParseTarget, AFrameOverTheCapLetsTheKeptMessageGo) {
  Route route;
  route.send(256 * 1024);
  route.send(multiplexer::Server::KEPT_PARSE_BYTES + 1024);
  // The next frame is parsed into a fresh message, which allocates its
  // payload once; the one after it into that message again.
  EXPECT_EQ(2u, route.large_blocks_routing(1, 256 * 1024)) << "the body and the fresh message's payload";
  EXPECT_EQ(1u, route.large_blocks_routing(1, 256 * 1024)) << "the body alone";
}

// The cap counts every string a frame may make big: a workflow over it
// lets the kept message go as a payload does.
TEST(ParseTarget, AFrameWithABigWorkflowLetsTheKeptMessageGo) {
  Route route;
  route.send(256 * 1024);
  route.send(16, multiplexer::Server::KEPT_PARSE_BYTES + 1024);
  EXPECT_EQ(2u, route.large_blocks_routing(1, 256 * 1024)) << "the body and the fresh message's payload";
}

// Override rules, whose room a clear keeps, are not kept from one frame to
// the next: the frame after them is parsed into a fresh message.
TEST(ParseTarget, AFrameWithOverrideRulesLetsTheKeptMessageGo) {
  Route route;
  route.send(256 * 1024);
  route.send(16, 0, 3);
  EXPECT_EQ(2u, route.large_blocks_routing(1, 256 * 1024)) << "the body and the fresh message's payload";
}

}  // namespace
