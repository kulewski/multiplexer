// Recording a routed message copies of its payload only what the sinks
// keep. A broadcast of a large payload to several recipients, recorded to
// a file session that keeps 16 bytes of each payload and a tap that keeps
// 32, asks the multiplexer's thread for a few kilobytes more than the same
// broadcast unrecorded, where it copied the whole payload into each
// recipient's record, and again for the file and for each tap to cut it. Counted, not timed: a global operator new of
// the test's own adds up the bytes the multiplexer's thread allocates while the test says so, a handler posted to that
// thread switching it on and off around the broadcast. And each sink gets the payload its limit says, cut from the same
// records: the file 16 bytes, a tap 32, a tap with no limit all of it.
#include <fcntl.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <new>
#include <string>
#include <vector>

#include "lib/protobuf/stream.h"
#include "multiplexer/Recording.pb.h"
#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"

namespace {

// Whether this thread's allocations are counted, and their bytes so far.
thread_local bool counting = false;
std::atomic<std::uint64_t> counted{0};

// Every allocation of the program, counted on a thread that says so, or
// null; malloc's, so that every form of delete below, free, matches every
// form of new, as a sanitizer checks.
void* counted_allocation(std::size_t size) noexcept {
  if (counting) {
    counted.fetch_add(size, std::memory_order_relaxed);
  }
  return std::malloc(size ? size : 1);
}

}  // namespace

void* operator new(std::size_t size) {
  if (void* memory = counted_allocation(size)) {
    return memory;
  }
  throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return operator new(size); }
void* operator new(std::size_t size, const std::nothrow_t&) noexcept { return counted_allocation(size); }
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept { return counted_allocation(size); }
// Never inlined: GCC at -O2, inlining one into code that got its memory from
// operator new, sees free() on that memory and calls it a mismatch
// (-Wmismatched-new-delete), not knowing that every new above is malloc's.
__attribute__((noinline)) void operator delete(void* memory) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete[](void* memory) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete(void* memory, const std::nothrow_t&) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete[](void* memory, const std::nothrow_t&) noexcept { std::free(memory); }

using multiplexer::Client;
using multiplexer::MultiplexerMessage;
using multiplexer::Record;
using multiplexer::RecordingControl;
using multiplexer::RecordingStatus;
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

// A multiplexer, `count` receivers of TEST_EVENT, every one a copy, and a
// sender of it.
struct Broadcast {
  explicit Broadcast(unsigned int count) : sender(peers::WEBSITE) {
    for (unsigned int index = 0; index < count; ++index) {
      receivers.emplace_back(new Client(peers::TEST_EVENT_BACKEND));
      EXPECT_TRUE(receivers.back()->wait_for_connection(receivers.back()->connect("127.0.0.1", mx.port, 5), 5));
    }
    EXPECT_TRUE(sender.wait_for_connection(sender.connect("127.0.0.1", mx.port, 5), 5));
  }

  // `payload` to every receiver, written and received by each.
  void send(const std::string& payload) {
    MultiplexerMessage msg;
    msg.set_id(sender.random64());
    msg.set_sender(sender.instance_id());
    msg.set_type(types::TEST_EVENT);
    msg.set_message(payload);
    sender.send(msg, 30);
    for (const std::unique_ptr<Client>& receiver : receivers) {
      EXPECT_EQ(msg.id(), receiver->receive_message(30).first->id());
    }
  }

  // The bytes the multiplexer's thread allocated while routing `payload`
  // to every receiver: counted from a handler before the send to one after
  // every receiver had it, by when the routing handler has returned.
  std::uint64_t allocated_sending(const std::string& payload) {
    on_its_thread(mx, [] {
      counted = 0;
      counting = true;
    });
    send(payload);
    on_its_thread(mx, [] { counting = false; });
    return counted.load();
  }

  InProcessMultiplexer mx;
  std::vector<std::unique_ptr<Client>> receivers;
  Client sender;
};

// A peer tapping `mx` with `payload_limit`, its TAP answered.
std::unique_ptr<Client> tapping(InProcessMultiplexer& mx, unsigned int payload_limit) {
  std::unique_ptr<Client> tap(new Client(multiplexer::RECORDING_CONTROLLER));
  EXPECT_TRUE(tap->wait_for_connection(tap->connect("127.0.0.1", mx.port, 5), 5));
  RecordingControl control;
  control.set_action(RecordingControl::TAP);
  control.set_payload_limit(payload_limit);
  MultiplexerMessage msg;
  msg.set_id(tap->random64());
  msg.set_sender(tap->instance_id());
  msg.set_type(multiplexer::RECORDING_CONTROL);
  control.SerializeToString(msg.mutable_message());
  tap->send(msg, 5);
  for (;;) {
    const std::shared_ptr<MultiplexerMessage> answer = tap->receive_message(5).first;
    RecordingStatus status;
    if (answer->type() == multiplexer::RECORDING_STATUS && answer->references() == msg.id() &&
        status.ParseFromString(answer->message())) {
      EXPECT_TRUE(status.tapping()) << status.error();
      return tap;
    }
  }
}

// The payloads of the routed records `tap` has received, `count` of them.
std::vector<std::pair<std::string, bool>> routed_payloads(Client& tap, unsigned int count) {
  std::vector<std::pair<std::string, bool>> payloads;
  while (payloads.size() < count) {
    const std::shared_ptr<MultiplexerMessage> streamed = tap.receive_message(30).first;
    Record record;
    if (streamed->type() == multiplexer::RECORDING_RECORD && record.ParseFromString(streamed->message()) &&
        record.has_routed()) {
      payloads.emplace_back(record.routed().payload(), record.routed().truncated());
    }
  }
  return payloads;
}

// Where a test writes its file session.
std::string session_path(const std::string& name) {
  const char* directory = std::getenv("TEST_TMPDIR");
  return std::string(directory ? directory : "/tmp") + "/" + name + ".rec";
}

}  // namespace

TEST(RecordingCopies, ABroadcastCopiesOnlyWhatTheSinksKeep) {
  const unsigned int recipients = 3;
  const std::string payload(4 << 20, 'x');
  Broadcast broadcast(recipients);
  broadcast.send(payload);  // the buffers every send has, made once
  const std::uint64_t unrecorded = broadcast.allocated_sending(payload);

  std::string error;
  on_its_thread(broadcast.mx, [&broadcast, &error] {
    broadcast.mx.server->set_allow_tap(true);
    broadcast.mx.server->start_recording(session_path("copies"), "copies", 16, 0, 0, &error);
  });
  ASSERT_EQ("", error);
  std::unique_ptr<Client> tap = tapping(broadcast.mx, 32);
  const std::uint64_t recorded = broadcast.allocated_sending(payload);
  std::cout << "allocated on the multiplexer's thread: " << unrecorded << " bytes unrecorded, " << recorded
            << " recorded\n";
  EXPECT_LT(recorded, unrecorded + payload.size() / 4)
      << "a few kilobytes in all, where each of " << recipients << " recipients copied the " << payload.size()
      << "-byte payload whole, once for its record and once more for each sink to cut it";
  on_its_thread(broadcast.mx, [&broadcast] { broadcast.mx.server->stop_recording("done"); });
}

TEST(RecordingCopies, EachSinkGetsWhatItsLimitKeeps) {
  const unsigned int recipients = 2;
  std::string payload;
  for (int index = 0; index < 100; ++index) {
    payload += static_cast<char>('a' + index % 26);
  }
  Broadcast broadcast(recipients);
  const std::string path = session_path("limits");
  std::string error;
  on_its_thread(broadcast.mx, [&broadcast, &path, &error] {
    broadcast.mx.server->set_allow_tap(true);
    broadcast.mx.server->start_recording(path, "limits", 16, 0, 0, &error);
  });
  ASSERT_EQ("", error);
  std::unique_ptr<Client> tap_32 = tapping(broadcast.mx, 32);
  std::unique_ptr<Client> tap_all = tapping(broadcast.mx, 0);
  broadcast.send(payload);
  on_its_thread(broadcast.mx, [&broadcast] { broadcast.mx.server->stop_recording("done"); });

  const std::vector<std::pair<std::string, bool>> cut_to_32(recipients, {payload.substr(0, 32), true});
  const std::vector<std::pair<std::string, bool>> whole(recipients, {payload, false});
  EXPECT_EQ(cut_to_32, routed_payloads(*tap_32, recipients));
  EXPECT_EQ(whole, routed_payloads(*tap_all, recipients));
  std::vector<std::pair<std::string, bool>> in_file;
  mx::protobuf::FileMessageInputStream file(open(path.c_str(), O_RDONLY), true);
  Record record;
  while (file.read(record)) {
    if (record.has_routed()) {
      in_file.emplace_back(record.routed().payload(), record.routed().truncated());
    }
  }
  const std::vector<std::pair<std::string, bool>> cut_to_16(recipients, {payload.substr(0, 16), true});
  EXPECT_EQ(cut_to_16, in_file);
}
