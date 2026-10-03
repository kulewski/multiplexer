// A negative timeout is no deadline, as an infinite one is, in both C++
// clients: ThreadedClient's connect, query, flushing and plain sends,
// flush_all and shutdown wait as long as it takes, where their deadline
// was already past and each gave up at once, a request already sent; the
// SyncClient's queue() holds its message with no deadline, where it had
// DEFAULT_TIMEOUT, its shutdown() writes what was sent first, where it
// dropped it at once, and its schedule_one() through a connection that is
// gone waits for another, where it threw at once. Ordered, not timed: a
// multiplexer frozen before it welcomes the client or routes its request
// holds every call at the point where the old deadline ended it, and a
// message with a short deadline, dropped meanwhile, says when the old one
// would have been.
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <future>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

using multiplexer::MultiplexerMessage;
using multiplexer::ThreadedClient;
using multiplexer::testing::InProcessMultiplexer;

namespace {

const float NO_DEADLINE = -1;  // the timeout every test gives

// Holds a multiplexer's io thread, so that it accepts, welcomes and routes
// nothing while the sockets stay open: a multiplexer frozen, until
// release() or the end of the scope.
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
  ~Freeze() { release(); }
  void release() {
    if (!done) {
      done = true;
      released.set_value();
    }
  }
  std::promise<void> released;
  bool done = false;
};

// The bytes waiting to be read on the multiplexer's side of the connections
// to `port`, established ones, not the listening socket, as /proc/net/tcp
// counts them in this network namespace: what a frozen multiplexer has not
// read yet.
std::size_t unread_bytes(unsigned short port) {
  std::ifstream table("/proc/net/tcp");
  std::string line;
  std::getline(table, line);  // the header
  std::size_t unread = 0;
  while (std::getline(table, line)) {
    std::istringstream fields(line);
    std::string slot, local, remote, state, queues;
    fields >> slot >> local >> remote >> state >> queues;
    const unsigned long local_port = std::stoul(local.substr(local.find(':') + 1), nullptr, 16);
    if (local_port == port && state == "01") {                                 // ESTABLISHED
      unread += std::stoul(queues.substr(queues.find(':') + 1), nullptr, 16);  // tx_queue:rx_queue
    }
  }
  return unread;
}

// A backend of PYTHON_TEST_SERVER, the type PYTHON_TEST_REQUEST is routed
// to: keeps the payload of every message it is handed, and answers a
// "question" with it.
struct Backend {
  explicit Backend(unsigned short port)
      : client(multiplexer::peers::PYTHON_TEST_SERVER, [this](const multiplexer::IncomingMessage& incoming) {
          {
            std::lock_guard<std::mutex> lock(mutex);
            payloads.push_back(incoming.third->message());
          }
          arrived.notify_all();
          if (incoming.third->message() != "question") {
            return;  // an event
          }
          MultiplexerMessage reply =
              client.new_message(multiplexer::types::PYTHON_TEST_RESPONSE, incoming.third->message());
          reply.set_to(incoming.third->sender());
          reply.set_references(incoming.third->id());
          client.send(reply, incoming.second);
        }) {
    client.connect("127.0.0.1", port, 5);
  }
  // Whether a message with `payload` arrived, within 30 s.
  bool wait_for(const std::string& payload) {
    std::unique_lock<std::mutex> lock(mutex);
    return arrived.wait_for(lock, std::chrono::seconds(30),
                            [&] { return std::find(payloads.begin(), payloads.end(), payload) != payloads.end(); });
  }
  // Before the client, which writes into them until it is gone.
  std::mutex mutex;
  std::condition_variable arrived;
  std::vector<std::string> payloads;
  ThreadedClient client;
};

// A message for the backend, by its type; the SyncClient that sends it
// fills in the id and the sender.
MultiplexerMessage request(const std::string& payload) {
  MultiplexerMessage msg;
  msg.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  msg.set_message(payload);
  return msg;
}

// Whether `predicate()` holds within 10 s, asked every 10 ms.
template <typename Predicate>
bool eventually(Predicate predicate) {
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (!predicate()) {
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return true;
}

}  // namespace

// connect() waits for the welcome of a multiplexer that does not answer
// yet: its timer had expired as it was set, and it returned false.
TEST(NegativeTimeout, AThreadedConnectWaitsForTheWelcome) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  std::future<bool> connected;
  {
    Freeze frozen(mx);
    connected =
        std::async(std::launch::async, [&client, &mx] { return client.connect("127.0.0.1", mx.port, NO_DEADLINE); });
  }
  ASSERT_EQ(std::future_status::ready, connected.wait_for(std::chrono::seconds(30)));
  EXPECT_TRUE(connected.get()) << "connected once the multiplexer welcomed it";
}

// A query waits for its reply through a multiplexer that routes nothing
// yet: every stage's timer had expired as it was set, so the request went
// out, a search followed, and the query ended TIMED_OUT, the multiplexer
// still frozen.
TEST(NegativeTimeout, AThreadedQueryWaitsForTheReply) {
  InProcessMultiplexer mx;
  Backend backend(mx.port);
  std::promise<ThreadedClient::Result> ended;  // before the client, whose callback sets it
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  {
    Freeze frozen(mx);
    client.query(
        "question", multiplexer::types::PYTHON_TEST_REQUEST,
        [&ended](const ThreadedClient::Result& result) { ended.set_value(result); }, NO_DEADLINE);
  }
  std::future<ThreadedClient::Result> result = ended.get_future();
  ASSERT_EQ(std::future_status::ready, result.wait_for(std::chrono::seconds(30)));
  const ThreadedClient::Result got = result.get();
  ASSERT_EQ(ThreadedClient::REPLIED, got.outcome);
  EXPECT_EQ("question", got.reply.third->message());
}

// A flushing send waits for a connection and writes its message: its
// deadline was past, so it returned 0 and the message went a moment later.
TEST(NegativeTimeout, AThreadedFlushingSendWaitsForAConnection) {
  InProcessMultiplexer mx;
  Backend backend(mx.port);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  std::future<unsigned int> written;
  {
    Freeze frozen(mx);
    EXPECT_FALSE(client.connect("127.0.0.1", mx.port, 0)) << "on its way, not welcomed";
    written = std::async(std::launch::async, [&client] {
      return client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "flushed"), NO_DEADLINE);
    });
    ASSERT_TRUE(eventually([&client] { return client.waiting_messages() == 1; })) << "held for the connection";
  }
  ASSERT_EQ(std::future_status::ready, written.wait_for(std::chrono::seconds(30)));
  EXPECT_EQ(1u, written.get());
  EXPECT_TRUE(backend.wait_for("flushed"));
  EXPECT_EQ(0u, client.dropped());
}

// A send that does not flush, as Python's send_message() without flush=,
// holds its message for a connection with no deadline. Another with a
// 50 ms deadline, sent after it, is dropped meanwhile: the first had a
// millisecond, and was dropped before it.
TEST(NegativeTimeout, AThreadedSendHoldsItsMessageForAConnection) {
  InProcessMultiplexer mx;
  Backend backend(mx.port);
  std::mutex mutex;
  std::condition_variable told;
  std::vector<std::uint64_t> dropped;  // before the client, whose observer writes into it
  std::promise<unsigned int> patient_ended;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  client.set_drop_observer([&](std::uint64_t id, multiplexer::DropReason) {
    std::lock_guard<std::mutex> lock(mutex);
    dropped.push_back(id);
    told.notify_all();
  });
  const MultiplexerMessage patient = client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "patient");
  const MultiplexerMessage hasty = client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "hasty");
  const auto was_dropped = [&dropped](std::uint64_t id) {
    return std::find(dropped.begin(), dropped.end(), id) != dropped.end();
  };
  {
    Freeze frozen(mx);
    EXPECT_FALSE(client.connect("127.0.0.1", mx.port, 0)) << "on its way, not welcomed";
    client.send_serialized(patient.SerializeAsString(), patient.id(), patient.type(), multiplexer::LanePtr(),
                           NO_DEADLINE, [&patient_ended](unsigned int written) { patient_ended.set_value(written); });
    client.send_serialized(hasty.SerializeAsString(), hasty.id(), hasty.type(), multiplexer::LanePtr(), 0.05f);
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(told.wait_for(lock, std::chrono::seconds(30), [&] { return was_dropped(hasty.id()); }));
    ASSERT_FALSE(was_dropped(patient.id())) << "the message with no deadline is held still";
  }
  std::future<unsigned int> written = patient_ended.get_future();
  ASSERT_EQ(std::future_status::ready, written.wait_for(std::chrono::seconds(30)));
  EXPECT_EQ(1u, written.get());
  EXPECT_TRUE(backend.wait_for("patient"));
}

// flush_all() waits until what was sent before it is written, once a
// connection comes up: its timer had expired as it was set, and it said
// false. The callback form, which an asyncio layer awaits, so that the
// flush has begun before the multiplexer thaws.
TEST(NegativeTimeout, AThreadedFlushAllWaitsForWhatWasSent) {
  InProcessMultiplexer mx;
  Backend backend(mx.port);
  std::promise<bool> flushed;  // before the client, whose callback sets it
  ThreadedClient client(multiplexer::peers::WEBSITE);
  {
    Freeze frozen(mx);
    EXPECT_FALSE(client.connect("127.0.0.1", mx.port, 0)) << "on its way, not welcomed";
    client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "sent before"));
    client.flush_all_with_callback(NO_DEADLINE, [&flushed](bool all_written) { flushed.set_value(all_written); });
    EXPECT_EQ(1u, client.waiting_messages()) << "held: a round trip through the io thread, after the flush began";
  }
  std::future<bool> result = flushed.get_future();
  ASSERT_EQ(std::future_status::ready, result.wait_for(std::chrono::seconds(30)));
  EXPECT_TRUE(result.get());
  EXPECT_TRUE(backend.wait_for("sent before"));
}

// shutdown() writes what was sent before it, however long that takes: it
// dropped it at once, as with 0.
TEST(NegativeTimeout, AThreadedShutdownWritesWhatWasSentFirst) {
  InProcessMultiplexer mx;
  Backend backend(mx.port);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  std::thread closing;
  {
    Freeze frozen(mx);
    EXPECT_FALSE(client.connect("127.0.0.1", mx.port, 0)) << "on its way, not welcomed";
    client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "last"));
    EXPECT_EQ(1u, client.waiting_messages()) << "held";
    closing = std::thread([&client] { client.shutdown(NO_DEADLINE); });
    EXPECT_TRUE(eventually([&client] {  // until the shutdown has begun
      try {
        client.connections_count();
        return false;
      } catch (const ThreadedClient::NotConnected&) {
        return true;
      }
    }));
  }
  closing.join();
  ASSERT_EQ(0u, client.dropped()) << "nothing given up on";
  EXPECT_TRUE(backend.wait_for("last"));
}

// The SyncClient's shutdown() writes what was sent before it: it dropped it
// at once, as with 0. The client queued it while the multiplexer was
// frozen, and only a call that runs the loop reads the welcome since.
TEST(NegativeTimeout, ASyncShutdownWritesWhatWasSentFirst) {
  InProcessMultiplexer mx;
  Backend backend(mx.port);
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  {
    Freeze frozen(mx);
    client.async_connect("127.0.0.1", mx.port);
    ASSERT_TRUE(client.queue(request("last"))) << "held for the connection";
  }
  client.shutdown(NO_DEADLINE);
  ASSERT_EQ(0u, client.dropped()) << "nothing given up on";
  EXPECT_TRUE(backend.wait_for("last"));
}

// The SyncClient's queue() holds its message with no deadline: nothing is
// left for the loop to wait for, so a receive on a client that has no
// connection, nor one on its way, throws NotConnected at once with nothing
// dropped, where the message's DEFAULT_TIMEOUT kept the receive running
// the loop until the message was dropped. It goes out once a connection
// comes up.
TEST(NegativeTimeout, ASyncQueueHoldsItsMessageWithNoDeadline) {
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.queue(request("held"), NO_DEADLINE)) << "held for a connection";
  EXPECT_THROW(client.receive_message(NO_DEADLINE), multiplexer::Client::NotConnected);
  ASSERT_EQ(0u, client.dropped()) << "nothing given up on";
  InProcessMultiplexer mx;
  Backend backend(mx.port);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  EXPECT_TRUE(client.flush_all(5));
  EXPECT_TRUE(backend.wait_for("held"));
}

// The SyncClient's schedule_one() through a connection that is gone waits
// for another to come up, as long as it takes: it threw NotConnected at
// once, waiting only for a positive timeout. The frozen multiplexer thaws
// once the client's welcome, which only a call that runs the loop writes,
// waits in its socket: this call has begun.
TEST(NegativeTimeout, ASyncScheduleThroughAConnectionGoneWaitsForAnother) {
  InProcessMultiplexer mx;
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  multiplexer::Client::ScheduledMessageTracker tracker{multiplexer::Client::BasicScheduledMessageTracker()};
  {
    Freeze frozen(mx);
    client.async_connect("127.0.0.1", mx.port);
    std::thread thawing([&frozen, &mx] {
      EXPECT_TRUE(eventually([&mx] { return unread_bytes(mx.port) > 0; })) << "no welcome came";
      frozen.release();
    });
    EXPECT_NO_THROW(tracker = client.schedule_one(request("elsewhere"), multiplexer::ConnectionWrapper(), NO_DEADLINE));
    thawing.join();
  }
  ASSERT_TRUE(tracker);
  client.flush(tracker, 5);
  EXPECT_TRUE(tracker.is_sent());
}
