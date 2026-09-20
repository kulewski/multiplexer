// An in-process soak: a Server, a backend on a thread of its own and a
// ThreadedClient, thousands of queries, and the C heap measured exactly
// before and after. A leak per message in the core shows here in about a
// second, without the integration harness.
#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <thread>

#include "lib/memory.h"
#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/backend/base_threaded_multiplexer_server.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/server.h"
#include "multiplexer/threaded_client.h"

using multiplexer::ThreadedClient;

namespace {

// A multiplexer with the example rules, on its own thread.
struct InProcessMultiplexer {
  InProcessMultiplexer() {
    std::promise<unsigned short> bound;
    thread = std::thread([this, &bound] {
      const char* srcdir = getenv("TEST_SRCDIR");
      std::string rules = std::string(srcdir ? srcdir : ".") + (srcdir ? "/mx/" : "/") + "multiplexer.rules";
      server = multiplexer::Server::Create(io_service, "127.0.0.1", 0);
      server->clear_rules();
      server->read_rules(rules);
      server->start();
      bound.set_value(server->local_port());
      io_service.run();
    });
    port = bound.get_future().get();
  }
  ~InProcessMultiplexer() {
    io_service.post([this] { server->stop(); });
    thread.join();
  }
  asio::io_service io_service;
  multiplexer::Server::pointer server;
  unsigned short port = 0;
  std::thread thread;
};

// A PYTHON_TEST_SERVER that upper-cases, served from its own thread.
struct EchoBackend : multiplexer::backend::BaseMultiplexerServer {
  EchoBackend(const multiplexer::backend::MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER) {}
  std::uint64_t instance_id() const { return conn->instance_id(); }
  void handle_message(multiplexer::MultiplexerMessage& mxmsg) override {
    std::string payload = mxmsg.message();
    for (char& character : payload) {
      character = std::toupper(static_cast<unsigned char>(character));
    }
    send_message(mx::util::kwargs::Kwargs()
                     .set("message", payload)
                     .set("type", static_cast<std::uint32_t>(multiplexer::types::PYTHON_TEST_RESPONSE)));
  }
};

// The same backend on worker threads behind a heartbeating io thread.
struct EchoThreadedBackend : multiplexer::backend::BaseThreadedMultiplexerServer {
  EchoThreadedBackend(const multiplexer::backend::MultiplexerAddresses& addresses, unsigned int workers)
      : BaseThreadedMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER, options_for(workers)) {}
  static Options options_for(unsigned int workers) {
    Options options;
    options.workers = workers;
    return options;
  }
  void handle_message(const multiplexer::backend::RequestPtr& request) override {
    std::string payload = request->mxmsg().message();
    for (char& character : payload) {
      character = std::toupper(static_cast<unsigned char>(character));
    }
    request->reply(payload, multiplexer::types::PYTHON_TEST_RESPONSE);
  }
};

void run_queries(ThreadedClient& client, int count) {
  for (int index = 0; index < count; ++index) {
    ThreadedClient::Result result = client.query("hello", multiplexer::types::PYTHON_TEST_REQUEST, 10);
    ASSERT_EQ(ThreadedClient::REPLIED, result.outcome);
    ASSERT_EQ("HELLO", result.reply.third->message());
  }
}

// The same through lanes and by address: typed queries on one lane,
// addressed queries on it with either probe, a lane made and dropped per
// query, a flushing send per query, and the reply's connection preferred.
void run_lane_queries(ThreadedClient& client, std::uint64_t backend_id, int count) {
  multiplexer::LanePtr lane(new multiplexer::Lane());
  for (int index = 0; index < count; ++index) {
    ThreadedClient::Result result = client.query("hello", multiplexer::types::PYTHON_TEST_REQUEST, 10, lane);
    ASSERT_EQ(ThreadedClient::REPLIED, result.outcome);
    multiplexer::MultiplexerMessage request = client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "hello");
    request.set_to(backend_id);
    result = client.query(request, 10, lane, index % 2 ? multiplexer::PROBE_PING : multiplexer::PROBE_SEARCH);
    ASSERT_EQ(ThreadedClient::REPLIED, result.outcome);
    ASSERT_EQ("HELLO", result.reply.third->message());
    multiplexer::LanePtr dropped(new multiplexer::Lane(index % 3 == 0));
    result = client.query(request, 10, dropped);
    ASSERT_EQ(ThreadedClient::REPLIED, result.outcome);
    ASSERT_EQ(1, client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "event"), dropped, 10));
    result = client.query(request, result.reply.second, 10);
    ASSERT_EQ(ThreadedClient::REPLIED, result.outcome);
  }
}

}  // namespace

TEST(Soak, ThousandsOfQueriesDoNotGrowTheHeap) {
  InProcessMultiplexer mx;
  multiplexer::backend::MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair(std::string("127.0.0.1"), mx.port));
  // The backend is built and driven on one thread, as the library requires.
  std::atomic<bool> keep_serving{true};
  std::promise<std::uint64_t> backend_ready;
  std::thread backend_thread([&] {
    EchoBackend backend(addresses);
    backend_ready.set_value(backend.instance_id());
    while (keep_serving) {
      try {
        backend.loop_iter(0.2f);
      } catch (multiplexer::Client::OperationTimedOut&) {
      }
    }
  });
  std::uint64_t backend_id = backend_ready.get_future().get();
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));

  run_queries(client, 2000);  // warm-up: buffers, caches, the dedup window, the ring of finished query ids
  run_lane_queries(client, backend_id, 200);
  size_t before = mx::heap_in_use_bytes();
  run_queries(client, 8000);
  run_lane_queries(client, backend_id, 1000);  // five thousand more, through lanes and by address
  size_t after = mx::heap_in_use_bytes();
  // The three peers share this heap; a leak per message would be megabytes.
  EXPECT_LE(after, before + 64 * 1024) << "heap grew from " << before << " to " << after << " bytes over 13000 queries";

  keep_serving = false;
  backend_thread.join();
  client.shutdown();
}

TEST(Soak, ThousandsOfRequestsThroughAThreadedBackendDoNotGrowTheHeap) {
  InProcessMultiplexer mx;
  multiplexer::backend::MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair(std::string("127.0.0.1"), mx.port));
  EchoThreadedBackend backend(addresses, 2);
  std::thread serving([&] { backend.serve_forever(0.05f); });
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));

  run_queries(client, 2000);  // warm-up
  size_t before = mx::heap_in_use_bytes();
  run_queries(client, 8000);
  std::vector<std::future<void>> at_once;  // and from four threads at once, for the two workers
  for (int thread = 0; thread < 4; ++thread) {
    at_once.push_back(std::async(std::launch::async, [&] { run_queries(client, 1000); }));
  }
  for (auto& done : at_once) {
    done.get();
  }
  size_t after = mx::heap_in_use_bytes();
  EXPECT_LE(after, before + 64 * 1024) << "heap grew from " << before << " to " << after
                                       << " bytes over 12000 requests through a threaded backend";

  backend.stop();
  serving.join();
  client.shutdown();
}
