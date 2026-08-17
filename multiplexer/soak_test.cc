// An in-process soak: a Server, a backend on a thread of its own and a
// ThreadedClient, thousands of queries, and the C heap measured exactly
// before and after. A leak per message in the core shows here in about a
// second, without the integration harness.
#include <atomic>
#include <future>
#include <thread>

#include <gtest/gtest.h>

#include "lib/memory.h"
#include "multiplexer/backend/base_multiplexer_server.h"
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
      const char *srcdir = getenv("TEST_SRCDIR");
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
  boost::asio::io_service io_service;
  multiplexer::Server::pointer server;
  unsigned short port = 0;
  std::thread thread;
};

// A PYTHON_TEST_SERVER that upper-cases, served from its own thread.
struct EchoBackend : multiplexer::backend::BaseMultiplexerServer {
  EchoBackend(const multiplexer::backend::MultiplexerAddresses &addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER) {}
  void handle_message(multiplexer::MultiplexerMessage &mxmsg) override {
    std::string payload = mxmsg.message();
    for (char &character : payload)
      character = std::toupper(static_cast<unsigned char>(character));
    send_message(mx::util::kwargs::Kwargs()
                     .set("message", payload)
                     .set("type", static_cast<boost::uint32_t>(multiplexer::types::PYTHON_TEST_RESPONSE)));
  }
};

void run_queries(ThreadedClient &client, int count) {
  for (int index = 0; index < count; ++index) {
    ThreadedClient::Result result = client.query("hello", multiplexer::types::PYTHON_TEST_REQUEST, 10);
    ASSERT_EQ(ThreadedClient::REPLIED, result.outcome);
    ASSERT_EQ("HELLO", result.reply.third->message());
  }
}

} // namespace

TEST(Soak, ThousandsOfQueriesDoNotGrowTheHeap) {
  InProcessMultiplexer mx;
  multiplexer::backend::MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair(std::string("127.0.0.1"), mx.port));
  // The backend is built and driven on one thread, as the library requires.
  std::atomic<bool> keep_serving{true};
  std::promise<void> backend_ready;
  std::thread backend_thread([&] {
    EchoBackend backend(addresses);
    backend_ready.set_value();
    while (keep_serving) {
      try {
        backend.loop_iter(0.2f);
      } catch (multiplexer::Client::OperationTimedOut &) {
      }
    }
  });
  backend_ready.get_future().wait();
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));

  run_queries(client, 2000); // warm-up: buffers, caches, the dedup window, the ring of finished query ids
  size_t before = mx::heap_in_use_bytes();
  run_queries(client, 8000);
  size_t after = mx::heap_in_use_bytes();
  // The three peers share this heap; a leak per message would be megabytes.
  EXPECT_LE(after, before + 64 * 1024) << "heap grew from " << before << " to " << after << " bytes over 8000 queries";

  keep_serving = false;
  backend_thread.join();
  client.shutdown();
}
