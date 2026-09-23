// A backend built on one thread, connected and served from another: serve_forever()
// adopts the serving thread, so the debug-build thread checks, which bind a
// client and its connections to the thread that made them, do not fire.
#include <gtest/gtest.h>

#include <thread>

#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"

using multiplexer::backend::BaseMultiplexerServer;
using multiplexer::backend::MultiplexerAddresses;
using multiplexer::testing::InProcessMultiplexer;

namespace {

// Serves three iterations, then stops.
class CountingBackend : public BaseMultiplexerServer {
 public:
  explicit CountingBackend(const MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER) {}
  int iterations = 0;
  multiplexer::Client* conn_for_test() { return conn; }
  void close_for_test() { close(); }

 protected:
  void handle_message(multiplexer::MultiplexerMessage&) override { no_response(); }
  void periodic_task() override {
    if (++iterations >= 3) {
      working = false;
    }
  }
};

}  // namespace

TEST(ServeThread, BuiltOnOneThreadServedFromAnother) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  CountingBackend backend(addresses);  // built on this thread; serve_forever() connects on the other
  std::thread server([&backend] { backend.serve_forever(0.1f); });
  server.join();
  EXPECT_EQ(3, backend.iterations);
}

TEST(ServeThread, ExplicitRebindForOwnLoop) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  CountingBackend backend(addresses);
  std::thread driver([&backend] {
    backend.conn_for_test()->bind_to_current_thread();
    backend.connect();  // what serve_forever() would have done first
    for (int i = 0; i < 3; ++i) {
      try {
        backend.loop_iter(0.1f);
      } catch (multiplexer::Client::OperationTimedOut&) {
      }
    }
    backend.close_for_test();  // the owning thread closes; destroying on another would fail the check
  });
  driver.join();
}

TEST(ServeThread, NothingIsConnectedBeforeServeForever) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  CountingBackend backend(addresses);
  EXPECT_NE(0u, backend.conn_for_test()->instance_id()) << "the id is known before serving";
  EXPECT_EQ(0u, backend.conn_for_test()->connections_count()) << "and nothing is connected";
  backend.connect();  // a program that announces itself before serving
  EXPECT_EQ(1u, backend.conn_for_test()->connections_count());
  std::thread server([&backend] { backend.serve_forever(0.1f); });  // connects nothing more
  server.join();
  EXPECT_EQ(3, backend.iterations);
}
