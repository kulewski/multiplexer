// Clients inherited across fork() are orphans in the child: every call
// throws UsedAfterFork, destroying them neither hangs nor touches the
// parent's connections, and the parent's clients keep working. See
// lib/fork.h.
#include <sys/wait.h>
#include <unistd.h>

#include <memory>

#include <gtest/gtest.h>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

using multiplexer::Client;
using multiplexer::ThreadedClient;
using multiplexer::testing::InProcessMultiplexer;

namespace {

// Runs in the child: returns 0 when every check passed, otherwise the
// number of the first failed check. gtest cannot report from a forked
// child, so the parent asserts on the exit code.
int child_checks(std::unique_ptr<Client> &sync, std::unique_ptr<ThreadedClient> &threaded, unsigned short port) {
  try {
    sync->query("x", multiplexer::types::PYTHON_TEST_REQUEST, 1);
    return 1;
  } catch (Client::UsedAfterFork &) {
  }
  try {
    sync->connect("127.0.0.1", 1, 0.1f);
    return 2;
  } catch (Client::UsedAfterFork &) {
  }
  try {
    threaded->query("x", multiplexer::types::PYTHON_TEST_REQUEST, 1);
    return 3;
  } catch (ThreadedClient::UsedAfterFork &) {
  }
  try {
    threaded->send(threaded->new_message(multiplexer::types::PYTHON_TEST_REQUEST, "x"));
    return 4;
  } catch (ThreadedClient::UsedAfterFork &) {
  }
  sync.reset();     // the orphan teardown: must not hang
  threaded.reset(); // nor this one
  ThreadedClient fresh(multiplexer::peers::WEBSITE);
  if (!fresh.connect("127.0.0.1", port, 5))
    return 5;
  if (fresh.query("x", multiplexer::types::PYTHON_TEST_REQUEST, 5).outcome != ThreadedClient::FAILED)
    return 6; // FAILED: connected, and nobody serves the type
  return 0;
}

} // namespace

TEST(Fork, InheritedClientsAreOrphansAndTheParentKeepsWorking) {
  InProcessMultiplexer mx;
  std::unique_ptr<Client> sync(new Client(multiplexer::peers::WEBSITE));
  sync->connect("127.0.0.1", mx.port, 5);
  std::unique_ptr<ThreadedClient> threaded(new ThreadedClient(multiplexer::peers::WEBSITE));
  ASSERT_TRUE(threaded->connect("127.0.0.1", mx.port, 5));

  pid_t pid = fork();
  ASSERT_NE(-1, pid);
  if (pid == 0)
    _exit(child_checks(sync, threaded, mx.port));

  int status = 0;
  ASSERT_EQ(pid, waitpid(pid, &status, 0));
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(0, WEXITSTATUS(status)) << "the child's first failed check";

  // The parent's clients still work and are still registered.
  EXPECT_EQ(1u, threaded->connections_count());
  EXPECT_EQ(1u, sync->connections_count());
  EXPECT_EQ(ThreadedClient::FAILED, threaded->query("x", multiplexer::types::PYTHON_TEST_REQUEST, 5).outcome);
  EXPECT_THROW(sync->read_raw_message(0.2f), Client::OperationTimedOut);
}
