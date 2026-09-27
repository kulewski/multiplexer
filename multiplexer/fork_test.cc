// Clients inherited across fork() are orphans in the child: every call
// throws UsedAfterFork, destroying them neither hangs nor touches the
// parent's connections, and the parent's clients keep working. See
// lib/fork.h. And the calls throw before they take any lock, which a
// parent thread may have held at the instant of the fork: a thread of the
// parent is frozen inside a call, holding a lock the call took, while the
// main thread forks. A client made in the child refuses a lane or a
// connection from before the fork, the child's copies of the parent's
// sockets are closed once, and a threaded backend the child inherited
// refuses its calls and is destroyed without waiting for the parent's
// workers.
#include <dirent.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <pthread.h>
#include <sched.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "lib/timer.h"
#include "multiplexer/backend/base_threaded_multiplexer_server.h"
#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

using multiplexer::Client;
using multiplexer::ConnectionWrapper;
using multiplexer::ThreadedClient;
using multiplexer::testing::InProcessMultiplexer;

// The sanitizer's allocator is not fork-safe: a fork leaves it locked in
// the child whenever another thread is allocating. GCC says so with
// __SANITIZE_ADDRESS__, clang with __has_feature.
#if defined(__SANITIZE_ADDRESS__)
#define MX_FORK_TEST_ASAN 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define MX_FORK_TEST_ASAN 1
#endif
#endif

namespace {

// A parent thread frozen while it holds a lock. This binary's
// pthread_mutex_lock wraps the real one: a thread that set
// freeze_after_locks to N stops right after its Nth lock from then on,
// still holding it, until thaw is set. A fork made meanwhile gives a child
// in which that lock is taken by a thread that does not exist there, as a
// real program's fork does when one of its threads happens to be inside
// the lock at that instant. Otherwise the wrapper costs a thread-local
// read.
thread_local int freeze_after_locks = 0;
std::atomic<bool> frozen(false);
std::atomic<bool> thaw(false);
using LockFunction = int (*)(pthread_mutex_t*);
std::atomic<LockFunction> real_lock(nullptr);

}  // namespace

extern "C" int pthread_mutex_lock(pthread_mutex_t* mutex) {
  LockFunction real = real_lock.load(std::memory_order_acquire);
  if (!real) {
    real = reinterpret_cast<LockFunction>(dlsym(RTLD_NEXT, "pthread_mutex_lock"));
    real_lock.store(real, std::memory_order_release);
  }
  const int result = real(mutex);
  if (freeze_after_locks > 0 && --freeze_after_locks == 0) {
    frozen.store(true);
    while (!thaw.load()) {
      sched_yield();  // no sleep and no condition variable: they would take locks of their own
    }
  }
  return result;
}

namespace {

// A thread running `call` until it is frozen right after its `locks`th
// lock, from construction until release(). held() is false when the call
// returned before taking that many, which a test treats as a failure: the
// lock it meant to hold at the fork was not held.
class Frozen {
 public:
  Frozen(int locks, std::function<void()> call) : held_(true) {
    frozen.store(false);
    thaw.store(false);
    thread_ = std::thread([this, locks, call] {
      freeze_after_locks = locks;
      try {
        call();
      } catch (...) {
      }
      if (freeze_after_locks != 0) {
        freeze_after_locks = 0;
        held_.store(false);
        frozen.store(true);
      }
    });
    while (!frozen.load()) {
      sched_yield();
    }
  }
  bool held() const { return held_.load(); }
  void release() {
    thaw.store(true);
    thread_.join();
  }

 private:
  std::atomic<bool> held_;
  std::thread thread_;
};

// Runs the calls in order and returns 0 when each threw UsedAfterFork,
// otherwise `first` plus the index of the first that did not.
int each_throws_used_after_fork(const std::vector<std::function<void()>>& calls, int first) {
  for (size_t index = 0; index < calls.size(); ++index) {
    try {
      calls[index]();
      return first + static_cast<int>(index);
    } catch (const ThreadedClient::UsedAfterFork&) {
    } catch (...) {
      return first + static_cast<int>(index);
    }
  }
  return 0;
}

// The exit code of the child `pid`, or -1 when a signal ended it: the
// alarm, when a call waited for good.
int exit_code_of(pid_t pid) {
  int status = 0;
  if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status)) {
    return -1;
  }
  return WEXITSTATUS(status);
}

// Runs in the child: returns 0 when every check passed, otherwise the
// number of the first failed check. gtest cannot report from a forked
// child, so the parent asserts on the exit code.
int child_checks(std::unique_ptr<Client>& sync, std::unique_ptr<ThreadedClient>& threaded, unsigned short port) {
  try {
    sync->query("x", multiplexer::types::PYTHON_TEST_REQUEST, 1);
    return 1;
  } catch (Client::UsedAfterFork&) {
  }
  try {
    sync->connect("127.0.0.1", 1, 0.1f);
    return 2;
  } catch (Client::UsedAfterFork&) {
  }
  try {
    threaded->query("x", multiplexer::types::PYTHON_TEST_REQUEST, 1);
    return 3;
  } catch (ThreadedClient::UsedAfterFork&) {
  }
  try {
    threaded->send(threaded->new_message(multiplexer::types::PYTHON_TEST_REQUEST, "x"));
    return 4;
  } catch (ThreadedClient::UsedAfterFork&) {
  }
  sync.reset();      // the orphan teardown: must not hang
  threaded.reset();  // nor this one
  ThreadedClient fresh(multiplexer::peers::WEBSITE);
  if (!fresh.connect("127.0.0.1", port, 5)) {
    return 5;
  }
  if (fresh.query("x", multiplexer::types::PYTHON_TEST_REQUEST, 5).outcome != ThreadedClient::FAILED) {
    return 6;  // FAILED: connected, and nobody serves the type
  }
  return 0;
}

// Runs in a child forked while a parent thread held a lock one of its
// calls took: every call on the inherited clients and lane must throw
// UsedAfterFork without waiting for a lock, and clients made in the child
// must refuse a lane or a connection from before the fork rather than
// write on the parent's connection, as must a lane made here from such a
// connection. Returns 0, or the number of the first check that did
// otherwise; a call that waits for the held lock waits for good, until the
// alarm ends the child.
int inherited_calls_raise(Client& sync, ThreadedClient& threaded, const multiplexer::LanePtr& lane,
                          const ConnectionWrapper& connection, const Client::ScheduledMessageTracker& tracker,
                          unsigned short port) {
  alarm(10);
  multiplexer::MultiplexerMessage message;  // not const: a non-const one once chose a schedule_all that did not check
  message.set_id(1);
  message.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  message.set_message("x");
  asio::io_service timers;
  mx::SimpleTimer timer(timers, 1.0f);
  ThreadedClient fresh(multiplexer::peers::WEBSITE);
  if (!fresh.connect("127.0.0.1", port, 5)) {
    return 50;
  }
  Client fresh_sync(multiplexer::peers::WEBSITE);
  fresh_sync.connect("127.0.0.1", port, 5);
  return each_throws_used_after_fork({
                                         [&] { threaded.connections_count(); },
                                         [&] { threaded.routing_acknowledged(); },
                                         [&] { threaded.flush_all(0.1f); },
                                         [&] { threaded.query(message, 1); },
                                         [&] { threaded.send(message, 1.0f); },
                                         [&] { threaded.send(message); },
                                         [&] { threaded.connect("127.0.0.1", port, 0.1f); },
                                         [&] { lane->connection(); },
                                         [&] { lane->holds_connection(); },
                                         [&] { lane->closed(); },
                                         [&] { sync.connections_count(); },
                                         [&] { sync.has_incoming_messages(); },
                                         [&] { sync.routing_acknowledged(); },
                                         [&] { sync.query("x", multiplexer::types::PYTHON_TEST_REQUEST, 1); },
                                         [&] { sync.schedule_all(message); },
                                         [&] { sync.flush(tracker, timer); },
                                         [&] { std::make_shared<multiplexer::Lane>(connection)->connection(); },
                                         [&] { std::make_shared<multiplexer::Lane>()->adopt(connection); },
                                         [&] { fresh.send(message, lane); },
                                         [&] { fresh.send(message, connection); },
                                         [&] { fresh.send(message, connection, 1.0f); },
                                         [&] { fresh.query(message, connection, 1); },
                                         [&] { fresh_sync.schedule_one(message, connection); },
                                         [&] { fresh_sync.send(message, connection, 1); },
                                         [&] { fresh_sync.query(message, connection, 1); },
                                     },
                                     1);
}

// How many threads of this process sleep in a futex wait on an address in
// [begin, end), from /proc: a worker waiting on a condition variable of
// the object there.
int sleeping_inside(const char* begin, const char* end) {
  int count = 0;
  DIR* tasks = opendir("/proc/self/task");
  if (!tasks) {
    return -1;
  }
  while (const dirent* task = readdir(tasks)) {
    if (task->d_name[0] == '.') {
      continue;
    }
    std::ifstream syscall(std::string("/proc/self/task/") + task->d_name + "/syscall");
    long number = -1;
    std::string address;
    if (syscall >> number >> address && number == SYS_futex) {
      const std::uintptr_t futex = std::stoull(address, nullptr, 16);
      if (futex >= reinterpret_cast<std::uintptr_t>(begin) && futex < reinterpret_cast<std::uintptr_t>(end)) {
        ++count;
      }
    }
  }
  closedir(tasks);
  return count;
}

// A threaded backend that answers nothing, for its own lifecycle.
class Idle : public multiplexer::backend::BaseThreadedMultiplexerServer {
 public:
  Idle(unsigned short port, unsigned int workers)
      : BaseThreadedMultiplexerServer({{"127.0.0.1", port}}, multiplexer::peers::PYTHON_TEST_SERVER, options(workers)) {
  }

 protected:
  void handle_message(const multiplexer::backend::RequestPtr& request) override { request->no_response(); }

 private:
  static Options options(unsigned int workers) {
    Options options;
    options.workers = workers;
    return options;
  }
};

}  // namespace

TEST(Fork, InheritedClientsAreOrphansAndTheParentKeepsWorking) {
  InProcessMultiplexer mx;
  std::unique_ptr<Client> sync(new Client(multiplexer::peers::WEBSITE));
  sync->connect("127.0.0.1", mx.port, 5);
  std::unique_ptr<ThreadedClient> threaded(new ThreadedClient(multiplexer::peers::WEBSITE));
  ASSERT_TRUE(threaded->connect("127.0.0.1", mx.port, 5));
  // Fork only once the client's io thread and the multiplexer's are idle:
  // one inside the allocator at the fork leaves its lock taken in the
  // child, where an AddressSanitizer build waits on it forever (glibc's
  // malloc takes its locks across a fork; the sanitizer's does not). A
  // round trip through each thread lets it finish what it was doing.
  threaded->connections_count();
  std::promise<void> served;
  mx.io_service.post([&served] { served.set_value(); });
  served.get_future().wait();

  pid_t pid = fork();
  ASSERT_NE(-1, pid);
  if (pid == 0) {
    _exit(child_checks(sync, threaded, mx.port));
  }

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

// A parent thread inside a call, holding a lock the call took, at the
// instant of the fork: the child's calls on what it inherited must not
// wait for that lock. Frozen inside each kind of call in turn, one fork
// each, in the first lock it takes: asio's, taken to post to the io
// thread or, by a query, to make its timer; and the lane's. Not under
// AddressSanitizer, whose allocator a fork leaves locked in the child
// whenever another thread is allocating; the hazard here is normal
// builds'.
TEST(Fork, InheritedCallsRaiseWhileAParentThreadHoldsALock) {
#if defined(MX_FORK_TEST_ASAN)
  GTEST_SKIP() << "the sanitizer's allocator is not fork-safe";
#endif
  InProcessMultiplexer mx;
  Client sync(multiplexer::peers::WEBSITE);
  const ConnectionWrapper connection = sync.connect("127.0.0.1", mx.port, 5);
  ThreadedClient threaded(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(threaded.connect("127.0.0.1", mx.port, 5));
  multiplexer::MultiplexerMessage message;
  message.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  message.set_message("x");
  multiplexer::LanePtr lane(new multiplexer::Lane());
  ASSERT_EQ(1u, threaded.send(message, lane, 5));  // the lane holds a connection now
  // Queued and never written: nothing runs the synchronous client's loop.
  const Client::ScheduledMessageTracker tracker = sync.schedule_one(message);
  const std::vector<std::tuple<const char*, int, std::function<void()>>> holders = {
      {"connections_count", 1, [&] { threaded.connections_count(); }},
      {"flush_all", 1, [&] { threaded.flush_all(1); }},
      {"query", 1, [&] { threaded.query(message, 1); }},
      {"a flushing send", 1, [&] { threaded.send(message, 1.0f); }},
      {"a send", 1, [&] { threaded.send(message); }},
      {"the lane", 1, [&] { lane->connection(); }},
  };
  for (const auto& holder : holders) {
    Frozen frozen_call(std::get<1>(holder), std::get<2>(holder));
    pid_t pid = fork();
    ASSERT_NE(-1, pid);
    if (pid == 0) {
      _exit(inherited_calls_raise(sync, threaded, lane, connection, tracker, mx.port));
    }
    frozen_call.release();
    EXPECT_TRUE(frozen_call.held()) << std::get<0>(holder) << ": the call took fewer locks than the test holds";
    const int code = exit_code_of(pid);
    EXPECT_NE(-1, code) << std::get<0>(holder) << ": a call waited for the held lock until the alarm";
    EXPECT_EQ(0, code) << std::get<0>(holder) << ": the first check that did not throw UsedAfterFork";
  }
}

// The child's copies of the parent's sockets are closed once, however many
// of shutdown() and the destructor run: a second close would close what
// the child opened since under the same numbers, a new client's socket or
// timer. The child shuts both clients down, opens files until it has
// every low number the copies had, then drops the clients.
TEST(Fork, AnInheritedClientClosesTheChildsCopiesOnce) {
  InProcessMultiplexer mx;
  std::unique_ptr<Client> sync(new Client(multiplexer::peers::WEBSITE));
  sync->connect("127.0.0.1", mx.port, 5);
  std::unique_ptr<ThreadedClient> threaded(new ThreadedClient(multiplexer::peers::WEBSITE));
  ASSERT_TRUE(threaded->connect("127.0.0.1", mx.port, 5));
  threaded->connections_count();  // the io threads idle at the fork, as in the first test
  std::promise<void> served;
  mx.io_service.post([&served] { served.set_value(); });
  served.get_future().wait();

  pid_t pid = fork();
  ASSERT_NE(-1, pid);
  if (pid == 0) {
    alarm(10);
    sync->shutdown();
    threaded->shutdown();
    std::vector<int> opened;
    for (int index = 0; index < 64; ++index) {
      opened.push_back(open("/dev/null", O_RDONLY));
    }
    sync.reset();
    threaded.reset();
    for (size_t index = 0; index < opened.size(); ++index) {
      if (opened[index] < 0 || fcntl(opened[index], F_GETFD) == -1) {
        _exit(1 + static_cast<int>(index));
      }
    }
    _exit(0);
  }
  EXPECT_EQ(0, exit_code_of(pid)) << "the first file of the child's that a client closed, counting from 1";
  EXPECT_EQ(1u, threaded->connections_count());
  EXPECT_EQ(1u, sync->connections_count());
}

// A child that makes a fresh client before it drops the inherited one, as a
// post-fork hook assigning a new client does: glibc gives the fresh io
// thread the handle the parent's io thread had, which the inherited
// client's teardown must leave alone, where it detached it and the fresh
// client's shutdown() then aborted the child.
TEST(Fork, AFreshClientOutlivesTheInheritedOnesTeardown) {
  InProcessMultiplexer mx;
  std::unique_ptr<ThreadedClient> threaded(new ThreadedClient(multiplexer::peers::WEBSITE));
  ASSERT_TRUE(threaded->connect("127.0.0.1", mx.port, 5));
  threaded->connections_count();  // the io thread idle at the fork, as in the first test

  pid_t pid = fork();
  ASSERT_NE(-1, pid);
  if (pid == 0) {
    alarm(10);
    ThreadedClient fresh(multiplexer::peers::WEBSITE);
    if (!fresh.connect("127.0.0.1", mx.port, 5)) {
      _exit(2);
    }
    threaded.reset();  // the inherited one, after the fresh one started its io thread
    fresh.shutdown();
    _exit(0);
  }
  EXPECT_EQ(0, exit_code_of(pid)) << "2: the fresh client never connected; otherwise killed or aborted";
  EXPECT_EQ(1u, threaded->connections_count());
}

// A child that makes a fresh client before it drops an inherited threaded
// backend: glibc may give the fresh io thread the handle one of the
// parent's workers had, which the backend's teardown must leave alone,
// where it detached them and the fresh client's shutdown() then aborted
// the child.
TEST(Fork, AFreshClientOutlivesAnInheritedServersTeardown) {
#if defined(MX_FORK_TEST_ASAN)
  GTEST_SKIP() << "the sanitizer's allocator is not fork-safe";
#endif
  InProcessMultiplexer mx;
  const int workers = 4;
  std::unique_ptr<Idle> server(new Idle(mx.port, workers));
  server->connect();
  const char* object = reinterpret_cast<const char*>(server.get());
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (sleeping_inside(object, object + sizeof(Idle)) < workers) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "the workers never waited on the condition variable";
    sched_yield();
  }

  pid_t pid = fork();
  ASSERT_NE(-1, pid);
  if (pid == 0) {
    alarm(10);
    ThreadedClient fresh(multiplexer::peers::WEBSITE);
    if (!fresh.connect("127.0.0.1", mx.port, 5)) {
      _exit(2);
    }
    server.reset();  // the inherited one, after the fresh one started its io thread
    fresh.shutdown();
    _exit(0);
  }
  EXPECT_EQ(0, exit_code_of(pid)) << "2: the fresh client never connected; otherwise killed or aborted";
}

// A threaded backend a forked child inherited, while its workers waited on
// its condition variable and a parent thread held its mutex: each call
// throws UsedAfterFork without waiting for the mutex, stop() only clears
// `working`, and the destructor returns, where glibc's
// pthread_cond_destroy would wait for the workers for good.
TEST(Fork, AnInheritedThreadedServerRaisesAndGoesAway) {
#if defined(MX_FORK_TEST_ASAN)
  GTEST_SKIP() << "the sanitizer's allocator is not fork-safe";
#endif
  InProcessMultiplexer mx;
  const int workers = 2;
  std::unique_ptr<Idle> server(new Idle(mx.port, workers));
  server->connect();
  const char* object = reinterpret_cast<const char*>(server.get());
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (sleeping_inside(object, object + sizeof(Idle)) < workers) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "the workers never waited on the condition variable";
    sched_yield();
  }
  Frozen frozen_call(1, [&] { server->pending(); });
  pid_t pid = fork();
  ASSERT_NE(-1, pid);
  if (pid == 0) {
    alarm(10);
    const int failed = each_throws_used_after_fork({
                                                       [&] { server->close(); },
                                                       [&] { server->pending(); },
                                                       [&] { server->connect(); },
                                                       [&] { server->serve_forever(0.1f); },
                                                   },
                                                   1);
    if (failed == 0) {
      server->stop();
      server.reset();
    }
    _exit(failed);
  }
  frozen_call.release();
  EXPECT_TRUE(frozen_call.held()) << "pending() took no lock";
  const int code = exit_code_of(pid);
  EXPECT_NE(-1, code) << "a call, or the destructor, waited until the alarm";
  EXPECT_EQ(0, code) << "the first call that did not throw UsedAfterFork";
  server->close();
}
