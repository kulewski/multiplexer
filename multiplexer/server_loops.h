// ServerLoops: the event loops of a multiplexer on several of them
// (Server::set_threads, run_multiplexer --threads N): the main io_service,
// which the program's thread runs and which keeps the acceptor, the signals
// and the multiplexer's own timers, and N - 1 more, each an io_service run
// by a thread of its own. Connections are dealt out over the N as they are
// accepted, round robin, the main loop included; each lives on its loop,
// which reads it, runs its timers and writes to it when a write had to
// wait (Connection's "Writing through").
//
// Used by Server alone, on the main loop's thread: next() at each accept,
// start() before the main loop runs, finish() once it has returned. The
// reactor of each extra loop, its epoll, eventfd and timerfd descriptors,
// is made with the loop: made at its first connection, as asio makes it,
// it threw at the descriptor limit out of the accept that made that
// connection, and the multiplexer stopped accepting for good.
#ifndef MX_MULTIPLEXER_SERVER_LOOPS_H_
#define MX_MULTIPLEXER_SERVER_LOOPS_H_

#include <asio/executor_work_guard.hpp>
#include <asio/io_service.hpp>
#include <memory>
#include <thread>
#include <vector>

#include "lib/thread_checker.h"

namespace multiplexer {

class ServerLoops {
 public:
  // `count` loops, the first `main`, which the caller's thread runs; count
  // is 2 at least.
  ServerLoops(asio::io_service& main, unsigned int count);
  // finish(): the loops let go and their threads joined. Never on a thread
  // of the loops' own, which could not join itself.
  ~ServerLoops();
  ServerLoops(const ServerLoops&) = delete;
  ServerLoops& operator=(const ServerLoops&) = delete;

  // How many loops, the main one included; from any thread.
  unsigned int count() const { return count_; }
  // The loop the next connection accepted lives on, round robin from the
  // main one.
  asio::io_service& next();
  // Starts the thread of each loop beside the main one.
  void start();
  // Lets every loop beside the main one run out of work, then joins its
  // thread; for once the main loop has returned, the multiplexer stopped.
  // A second call does nothing.
  void finish();

 private:
  // A loop beside the main one: its io_service, made with its reactor,
  // the work that keeps its run() going until finish(), and its thread.
  struct Loop {
    Loop();
    asio::io_service io_service;
    asio::executor_work_guard<asio::io_service::executor_type> work;
    std::thread thread;
  };
  // A loop's thread: io_service.run(), run again when a handler throws,
  // as the main loop is (mxcontrol/start_multiplexer_server.cc).
  static void _run(asio::io_service& io_service, unsigned int number);

  // The thread that runs the main loop, which every method here runs on.
  mx::ThreadChecker main_thread_;
  asio::io_service& main_;
  const unsigned int count_;
  std::vector<std::unique_ptr<Loop>> loops_ MX_GUARDED_BY(main_thread_);
  unsigned int next_ MX_GUARDED_BY(main_thread_) = 0;
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_SERVER_LOOPS_H_
