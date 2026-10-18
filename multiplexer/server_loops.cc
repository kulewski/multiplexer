// ServerLoops: the loops' threads and the round robin over the loops; see
// server_loops.h.
#include "multiplexer/server_loops.h"

#include <asio/steady_timer.hpp>
#include <exception>
#include <functional>
#include <string>

#include "lib/assertion.h"
#include "lib/logging/logging.h"
#include "lib/repr.h"

namespace multiplexer {

using namespace ::mx::logging::consts;

// The timer is made for nothing else: the first timer or socket of an
// io_service makes its reactor, which is what has to happen now.
ServerLoops::Loop::Loop() : work(asio::make_work_guard(io_service)) { asio::steady_timer reactor_now(io_service); }

ServerLoops::ServerLoops(asio::io_service& main, unsigned int count) : main_(main), count_(count) {
  MX_DCHECK_RUN_ON(&main_thread_);
  AssertMsg(count >= 2, "ServerLoops are 2 loops at least, " + mx::repr(count) + " asked for");
  services_.push_back(&main);
  for (unsigned int loop = 1; loop < count; ++loop) {
    loops_.emplace_back(new Loop());
    services_.push_back(&loops_.back()->io_service);
  }
}

// Whatever finish() left, a server let go of without it: here, on a thread
// that is none of the loops'.
ServerLoops::~ServerLoops() {
  for (const std::unique_ptr<Loop>& loop : loops_) {
    loop->work.reset();
    if (loop->thread.joinable()) {
      AssertMsg(loop->thread.get_id() != std::this_thread::get_id(),
                "a multiplexer on several loops let go of on a loop's own thread; Server::finish_loops() first");
      loop->thread.join();
    }
  }
}

asio::io_service& ServerLoops::loop(unsigned int index) const {
  AssertMsg(index < count_, "loop " + mx::repr(index) + " of " + mx::repr(count_) + " asked for");
  return *services_[index];
}

asio::io_service& ServerLoops::next() {
  MX_DCHECK_RUN_ON(&main_thread_);
  const unsigned int loop = next_;
  next_ = (next_ + 1) % count_;
  return loop == 0 ? main_ : loops_[loop - 1]->io_service;
}

void ServerLoops::start() {
  MX_DCHECK_RUN_ON(&main_thread_);
  for (std::size_t index = 0; index < loops_.size(); ++index) {
    Loop& loop = *loops_[index];
    if (!loop.thread.joinable()) {
      loop.thread = std::thread(&ServerLoops::_run, std::ref(loop.io_service), static_cast<unsigned int>(index + 1));
    }
  }
}

void ServerLoops::finish() {
  MX_DCHECK_RUN_ON(&main_thread_);
  for (const std::unique_ptr<Loop>& loop : loops_) {
    loop->work.reset();
    if (loop->thread.joinable()) {
      loop->thread.join();
    }
  }
}

// What escapes a handler is logged and the loop goes on, as the main loop
// does in run_multiplexer: one connection's fault must not stop the
// connections of a whole loop.
void ServerLoops::_run(asio::io_service& io_service, unsigned int number) {
  for (;;) {
    try {
      io_service.run();
      return;
    } catch (const std::exception& error) {
      MX_LOG(ERROR, LOWVERBOSITY,
             CTX("multiplexer.server")
                 TEXT("exception escaped an io handler of loop " + mx::repr(number) + ": " + error.what()));
    }
  }
}

}  // namespace multiplexer
