// multiplexer::Monitor: the checks posted to the loops and the answers to
// /healthz and /readyz; see monitor.h.
#include "multiplexer/monitor.h"

#include <chrono>
#include <condition_variable>
#include <utility>
#include <vector>

#include "lib/mutex.h"
#include "lib/repr.h"
#include "lib/seconds.h"

namespace multiplexer {

using mx::repr;

// What the loops answer into, shared with every check posted. By loop: the
// number of the last check posted to it and of the last it ran, equal when
// it has run every one.
struct Monitor::Answers {
  explicit Answers(unsigned int loops) : posted(loops, 0), ran(loops, 0) {}
  mx::Mutex mutex;
  std::condition_variable_any changed;
  std::vector<std::uint64_t> posted MX_GUARDED_BY(mutex);
  std::vector<std::uint64_t> ran MX_GUARDED_BY(mutex);
  bool stopping MX_GUARDED_BY(mutex) = false;  // stop(): a check under way returns
};

Monitor::Monitor(Server::pointer server, const asio::ip::tcp::endpoint& endpoint)
    : server_(std::move(server)),
      answers_(std::make_shared<Answers>(server_->loop_count())),
      http_(endpoint, [this](const std::string& target, std::string& body) { return _answer(target, body); }) {}

Monitor::~Monitor() { stop(); }

void Monitor::stop() {
  {
    mx::MutexLock lock(answers_->mutex);
    answers_->stopping = true;
  }
  answers_->changed.notify_all();
  http_.stop();
}

int Monitor::_answer(const std::string& target, std::string& body) {
  const std::string path = target.substr(0, target.find('?'));
  const bool readiness = path == "/readyz";
  if (!readiness && path != "/healthz") {
    body = "not found: /healthz and /readyz are served here\n";
    return 404;
  }
  if (readiness && server_->stopped()) {
    body = "the multiplexer is stopping\n";
    return 503;
  }
  const std::string stuck = _check_loops();
  if (!stuck.empty()) {
    body = stuck;
    return 503;
  }
  body = "ok\n";
  return 200;
}

std::string Monitor::_check_loops() {
  const std::shared_ptr<Answers> answers = answers_;
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + mx::from_seconds(CHECK_SECONDS);
  mx::UniqueLock lock(answers->mutex);
  const unsigned int loops = static_cast<unsigned int>(answers->posted.size());
  for (unsigned int loop = 0; loop < loops; ++loop) {
    if (answers->ran[loop] != answers->posted[loop]) {
      continue;  // the last check is still to run there
    }
    const std::uint64_t number = ++answers->posted[loop];
    server_->post_to_loop(loop, [answers, loop, number] {
      {
        mx::MutexLock held(answers->mutex);
        answers->ran[loop] = number;
      }
      answers->changed.notify_all();
    });
  }
  bool late = false;
  for (;;) {
    std::vector<unsigned int> unanswered;
    for (unsigned int loop = 0; loop < loops; ++loop) {
      if (answers->ran[loop] != answers->posted[loop]) {
        unanswered.push_back(loop);
      }
    }
    if (unanswered.empty()) {
      return "";
    }
    if (answers->stopping) {
      return "the monitor is stopping\n";
    }
    if (late) {
      if (loops == 1) {
        return "the event loop did not run a check within " + repr(CHECK_SECONDS) + " s\n";
      }
      std::string numbers;
      for (unsigned int loop : unanswered) {
        numbers += (numbers.empty() ? "" : ", ") + repr(loop);
      }
      return "event loop(s) " + numbers + " of " + repr(loops) + " (0 the main one) did not run a check within " +
             repr(CHECK_SECONDS) + " s\n";
    }
    late = answers->changed.wait_until(lock, deadline) == std::cv_status::timeout;
  }
}

}  // namespace multiplexer
