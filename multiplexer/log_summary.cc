// LogSummary: the counting and the summary lines; see log_summary.h.
#include "multiplexer/log_summary.h"

#include <cstdio>

#include "lib/repr.h"
#include "lib/seconds.h"

namespace multiplexer {

namespace {

// " [N more in the last 1.0 s]", or with `what` in place of "more".
std::string more_text(std::uint64_t count, const std::string& what, std::chrono::steady_clock::duration elapsed) {
  char seconds[32];
  std::snprintf(seconds, sizeof(seconds), "%.1f", std::chrono::duration<double>(elapsed).count());
  return " [" + mx::repr(count) + " " + what + " in the last " + seconds + " s]";
}

}  // namespace

LogSummary::LogSummary(asio::io_service& io_service, const std::string& context)
    : state_(std::make_shared<State>(io_service, context)) {}

LogSummary::~LogSummary() {
  asio::error_code ignored;
  state_->timer.cancel(ignored);
}

void LogSummary::flush() {
  asio::error_code ignored;
  state_->timer.cancel(ignored);
  state_->armed = false;
  state_->say(/*forget=*/true);
}

const std::string* LogSummary::_add(const Kind& kind, std::string text) {
  State& state = *state_;
  if (state.counts.size() >= MAX_KINDS) {
    if (!state.other) {
      state.other_since = std::chrono::steady_clock::now();
    }
    ++state.other;
    state.other_level = std::max(state.other_level, kind.level);
    state.arm();
    return nullptr;
  }
  State::Count& count = state.counts[kind];
  count.text = std::move(text);
  count.since = std::chrono::steady_clock::now();
  state.arm();
  return &count.text;
}

// One timer for every kind, armed while anything is counted; each tick
// says the counts and forgets the kinds that had nothing to say, and waits
// again while any kind is left.
void LogSummary::State::arm() {
  if (armed) {
    return;
  }
  armed = true;
  timer.expires_after(mx::from_seconds(INTERVAL));
  timer.async_wait([weak = weak_from_this()](const asio::error_code& error) {
    std::shared_ptr<State> state = weak.lock();
    if (error == asio::error::operation_aborted || !state) {
      return;
    }
    state->armed = false;
    state->say(/*forget=*/false);
    if (!state->counts.empty() || state->other) {
      state->arm();
    }
  });
}

void LogSummary::State::say(bool forget) {
  const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  for (std::map<Kind, Count>::iterator entry = counts.begin(); entry != counts.end();) {
    Count& count = entry->second;
    if (count.more) {
      MX_LOG(entry->first.level, VERBOSITY,
             CTX(context) TEXT(count.text + more_text(count.more, "more", now - count.since)));
      count.more = 0;
      count.since = now;
    } else if (!forget) {
      entry = counts.erase(entry);  // a whole interval without one: logged at once next time
      continue;
    }
    ++entry;
  }
  if (other) {
    MX_LOG(other_level, VERBOSITY, CTX(context) TEXT(more_text(other, "more lines of other kinds", now - other_since)));
    other = 0;
    other_level = 0;
  }
  if (forget) {
    counts.clear();
  }
}

}  // namespace multiplexer
