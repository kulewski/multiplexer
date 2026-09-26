// LogSummary: log lines about messages that went nowhere, at most about two
// per kind and second however many messages there are.
//
// A multiplexer whose receiver fell behind, or whose backends are all gone,
// used to log one to three lines per dropped message, each a synchronous
// write to stderr on its only io thread: thousands of lines a second, when
// it could least afford them. Now the first line of a kind is logged at
// once, by the caller, with what it knows about that message; the rest are
// counted, and about once a second one line per kind says how many more
// there were: "<the kind's text> [N more in the last 1.0 s]". A kind that
// went quiet for a whole second is forgotten, so its next line is logged at
// once again.
//
// The cost is on the failure path only: a map lookup per line counted, and
// nothing at all for a level the logging settings drop. The timer runs only
// while something is counted, so an idle process never wakes for it. Not
// thread-safe: one io thread, as the connections.
#ifndef MX_MULTIPLEXER_LOG_SUMMARY_H_
#define MX_MULTIPLEXER_LOG_SUMMARY_H_

#include <asio/io_service.hpp>
#include <asio/steady_timer.hpp>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <tuple>

#include "lib/logging/logging.h"

namespace multiplexer {

class LogSummary {
 public:
  // What tells two kinds of line apart: what the line says (a number of the
  // caller's own), its level, and the peer type and instance id it names,
  // 0 where it names none. A line's text is the same for every message of
  // its kind.
  struct Kind {
    unsigned int reason;
    unsigned int level;
    std::uint32_t type;
    std::uint64_t peer;
    bool operator<(const Kind& other) const {
      return std::tie(reason, level, type, peer) < std::tie(other.reason, other.level, other.type, other.peer);
    }
  };

  // At most this many kinds are counted at once; lines of further kinds go
  // into one count of their own, said as "[N more lines of other kinds in
  // the last 1.0 s]".
  static const unsigned int MAX_KINDS = 256;
  // How often the counts are said, in seconds.
  static constexpr float INTERVAL = 1.0;
  // The verbosity of every line about a dropped message, the first and the
  // summaries alike.
  static const unsigned int VERBOSITY = HIGHVERBOSITY;

  // `context` goes into the summary lines' context, as CTX() does.
  LogSummary(asio::io_service& io_service, const std::string& context);
  ~LogSummary();
  LogSummary(const LogSummary&) = delete;
  LogSummary& operator=(const LogSummary&) = delete;

  // Counts one line of `kind`. The first of its kind returns the text
  // `make_text()` gave, kept for the summary of the rest, and the caller
  // logs its line about this message now. The rest return null: counted,
  // to be said in the summary. Null too, counting nothing, when the
  // logging settings drop `kind.level` at VERBOSITY.
  template <typename MakeText>
  const std::string* first(const Kind& kind, MakeText make_text) {
    if (!::mx::logging::impl::should_log(kind.level, VERBOSITY)) {
      return nullptr;
    }
    State& state = *state_;
    std::map<Kind, State::Count>::iterator found = state.counts.find(kind);
    if (found != state.counts.end()) {
      ++found->second.more;
      return nullptr;
    }
    return _add(kind, make_text());
  }

  // Says every count now and forgets it, for an owner that stops: the
  // timer would otherwise keep its io loop alive for a second longer, and
  // whatever it counted would never be said.
  void flush();

 private:
  // The counts and the timer, shared with the timer's handler, which holds
  // them weakly: a handler that runs after its owner is gone does nothing.
  struct State : public std::enable_shared_from_this<State> {
    State(asio::io_service& io_service, const std::string& log_context) : timer(io_service), context(log_context) {}

    struct Count {
      std::string text;
      std::uint64_t more = 0;
      std::chrono::steady_clock::time_point since;  // the first line, or the last summary
    };
    std::map<Kind, Count> counts;
    std::uint64_t other = 0;  // lines of kinds beyond MAX_KINDS
    unsigned int other_level = 0;
    std::chrono::steady_clock::time_point other_since;
    asio::steady_timer timer;
    bool armed = false;
    std::string context;

    // Logs every count above zero and resets it; with `forget`, drops
    // every kind, else only the ones that had nothing to say.
    void say(bool forget);
    // Starts the wait for the next summary unless one is pending.
    void arm();
  };

  const std::string* _add(const Kind& kind, std::string text);

  std::shared_ptr<State> state_;
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_LOG_SUMMARY_H_
