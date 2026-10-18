// multiplexer::Monitor: the checks posted to the loops, the snapshot asked
// of the main loop, the process's figures read from /proc, and the answers
// to /healthz, /readyz and /metrics; see monitor.h.
#include "multiplexer/monitor.h"

#include <dirent.h>
#include <sys/resource.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <utility>

#include "lib/mutex.h"
#include "lib/release.h"
#include "lib/repr.h"
#include "lib/seconds.h"

namespace multiplexer {

using mx::repr;

namespace {

// A label's value as the text format quotes it.
std::string quoted(const std::string& value) {
  std::string text = "\"";
  for (const char c : value) {
    if (c == '\\' || c == '"') {
      text += '\\';
      text += c;
    } else if (c == '\n') {
      text += "\\n";
    } else {
      text += c;
    }
  }
  return text + "\"";
}

// A metric's HELP and TYPE lines.
void family(std::string& out, const char* name, const char* type, const char* help) {
  out += std::string("# HELP ") + name + " " + help + "\n# TYPE " + name + " " + type + "\n";
}

// `value` printed with `format`, as the text format takes a number.
std::string number(const char* format, double value) {
  char text[64];
  std::snprintf(text, sizeof text, format, value);
  return text;
}

// The process's start, in seconds since the epoch, from /proc: its start
// in clock ticks since the boot, /proc/self/stat's 22nd field, and the
// boot's time, /proc/stat's btime; now where /proc says neither.
double process_start_time() {
  const double now = std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();
  std::ifstream self("/proc/self/stat");
  std::string stat;
  std::getline(self, stat);
  const std::size_t name_end = stat.rfind(')');  // the name in parentheses may hold anything
  if (name_end == std::string::npos) {
    return now;
  }
  std::istringstream fields(stat.substr(name_end + 1));
  std::string field;
  unsigned long long ticks = 0;
  for (int index = 3; index <= 22 && fields >> field; ++index) {
    if (index == 22) {
      ticks = std::strtoull(field.c_str(), nullptr, 10);
    }
  }
  const long per_second = ::sysconf(_SC_CLK_TCK);
  std::ifstream system("/proc/stat");
  std::string line;
  while (std::getline(system, line)) {
    if (line.compare(0, 6, "btime ") == 0 && ticks && per_second > 0) {
      return std::strtod(line.c_str() + 6, nullptr) + static_cast<double>(ticks) / per_second;
    }
  }
  return now;
}

// The process's resident memory, from /proc/self/statm's second field, in
// pages; 0 where /proc does not say.
double resident_bytes() {
  std::ifstream statm("/proc/self/statm");
  unsigned long long size = 0, resident = 0;
  if (!(statm >> size >> resident)) {
    return 0;
  }
  return static_cast<double>(resident) * static_cast<double>(::sysconf(_SC_PAGESIZE));
}

// The descriptors open, /proc/self/fd's entries, but for the one that
// reads them; 0 where /proc does not say.
unsigned long open_descriptors() {
  DIR* directory = ::opendir("/proc/self/fd");
  if (!directory) {
    return 0;
  }
  unsigned long count = 0;
  while (const struct dirent* entry = ::readdir(directory)) {
    if (entry->d_name[0] != '.') {
      ++count;
    }
  }
  ::closedir(directory);
  return count ? count - 1 : 0;
}

}  // namespace

// What the loops answer into, shared with everything posted. By loop: the
// number of the last check posted to it and of the last it ran, equal when
// it has run every one, when the last was posted, and how long the last it
// ran had waited. Of the main loop: the number of the last snapshot asked
// for and of the last taken, and the snapshot.
struct Monitor::Answers {
  explicit Answers(unsigned int loops) : posted(loops, 0), ran(loops, 0), posted_at(loops), waited(loops) {}
  mx::Mutex mutex;
  std::condition_variable_any changed;
  std::vector<std::uint64_t> posted MX_GUARDED_BY(mutex);
  std::vector<std::uint64_t> ran MX_GUARDED_BY(mutex);
  std::vector<std::chrono::steady_clock::time_point> posted_at MX_GUARDED_BY(mutex);
  std::vector<std::chrono::steady_clock::duration> waited MX_GUARDED_BY(mutex);
  std::uint64_t asked MX_GUARDED_BY(mutex) = 0;
  std::uint64_t taken MX_GUARDED_BY(mutex) = 0;
  Server::Metrics snapshot MX_GUARDED_BY(mutex);
  bool stopping MX_GUARDED_BY(mutex) = false;  // stop(): a round under way returns
};

Monitor::Monitor(Server::pointer server, const asio::ip::tcp::endpoint& endpoint)
    : server_(std::move(server)),
      answers_(std::make_shared<Answers>(server_->loop_count())),
      started_at_(process_start_time()),
      http_(endpoint,
            mx::HttpTypedHandler([this](const std::string& target, std::string& body, std::string& content_type) {
              return _answer(target, body, content_type);
            })) {}

Monitor::~Monitor() { stop(); }

void Monitor::stop() {
  {
    mx::MutexLock lock(answers_->mutex);
    answers_->stopping = true;
  }
  answers_->changed.notify_all();
  http_.stop();
}

int Monitor::_answer(const std::string& target, std::string& body, std::string& content_type) {
  const std::string path = target.substr(0, target.find('?'));
  if (path == "/metrics") {
    const Checked checked = _check(true);
    if (!checked.snapshot) {
      body = checked.stopping ? "the monitor is stopping\n"
                              : "the main event loop did not answer within " + repr(CHECK_SECONDS) + " s\n";
      return 503;
    }
    content_type = METRICS_CONTENT_TYPE;
    body = _metrics(checked);
    return 200;
  }
  const bool readiness = path == "/readyz";
  if (!readiness && path != "/healthz") {
    body = "not found: /healthz, /readyz and /metrics are served here\n";
    return 404;
  }
  if (readiness && server_->stopped()) {
    body = "the multiplexer is stopping\n";
    return 503;
  }
  const Checked checked = _check(false);
  if (checked.unanswered.empty()) {
    body = "ok\n";
    return 200;
  }
  if (checked.stopping) {
    body = "the monitor is stopping\n";
    return 503;
  }
  if (checked.lag.size() == 1) {
    body = "the event loop did not run a check within " + repr(CHECK_SECONDS) + " s\n";
    return 503;
  }
  std::string numbers;
  for (const unsigned int loop : checked.unanswered) {
    numbers += (numbers.empty() ? "" : ", ") + repr(loop);
  }
  body = "event loop(s) " + numbers + " of " + repr(checked.lag.size()) +
         " (0 the main one) did not run a check within " + repr(CHECK_SECONDS) + " s\n";
  return 503;
}

Monitor::Checked Monitor::_check(bool snapshot) {
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
    const std::chrono::steady_clock::time_point posted_at = std::chrono::steady_clock::now();
    answers->posted_at[loop] = posted_at;
    server_->post_to_loop(loop, [answers, loop, number, posted_at] {
      const std::chrono::steady_clock::duration waited = std::chrono::steady_clock::now() - posted_at;
      {
        mx::MutexLock held(answers->mutex);
        answers->ran[loop] = number;
        answers->waited[loop] = waited;
      }
      answers->changed.notify_all();
    });
  }
  // Weakly: a snapshot still queued when the monitor is gone keeps no
  // multiplexer alive.
  if (snapshot && answers->taken == answers->asked) {
    const std::uint64_t number = ++answers->asked;
    server_->post_to_loop(0, [answers, server = Server::weak_pointer(server_), number] {
      const Server::pointer alive = server.lock();
      if (!alive) {
        return;
      }
      Server::Metrics metrics = alive->metrics();
      {
        mx::MutexLock held(answers->mutex);
        answers->snapshot = std::move(metrics);
        answers->taken = number;
      }
      answers->changed.notify_all();
    });
  }
  bool late = false;
  for (;;) {
    bool answered = true;
    for (unsigned int loop = 0; loop < loops; ++loop) {
      answered = answered && answers->ran[loop] == answers->posted[loop];
    }
    if ((answered && (!snapshot || answers->taken == answers->asked)) || answers->stopping || late) {
      break;
    }
    late = answers->changed.wait_until(lock, deadline) == std::cv_status::timeout;
  }
  Checked checked;
  checked.stopping = answers->stopping;
  const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  for (unsigned int loop = 0; loop < loops; ++loop) {
    const bool ran = answers->ran[loop] == answers->posted[loop];
    if (!ran) {
      checked.unanswered.push_back(loop);
    }
    checked.lag.push_back(
        std::chrono::duration<double>(ran ? answers->waited[loop] : now - answers->posted_at[loop]).count());
  }
  if (snapshot && answers->taken == answers->asked) {
    checked.snapshot.reset(new Server::Metrics(answers->snapshot));
  }
  return checked;
}

std::string Monitor::_metrics(const Checked& checked) const {
  const Server::Metrics& metrics = *checked.snapshot;
  std::string out;
  family(out, "mx_build_info", "gauge", "The multiplexer's version, in its label; always 1.");
  out += "mx_build_info{version=" + quoted(mx::release::version) + "} 1\n";
  family(out, "mx_rules_info", "gauge",
         "The rules in use, by the fingerprint mxcontrol rules status reports; always 1.");
  out += "mx_rules_info{fingerprint=" + quoted(metrics.rules_fingerprint) + "} 1\n";
  family(out, "mx_rules_file_failing", "gauge",
         "1 while the rules file on disk is not in use, missing or not parsing, the rules loaded before still "
         "routing; else 0.");
  out += std::string("mx_rules_file_failing ") + (metrics.rules_failing ? "1" : "0") + "\n";
  family(out, "mx_stopping", "gauge", "1 from the moment the multiplexer began to stop, its drain included; else 0.");
  out += std::string("mx_stopping ") + (metrics.stopping ? "1" : "0") + "\n";
  family(out, "mx_event_loop_lag_seconds", "gauge",
         "How long the last check posted to each event loop, 0 the main one, waited for the loop to run it; for "
         "one it has not run yet, how long it has waited so far.");
  for (std::size_t loop = 0; loop < checked.lag.size(); ++loop) {
    out +=
        "mx_event_loop_lag_seconds{loop=\"" + std::to_string(loop) + "\"} " + number("%.9f", checked.lag[loop]) + "\n";
  }
  family(out, "mx_messages_received_total", "counter", "Frames peers sent the multiplexer to route, a batch one.");
  out += "mx_messages_received_total " + std::to_string(metrics.received_messages) + "\n";
  family(out, "mx_received_bytes_total", "counter", "The bytes of those frames, their headers included.");
  out += "mx_received_bytes_total " + std::to_string(metrics.received_bytes) + "\n";
  family(out, "mx_messages_dropped_total", "counter",
         "Messages, or a peer's copy of one, that reached no peer they were routed for, by why (docs/metrics.md), "
         "their type and the peer type they were for.");
  for (const Server::Metrics::Dropped& drop : metrics.dropped) {
    out += "mx_messages_dropped_total{reason=" + quoted(drop.reason) + ",type=" + quoted(drop.type) +
           ",peer_type=" + quoted(drop.peer_type) + "} " + std::to_string(drop.count) + "\n";
  }
  family(out, "mx_peers", "gauge", "Registered connections, by peer type.");
  for (const auto& peers : metrics.peers) {
    out += "mx_peers{peer_type=" + quoted(peers.first) + "} " + std::to_string(peers.second) + "\n";
  }
  family(out, "mx_queued_messages", "gauge",
         "Frames waiting in the multiplexer's outgoing queues, by the peer type they wait for.");
  for (const auto& queued : metrics.queued) {
    out += "mx_queued_messages{peer_type=" + quoted(queued.first) + "} " + std::to_string(queued.second) + "\n";
  }
  family(out, "mx_accept_failures_total", "counter",
         "Connections the multiplexer could not accept, out of descriptors or memory, each tried again 0.1 s "
         "later.");
  out += "mx_accept_failures_total " + std::to_string(metrics.accept_failures) + "\n";
  // The process's own, under the names and words of Prometheus' client
  // libraries.
  struct rusage usage;
  double cpu = 0;
  if (::getrusage(RUSAGE_SELF, &usage) == 0) {
    cpu = usage.ru_utime.tv_sec + usage.ru_stime.tv_sec + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
  }
  struct rlimit limit;
  const double max_fds = ::getrlimit(RLIMIT_NOFILE, &limit) == 0 ? static_cast<double>(limit.rlim_cur) : 0;
  family(out, "process_start_time_seconds", "gauge", "Start time of the process since unix epoch in seconds.");
  out += "process_start_time_seconds " + number("%.3f", started_at_) + "\n";
  family(out, "process_cpu_seconds_total", "counter", "Total user and system CPU time spent in seconds.");
  out += "process_cpu_seconds_total " + number("%.6f", cpu) + "\n";
  family(out, "process_resident_memory_bytes", "gauge", "Resident memory size in bytes.");
  out += "process_resident_memory_bytes " + number("%.0f", resident_bytes()) + "\n";
  family(out, "process_open_fds", "gauge", "Number of open file descriptors.");
  out += "process_open_fds " + std::to_string(open_descriptors()) + "\n";
  family(out, "process_max_fds", "gauge", "Maximum number of open file descriptors.");
  out += "process_max_fds " + number("%.0f", max_fds) + "\n";
  return out;
}

}  // namespace multiplexer
