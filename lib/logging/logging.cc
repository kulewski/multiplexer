// Logging runtime: the verbosity table, the stderr text form, the optional
// binary stream (length-prefixed LogEntry records via lib/protobuf/stream.h),
// and the process context (hostname.program[.suffix]).
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <vector>
// #include <google/protobuf/io/zero_copy_stream_impl.h>
// #include <google/protobuf/io/coded_stream.h>
// #include <google/protobuf/wire_format.h>
// #include <google/protobuf/wire_format_inl.h>
#include "lib/assertion.h"
#include "lib/initialization.h"
#include "lib/logging/logging.h"
#include "lib/mutex.h"
#include "lib/protobuf/stream.h"
#include "lib/random.h"
#include "lib/repr.h"

namespace mx {
namespace logging {

bool module_is_initialized = false;

namespace impl {

// The binary stream and the lock each use of it takes, a write, a flush and
// a replacement, so that two threads logging at once, an io thread and the
// program's own, never write into one buffer or into one being freed.
// Taken only while a stream is set: with streaming off a log line costs
// one load of stream_set_.
static mx::Mutex stream_mutex_;
static std::unique_ptr<mx::protobuf::FileMessageOutputStream> message_output_stream_ MX_GUARDED_BY(stream_mutex_);
// Entries the stream's file did not take, in the run of failures under way.
static std::uint64_t stream_entries_dropped_ MX_GUARDED_BY(stream_mutex_) = 0;
static std::atomic<bool> stream_set_{false};

// Null until the library sets its default; constant-initialized, as the
// table below.
std::atomic<const ProcessContext*> process_context_{nullptr};

// A new node for `text`, on top of the one it replaces, which stays.
void publish_process_context(const std::string& text) {
  ProcessContext* next = new ProcessContext{text, process_context_.load(std::memory_order_relaxed)};
  while (!process_context_.compare_exchange_weak(next->replaced, next, std::memory_order_release,
                                                 std::memory_order_relaxed)) {
  }
}

// One entry per level, index 0 unused; constant-initialized so that a log
// line during another translation unit's static initialization is safe.
static_assert(MAX_LEVEL == 6, "the table below has one entry per level");
std::atomic<unsigned int> maximal_logging_verbosity[MAX_LEVEL + 1] = {
    {MX_LOGGING_DEFAULT_VERBOSITY()}, {MX_LOGGING_DEFAULT_VERBOSITY()}, {MX_LOGGING_DEFAULT_VERBOSITY()},
    {MX_LOGGING_DEFAULT_VERBOSITY()}, {MX_LOGGING_DEFAULT_VERBOSITY()}, {MX_LOGGING_DEFAULT_VERBOSITY()},
    {MX_LOGGING_DEFAULT_VERBOSITY()}};

static std::string hostname;
static std::string process_name;

/*
 * enum controlling the state of process_context_ variable
 */
enum ContextAutoStateEnum {
  NOTHING,
  SET_WITH_ALL_DEFAULTS,
  SET_WITH_DEFAULT_NAME,
  DURING_INITIALIZATION
} process_context_state_ = NOTHING;

/*
 * initialize_hostname()
 */
static inline void initialize_hostname() {
  std::string hostname;
  const unsigned int hostname_len_max = 1024;
  for (unsigned int hostname_len = 256; hostname_len < hostname_len_max + 1; hostname_len *= 2) {
    hostname.resize(hostname_len);
    if (gethostname(&hostname[0], hostname_len) == 0) {
      if (hostname_len < hostname_len_max && strlen(hostname.c_str()) >= hostname_len - 1) {
        continue;
      }
      impl::hostname = hostname.c_str();
      break;
    } else {
      perror("logging::impl::initialize_process_context_all_defaults");
      exit(EXIT_FAILURE);
    }
  }
}
MX_TRIGGER_STATIC_INITIALIZATION(initialize_hostname(), hostname.empty());

// The program's name in the log context: the basename of argv[0], as glibc
// keeps it from before any static initializer runs, spaces and all. It
// read /proc/<getpid()>/cmdline, which in a PID namespace without its own
// /proc is another process's or none, the process then exiting as the
// library loaded, and cut the name at a space. An empty argv[0], or one
// ending in '/', is "unknown", where the assertion below ended the process.
static inline void initialize_process_name() {
  process_name = program_invocation_short_name;
  if (process_name.empty()) {
    process_name = "unknown";
  }
}
MX_TRIGGER_STATIC_INITIALIZATION(initialize_process_name(), process_name.empty());

/*
 * initialize_process_context_all_defaults)
 *	    set process_context_ to <hostname>.<processname> or die
 */
static inline void initialize_process_context_all_defaults() {
  Assert(process_context_state_ != DURING_INITIALIZATION);
  // set process_context_ temporarily
  publish_process_context("<unknown>");
  process_context_state_ = DURING_INITIALIZATION;

  // trigger initialization if not yet triggered
  if (hostname.empty()) {
    initialize_hostname();
  }
  if (process_name.empty()) {
    initialize_process_name();
  }
  Assert(!hostname.empty());
  Assert(!process_name.empty());

  // build the process context
  publish_process_context(hostname + "." + process_name);
  process_context_state_ = SET_WITH_ALL_DEFAULTS;
}
MX_TRIGGER_STATIC_INITIALIZATION(initialize_process_context_all_defaults(), process_context_state_ == NOTHING);

// Flushes and frees the stream set, closing its descriptor if it owns it,
// and sets `stream` in its place, null for none.
static void replace_stream(mx::protobuf::FileMessageOutputStream* stream) MX_REQUIRES(stream_mutex_) {
  if (message_output_stream_) {
    message_output_stream_->flush();
  }
  message_output_stream_.reset(stream);
  stream_set_.store(stream != nullptr, std::memory_order_relaxed);
}

// The entries whose text stderr did not take since a line last got
// through, which the next one that does says first.
static std::atomic<std::uint64_t> text_entries_lost_{0};

// The stderr form of `log_msg` at `level`, one line, `data` after it when
// given: built in one string, which a stream formatting each piece would
// make slower.
static std::string text_of(unsigned int level, const LogEntry& log_msg, const std::string* data) {
  std::string text;
  text.reserve(96 + log_msg.context().size() + log_msg.workflow().size() + log_msg.text().size() +
               log_msg.source_file().size() + (data ? data->size() + 2 : 0));
  text.append("[").append(logging_get_level_name(level)).append("]");
  text.append("  ts=").append(std::to_string(log_msg.timestamp()));
  text.append("  pid=").append(std::to_string(log_msg.pid()));
  text.append("  ctx=").append(log_msg.context());
  text.append("  flw=\"").append(log_msg.workflow()).append("\"");
  text.append("  txt=\"").append(log_msg.text()).append("\"");
  if (log_msg.has_source_file()) {
    text.append("  from=").append(log_msg.source_file());
    if (log_msg.has_source_line()) {
      text.append(":").append(std::to_string(log_msg.source_line()));
    }
  }
  text.append("\n");
  if (data) {
    text.append(*data).append("\n\n");
  }
  return text;
}

// Writes `text` whole to stderr: false at the first error but EINTR, which
// a signal's handler causes and is tried again, as is what a partial write
// left. No lock: a thread holding one at a fork would leave a child's log
// waiting for good; a line of a few KiB is one write, which a pipe keeps
// whole.
static bool write_whole(const std::string& text) {
  const char* at = text.data();
  std::size_t left = text.size();
  while (left) {
    const ssize_t written = ::write(STDERR_FILENO, at, left);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    at += written;
    left -= static_cast<std::size_t>(written);
  }
  return true;
}

// A line stderr does not take, a full pipe that does not block or a full
// disk say, is dropped and counted, and the next that gets through says
// how many were, at ERROR, ahead of it in the same write. It went through
// std::cerr, which one failed write left failing for good, every later
// line of the process dropped unsaid.
void _emit_text(unsigned int level, const LogEntry& log_msg, const std::string* data) {
  std::string text = text_of(level, log_msg, data);
  const std::uint64_t lost = text_entries_lost_.exchange(0, std::memory_order_relaxed);
  if (lost) {
    LogEntry notice;
    notice.set_timestamp(current_timestamp());
    notice.set_pid(getpid());
    notice.set_context(current_process_context());
    notice.set_level(ERROR);
    notice.set_text(mx::repr(lost) + " log entries lost: stderr did not take them");
    text.insert(0, text_of(ERROR, notice, nullptr));
  }
  if (!write_whole(text)) {
    text_entries_lost_.fetch_add(lost + 1, std::memory_order_relaxed);
  }
}

void _emit_log(const LogEntry& log_msg) {
  if (!mx::logging::module_is_initialized) {
    std::cerr << "Warning: mx::logging::_emit_log called before "
                 "module_is_initialized. LogEntry ignored.\n";
    return;
  }
  if (!stream_set_.load(std::memory_order_relaxed)) {
    return;  // no logging stream set
  }
  enum { DROPPING, TAKEN_AGAIN, GONE } outcome;
  int error = 0;
  std::uint64_t dropped = 0;
  {
    mx::MutexLock locked(stream_mutex_);
    if (!message_output_stream_) {
      return;  // shut down since the load above
    }
    message_output_stream_->write(log_msg);
    message_output_stream_->flush();
    error = message_output_stream_->error();
    if (!error) {
      if (!stream_entries_dropped_) {
        return;
      }
      dropped = stream_entries_dropped_;
      stream_entries_dropped_ = 0;
      outcome = TAKEN_AGAIN;
    } else if (error != EPIPE && message_output_stream_->cut_back()) {
      // A regular file that took part of the entry, full say: cut back to
      // the last whole entry, so that the ones after are read, where every
      // entry after the part was lost to its reader. Said when the
      // dropping begins and when it ends.
      if (stream_entries_dropped_++) {
        return;
      }
      outcome = DROPPING;
    } else {
      // The reader is gone for good, a log shipper or the other end of a
      // pipe, or a pipe or a socket took part of the entry, after which its
      // reader finds no next one: the stream is dropped, once, and the
      // text on stderr goes on.
      replace_stream(nullptr);
      outcome = GONE;
    }
  }
  // Said once the lock is let go: the line goes to the stream too.
  const std::string why = std::system_category().message(error);
  switch (outcome) {
    case DROPPING:
      MX_LOG(WARNING, LOWVERBOSITY,
             CTX("logging") TEXT("the binary log stream could not take an entry (" + why +
                                 "): its file is cut back to the last whole entry, and entries are dropped until it "
                                 "takes them again"));
      break;
    case TAKEN_AGAIN:
      MX_LOG(WARNING, LOWVERBOSITY,
             CTX("logging") TEXT("the binary log stream takes entries again; " + repr(dropped) + " were dropped"));
      break;
    case GONE:
      MX_LOG(WARNING, LOWVERBOSITY,
             CTX("logging")
                 TEXT(error == EPIPE ? std::string("the binary log stream's reader is gone (EPIPE): no more entries "
                                                   "are written to it")
                                     : "the binary log stream took part of an entry (" + why +
                                           "): no more entries are written to it, as its reader could not find "
                                           "the next one"));
      break;
  }
}

};  // namespace impl

using namespace impl;

void set_maximal_logging_verbosity(const unsigned int for_level, const unsigned int minimal_verbosity) {
  Assert(for_level <= MAX_LEVEL);
  maximal_logging_verbosity[for_level].store(minimal_verbosity, std::memory_order_relaxed);
}

namespace {

std::string upper(std::string text) {
  for (char& character : text) {
    character = static_cast<char>(toupper(static_cast<unsigned char>(character)));
  }
  return text;
}

// The verbosity a name means: "HIGH" or "HIGHVERBOSITY", any case; -1 when none does.
int verbosity_named(const std::string& name) {
  std::string wanted = upper(name);
  for (unsigned int verbosity = 0; verbosity <= MAX_VERBOSITY; ++verbosity) {
    std::string full = consts::logging_get_verbosity_name(verbosity);
    std::string base =
        full.size() > 9 && full.substr(full.size() - 9) == "VERBOSITY" ? full.substr(0, full.size() - 9) : full;
    if (wanted == full || wanted == base) {
      return static_cast<int>(verbosity);
    }
  }
  return -1;
}

int level_named(const std::string& name) {
  std::string wanted = upper(name);
  for (unsigned int level = 0; level <= MAX_LEVEL; ++level) {
    if (wanted == consts::logging_get_level_name(level)) {
      return static_cast<int>(level);
    }
  }
  return -1;
}

std::string trimmed(const std::string& text) {
  std::string::size_type begin = text.find_first_not_of(" \t");
  if (begin == std::string::npos) {
    return std::string();
  }
  return text.substr(begin, text.find_last_not_of(" \t") - begin + 1);
}

}  // namespace

bool apply_verbosity_spec(const std::string& spec, std::string* error) {
  // Parsed whole before anything is set, so a bad spec changes nothing.
  std::vector<std::pair<int, int>> settings;  // (level, verbosity); level -1 means every level
  std::string::size_type start = 0;
  while (start <= spec.size()) {
    std::string::size_type comma = spec.find(',', start);
    std::string item = trimmed(spec.substr(start, comma == std::string::npos ? std::string::npos : comma - start));
    start = comma == std::string::npos ? spec.size() + 1 : comma + 1;
    if (item.empty()) {
      continue;
    }
    std::string::size_type colon = item.find(':');
    int level = -1;
    std::string verbosity_name = item;
    if (colon != std::string::npos) {
      level = level_named(trimmed(item.substr(0, colon)));
      if (level < 0) {
        if (error) {
          *error = "unknown level in '" + item + "'; one of DEBUG, INFO, OK, WARNING, ERROR, CRITICAL";
        }
        return false;
      }
      verbosity_name = trimmed(item.substr(colon + 1));
    }
    int verbosity = verbosity_named(verbosity_name);
    if (verbosity < 0) {
      if (error) {
        *error = "unknown verbosity in '" + item + "'; one of ZERO, LOW, MEDIUM, HIGH, CHATTERBOX";
      }
      return false;
    }
    settings.push_back(std::make_pair(level, verbosity));
  }
  if (settings.empty()) {
    if (error) {
      *error = "empty";
    }
    return false;
  }
  for (const std::pair<int, int>& setting : settings) {
    if (setting.first < 0) {
      for (unsigned int level = 0; level <= MAX_LEVEL; ++level) {
        maximal_logging_verbosity[level].store(static_cast<unsigned int>(setting.second), std::memory_order_relaxed);
      }
    } else {
      maximal_logging_verbosity[setting.first].store(static_cast<unsigned int>(setting.second),
                                                     std::memory_order_relaxed);
    }
  }
  return true;
}

namespace {
// MX_LOG_VERBOSITY, read once when the library loads: before main, before
// any Python import returns. A malformed value is reported and ignored.
struct VerbosityFromEnvironment {
  VerbosityFromEnvironment() {
    const char* spec = getenv(VERBOSITY_ENVIRONMENT_VARIABLE);
    if (!spec || !*spec) {
      return;
    }
    std::string error;
    if (!apply_verbosity_spec(spec, &error)) {
      MX_LOG(WARNING, LOWVERBOSITY,
             CTX("logging") TEXT(std::string(VERBOSITY_ENVIRONMENT_VARIABLE) + "='" + spec + "' ignored: " + error));
    }
  }
} verbosity_from_environment;
}  // namespace

static void _shutdown_logging_streams() {
  mx::MutexLock locked(stream_mutex_);
  replace_stream(nullptr);
}

// A fork waits for the entry being written, and the child starts with the
// lock free: a thread writing at the moment of a fork would leave the
// child's lock held by a thread the child does not have, and the child's
// first log line with the stream on, or set_logging_fd re-arming it, would
// wait forever.
static void lock_for_fork() MX_NO_THREAD_SAFETY_ANALYSIS { stream_mutex_.lock(); }
static void unlock_after_fork() MX_NO_THREAD_SAFETY_ANALYSIS { stream_mutex_.unlock(); }
MX_TRIGGER_STATIC_INITIALIZATION(pthread_atfork(lock_for_fork, unlock_after_fork, unlock_after_fork), true);

void set_logging_fd(unsigned int logging_fd, bool close_on_delete, bool log_the_fact) {
  AssertMsg(module_is_initialized, "You can't call set_logging_fd before main()");
  {
    mx::MutexLock locked(stream_mutex_);
    replace_stream(new mx::protobuf::FileMessageOutputStream(logging_fd, close_on_delete));
  }
  if (log_the_fact) {
    MX_LOG(DEBUG, LOWVERBOSITY, CTX("logging") TEXT("set logging FD to " + repr(logging_fd)));
  }
}

void set_logging_file(const std::string& file) {
  using namespace google::protobuf::io;
  AssertMsg(module_is_initialized, "You can't call set_logging_file before main()");

  // create new CodedOutputStream based on file
  int fd = open(file.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0600);
  if (fd < 0) {
    MX_LOG(ERROR, LOWVERBOSITY, TEXT("Failed to open file '" + file + "' for writing logs.") CTX("logging"));

  } else {
    try {
      set_logging_fd(fd, /*close_on_delete*/ true, /*log_the_fact*/ false);
    } catch (...) {
      close(fd);
      throw;
    }
    MX_LOG(DEBUG, LOWVERBOSITY, CTX("logging") TEXT("set logging file to '" + file + "'"));
  }
}

std::uint64_t create_log_id() {
  // One generator per thread: logging happens from every thread there is,
  // and a shared generator would be a data race (ThreadSanitizer found it).
  static thread_local mx::Random64 generator;
  return generator();
}

void die(const std::string& text) {
  MX_LOG_ALWAYS(ERROR, LOWVERBOSITY, TEXT(text));
  exit(1);
}

void set_process_context_program_name(const std::string& s) { impl::publish_process_context(hostname + "." + s); }

MX_TRIGGER_STATIC_INITIALIZATION(atexit(_shutdown_logging_streams), true);
// this should stay at the EOF
MX_TRIGGER_STATIC_INITIALIZATION(module_is_initialized = true, true);
};  // namespace logging
};  // namespace mx
