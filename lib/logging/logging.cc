// Logging runtime: the verbosity table, the stderr text form, the optional
// binary stream (length-prefixed LogEntry records via lib/protobuf/stream.h),
// and the process context (hostname.program[.suffix]).
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
// #include <google/protobuf/io/zero_copy_stream_impl.h>
// #include <google/protobuf/io/coded_stream.h>
// #include <google/protobuf/wire_format.h>
// #include <google/protobuf/wire_format_inl.h>
#include "lib/assertion.h"
#include "lib/initialization.h"
#include "lib/logging/logging.h"
#include "lib/protobuf/stream.h"
#include "lib/random.h"
#include "lib/repr.h"

namespace mx {
namespace logging {

bool module_is_initialized = false;

namespace impl {

std::unique_ptr<mx::protobuf::MessageOutputStream> message_output_stream_;

std::string process_context_;

// One entry per level, index 0 unused; constant-initialized so that a log
// line during another translation unit's static initialization is safe.
static_assert(MAX_LEVEL == 6, "the table below has one entry per level");
unsigned int maximal_logging_verbosity[MAX_LEVEL + 1] = {MX_LOGGING_DEFAULT_VERBOSITY(), MX_LOGGING_DEFAULT_VERBOSITY(),
                                                         MX_LOGGING_DEFAULT_VERBOSITY(), MX_LOGGING_DEFAULT_VERBOSITY(),
                                                         MX_LOGGING_DEFAULT_VERBOSITY(), MX_LOGGING_DEFAULT_VERBOSITY(),
                                                         MX_LOGGING_DEFAULT_VERBOSITY()};

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

static inline void initialize_process_name() {
  std::string proc = "/proc/" + mx::repr(getpid()) + "/cmdline";
  std::ifstream cmdline(proc.c_str(), std::ofstream::binary);
  if (!cmdline) {
    std::cerr << "logging::impl::initialize_process_context_all_defaults: " << proc << ": No such file or directory\n";
    exit(EXIT_FAILURE);
  }

  cmdline >> proc;                  // read the process name and parameters
  std::string name = proc.c_str();  // extract the process name
  if (name.empty()) {
    std::cerr << "logging::impl::initialize_process_context_all_defaults: "
                 "coulnd't get the process name from /proc\n";
  }

  process_name.clear();
  std::string::size_type spos = name.rfind('/');  // get the basename
  process_name.append(name, (spos == std::string::npos && name.size() > spos) ? 0 : spos + 1, name.size());
}
MX_TRIGGER_STATIC_INITIALIZATION(initialize_process_name(), process_name.empty());

/*
 * initialize_process_context_all_defaults)
 *	    set process_context_ to <hostname>.<processname> or die
 */
static inline void initialize_process_context_all_defaults() {
  Assert(process_context_state_ != DURING_INITIALIZATION);
  // set process_context_ temporarily
  process_context_ = "<unknown>";
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
  process_context_ = hostname + "." + process_name;
  process_context_state_ = SET_WITH_ALL_DEFAULTS;
}
MX_TRIGGER_STATIC_INITIALIZATION(initialize_process_context_all_defaults(), process_context_state_ == NOTHING);

void _emit_log(const LogEntry& log_msg) {
  if (!mx::logging::module_is_initialized) {
    std::cerr << "Warning: mx::logging::_emit_log called before "
                 "module_is_initialized. LogEntry ignored.\n";
    return;
  }
  if (!message_output_stream_) {
    // no logging stream set yet
    return;
  }
  message_output_stream_->write(log_msg);
  message_output_stream_->flush();
}

};  // namespace impl

using namespace impl;

void set_maximal_logging_verbosity(const unsigned int for_level, const unsigned int minimal_verbosity) {
  Assert(for_level <= MAX_LEVEL);
  maximal_logging_verbosity[for_level] = minimal_verbosity;
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
        maximal_logging_verbosity[level] = static_cast<unsigned int>(setting.second);
      }
    } else {
      maximal_logging_verbosity[setting.first] = static_cast<unsigned int>(setting.second);
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

static inline void _shutdown_logging_streams() {
  if (message_output_stream_) {
    message_output_stream_->flush();
    message_output_stream_.reset();
  }
}

void set_logging_fd(unsigned int logging_fd, bool close_on_delete, bool log_the_fact) {
  using namespace google::protobuf::io;
  AssertMsg(module_is_initialized, "You can't call set_logging_fd before main()");

  // create new CodedOutputStream based on logging_fd
  _shutdown_logging_streams();
  message_output_stream_.reset(new mx::protobuf::FileMessageOutputStream(logging_fd, close_on_delete));
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

void set_process_context_program_name(const std::string& s) { impl::process_context_ = hostname + "." + s; }

MX_TRIGGER_STATIC_INITIALIZATION(atexit(_shutdown_logging_streams), true);
// this should stay at the EOF
MX_TRIGGER_STATIC_INITIALIZATION(module_is_initialized = true, true);
};  // namespace logging
};  // namespace mx
