// Runtime behind logging.h: the level and verbosity tables, should_log(),
// the Entry that MX_LOG builds and emits, emit_log() for entries built
// elsewhere (the Python binding), and the process context. Included by
// logging.h at the end; nothing includes it directly.
#ifndef MX_LIB_LOGGING_IMPL_H_
#define MX_LIB_LOGGING_IMPL_H_

#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <cstdint>
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>
#include <iostream>
#include <sstream>
#include <string>

#include "lib/assertion.h"
#include "lib/logging/Logging.pb.h" /* generated */
#include "lib/logging/log_tokens.h"
#include "lib/preproc/common.h"
#include "lib/release.h"

namespace mx {
namespace logging {

namespace consts {

static inline const char *logging_get_level_name(const unsigned int level) {
  switch (level) {
  case DEBUG:
    return "DEBUG";
  case INFO:
    return "INFO";
  case OK:
    return "OK";
  case WARNING:
    return "WARNING";
  case ERROR:
    return "ERROR";
  case CRITICAL:
    return "CRITICAL";
  default:
    return "UNKNOWN";
  }
}

static inline const char *logging_get_verbosity_name(const unsigned int verbosity) {
  switch (verbosity) {
  case ZEROVERBOSITY:
    return "ZEROVERBOSITY";
  case LOWVERBOSITY:
    return "LOWVERBOSITY";
  case MEDIUMVERBOSITY:
    return "MEDIUMVERBOSITY";
  case HIGHVERBOSITY:
    return "HIGHVERBOSITY";
  case CHATTERBOX:
    return "CHATTERBOX";
  default:
    return "UNKNOWN";
  }
}

}; // namespace consts

namespace impl {

// flags
static const unsigned int NO_FLAGS = 0;
static const unsigned int SKIP_LOGGING_TO_STREAM = 1;

extern std::string process_context_;
extern unsigned int maximal_logging_verbosity[];

/*
 * current_timestamp()
 *	-> current timestamp as uint64_t
 */
static inline std::uint64_t current_timestamp() MX_ATTRIBUTE_ALWAYS_INLINE;
static inline std::uint64_t current_timestamp() { return time(NULL); }

/*
 * should_log
 *	The check MX_LOG makes before building anything: one array load.
 */
inline bool should_log(unsigned int level, unsigned int verbosity) MX_ATTRIBUTE_ALWAYS_INLINE;
inline bool should_log(unsigned int level, unsigned int verbosity) {
  DbgAssert(level <= MAX_LEVEL);
  DbgAssert(verbosity <= MAX_VERBOSITY);
  return impl::maximal_logging_verbosity[level] >= verbosity;
}

/*
 * _emit_log
 *	Emit log to logging stream (no cerr).
 */
void _emit_log(const LogEntry &log_msg);

// Writes an (already initialized) LogEntry on cerr and on the binary
// logging stream; the Python binding's entry point.
static inline void emit_log(const unsigned int level, const LogEntry &log_msg, unsigned int flags = 0) {
  if (!(flags & SKIP_LOGGING_TO_STREAM))
    _emit_log(log_msg);
  std::ostringstream cerr;
  cerr << "[" << logging_get_level_name(level) << "]"
       << "  ts=" << log_msg.timestamp() << "  pid=" << log_msg.pid() << "  ctx=" << log_msg.context() << "  flw=\""
       << log_msg.workflow() << "\""
       << "  txt=\"" << log_msg.text() << "\"";
  if (log_msg.has_source_file()) {
    cerr << "  from=" << log_msg.source_file();
    if (log_msg.has_source_line())
      cerr << ":" << log_msg.source_line();
  }
  cerr << "\n";
  std::cerr << cerr.str();
}

// as the above but without level->str optimization
static inline void emit_log(const LogEntry &log_msg, unsigned int flags = 0) {
  return emit_log(log_msg.level(), log_msg, flags);
}

}; // namespace impl

// One entry under construction: what MX_LOG builds once the check passed,
// each token a call, then emit(). Built only when the entry is emitted, so
// the context copy and the protobuf cost nothing on a disabled line.
class Entry {
public:
  Entry(unsigned int level, unsigned int verbosity, const char *file, unsigned int line)
      : level_(level), context_(impl::process_context_), flags_(impl::NO_FLAGS) {
    entry_.set_id(create_log_id());
    entry_.set_pid(getpid());
    entry_.set_level(level);
    entry_.set_verbosity(verbosity);
    entry_.set_timestamp(impl::current_timestamp());
    entry_.set_version(::mx::release::version);
    entry_.set_source_file(file);
    entry_.set_source_line(line);
    entry_.set_compilation_datetime(__DATE__ " " __TIME__);
  }
  Entry &text(const std::string &text) {
    entry_.set_text(text);
    return *this;
  }
  Entry &flow(const std::string &flow) {
    entry_.set_workflow(flow);
    return *this;
  }
  Entry &ctx(const std::string &context) {
    context_.append(".").append(context);
    return *this;
  }
  Entry &context(const std::string &context) {
    context_ = context;
    return *this;
  }
  // A protocol buffer message attached as the entry's data, tagged with
  // `type_id` and its class name, and printed after the entry on stderr.
  Entry &data(unsigned int type_id, const google::protobuf::Message &message) {
    message.SerializeToString(entry_.mutable_data());
    entry_.set_data_type(type_id);
    entry_.set_data_class(message.GetDescriptor()->name());
    google::protobuf::TextFormat::PrintToString(message, &data_text_);
    has_data_ = true;
    return *this;
  }
  Entry &skip_file_if(bool skip) {
    if (skip)
      flags_ |= impl::SKIP_LOGGING_TO_STREAM;
    return *this;
  }
  void emit() {
    entry_.set_context(context_);
    impl::emit_log(level_, entry_, flags_);
    if (has_data_)
      std::cerr << data_text_ << "\n\n";
  }

private:
  unsigned int level_;
  LogEntry entry_;
  std::string context_;
  unsigned int flags_;
  std::string data_text_;
  bool has_data_ = false;
};

static inline const std::string &process_context() { return impl::process_context_; }

static inline void set_process_context(const std::string &s) { impl::process_context_ = s; }

}; // namespace logging
}; // namespace mx

#endif // MX_LIB_LOGGING_IMPL_H_
