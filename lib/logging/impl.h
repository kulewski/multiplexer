// Runtime behind logging.h: the level and verbosity tables, should_log(),
// emit_log() and the process context. Included by logging.h at the end;
// nothing includes it directly.
#ifndef MX_LIB_LOGGING_IMPL_H_
#define MX_LIB_LOGGING_IMPL_H_

#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <boost/cstdint.hpp>
#include <boost/preprocessor/array/elem.hpp>
#include <boost/type_traits/is_base_of.hpp>
#include <boost/utility/enable_if.hpp>
#include <google/protobuf/text_format.h>
#include <iostream>
#include <sstream>
#include <string>

#include "lib/assertion.h"
#include "lib/logging/Logging.pb.h" /* generated */
#include "lib/logging/log_tokens.h"
#include "lib/preproc/common.h"
#include "lib/release.h"

#/*
#  * MX_SHOULD_LOG(level, verbosity, context)
#  *	Checks if the current (level, verbosity) is logged from context.
#  *	Use this and don't call mx::logging::should_log directly,
#  *	as context parameter may be not used.
#  */
#define MX_SHOULD_LOG(level, verbosity, context) (::mx::logging::impl::should_log(level, verbosity))

#/*
#  * MX_LOGGING_EMIT_LOG(level, verbosity, type, type_str, data_type_str,
#  *        context, log_msg, data_msg)
#  *	Emit log with parameters.
#  *	Use this and don't call emit_log directly. Obviously.
#  */
#define MX_LOGGING_EMIT_LOG(log_msg, data_msg, level, verbosity, log_flags)                                            \
  (::mx::logging::impl::emit_log(level, log_msg, data_msg, log_flags))

#/*
#  * MX_LOGGING_CURRENT__FILE__
#  *	Where the MX_LOG originated from.
#  */
// #if defined(__BASE_FILE__)
// # define MX_LOGGING_CURRENT__FILE__() (__FILE__ ":" __BASE_FILE__)
// #else
#define MX_LOGGING_CURRENT__FILE__() (__FILE__)
// #endif

#/*
#  * __MX_LOG
#  *	    MX_LOG (implementation)
#  */
#define __MX_LOG(log_msg, data_msg, contexttmp, level, verbosity, tokens)                                              \
  /* start MX_LOG */                                                                                                   \
  do {                                                                                                                 \
    const ::mx::logging::NoneType &data_msg = ::mx::logging::None;                                                     \
    /* Fool a compiler that `data_msg' is always used. */                                                              \
    ::mx::logging::impl::do_nothing(data_msg);                                                                         \
    bool __mx_log_mustlog = false;                                                                                     \
    __MX_LOG_PROCESS_TOKENS_BEFORE_CHECK((5, (log_msg, data_msg, contexttmp, level, verbosity)), tokens)               \
    if (__mx_log_mustlog || ::mx::logging::impl::should_log(level, verbosity)) {                                       \
      __MX_EMIT_LOG(log_msg, data_msg, contexttmp, level, verbosity, tokens);                                          \
    }                                                                                                                  \
  } while (0) // end MX_LOG

/*
 * __MX_EMIT_LOG
 *      Not-so-thin wrapper around MX_LOGGING_EMIT_LOG
 *      (logging::impl::emit_log).
 *
 *      Called, when we already known that logging should be performed. Used in
 *      __MX_LOG, __MX_ENTER and maybe other.
 */
#define __MX_EMIT_LOG(log_msg, data_msg, contexttmp, level, verbosity, tokens)                                         \
  /* __MX_EMIT_LOG */                                                                                                  \
  ::std::string contexttmp = ::mx::logging::process_context();                                                         \
  unsigned int __mx_log_flags = ::mx::logging::impl::NO_FLAGS;                                                         \
  __MX_EMIT_PROCESS_TOKENS_AFTER_CHECK((5, (log_msg, data_msg, contexttmp, level, verbosity)), tokens)                 \
  MX_CREATE_MESSAGE(::mx::logging::LogEntry, log_msg,                                                                  \
                    (set_id(::mx::logging::create_log_id()))(set_pid(getpid()))(set_level(level))(set_verbosity(       \
                        verbosity))(set_context(contexttmp))(set_timestamp(::mx::logging::impl::current_timestamp()))( \
                        set_version(::mx::release::version))(set_source_file(MX_LOGGING_CURRENT__FILE__()))(           \
                        set_source_line(__LINE__))(set_compilation_datetime(__DATE__ " " __TIME__)));                  \
  __MX_EMIT_PROCESS_TOKENS_BEFORE_EMIT((5, (log_msg, data_msg, contexttmp, level, verbosity)), tokens)                 \
  MX_LOGGING_EMIT_LOG(log_msg, data_msg, level, verbosity, __mx_log_flags);                                            \
  // end __MX_EMIT_LOG

/*
 * __MX_ENTER
 *      MX_ENTER implementation
 */
#define __MX_ENTER(tokens)                                                                                             \
  /* start MX_ENTER */                                                                                                 \
  unsigned int __mx_log_call_level = ::mx::logging::consts::DEBUG;                                                     \
  unsigned int __mx_log_call_verbosity = ::mx::logging::consts::CHATTERBOX;                                            \
  ::std::string __mx_log_call_context = ::mx::logging::process_context();                                              \
  __MX_ENTER_PROCESS_TOKENS_BEFORE_CHECK((3, (__mx_log_call_level, __mx_log_call_verbosity, __mx_log_call_context)),   \
                                         tokens)                                                                       \
  const bool __mx_log_call_should_log =                                                                                \
      MX_SHOULD_LOG(__mx_log_call_level, __mx_log_call_verbosity, __mx_log_call_context);                              \
  if (__mx_log_call_should_log) {                                                                                      \
    /* TODO(findepi) */                                                                                                \
    const ::mx::logging::NoneType &__mx_log_call_data_msg = ::mx::logging::None;                                       \
    {                                                                                                                  \
      __MX_EMIT_LOG(__mx_log_call_log_msg, __mx_log_call_data_msg, __mx_log_call_context, __mx_log_call_level,         \
                    __mx_log_call_verbosity,                                                                           \
                    TEXT(std::string("ENTER ") + __PRETTY_FUNCTION__) CONTEXT(__mx_log_call_context) tokens);          \
    }                                                                                                                  \
  }                                                                                                                    \
  // end MX_ENTER

#define __MX_RETURN(return, value)                                                                                     \
  /* start MX_RETURN */                                                                                                \
  do {                                                                                                                 \
    if (__mx_log_call_should_log) {                                                                                    \
      /* TODO(findepi) */                                                                                              \
      const ::mx::logging::NoneType &__mx_log_call_data_msg = ::mx::logging::None;                                     \
      {                                                                                                                \
        __MX_EMIT_LOG(__mx_log_call_log_msg, __mx_log_call_data_msg, __mx_log_call_context, __mx_log_call_level,       \
                      __mx_log_call_verbosity,                                                                         \
                      TEXT(std::string("LEAVE ") + __PRETTY_FUNCTION__) CONTEXT(__mx_log_call_context));               \
      }                                                                                                                \
    }                                                                                                                  \
    return (value);                                                                                                    \
  } while (0);                                                                                                         \
  // end MX_RETURN

namespace mx {
namespace logging {

struct NoneType {};
extern NoneType None;

namespace consts {

/*
 * defintions of levels
 *	    static const unsigned int DEBUG = ...;
 *
 * and specializations logging_level_name<L>
 *	    logging_level_name<DEBUG>::name() { return "DEBUG"; }
 */
#define MX_define_logging_level(r, d, tup)                                                                             \
  static const unsigned int BOOST_PP_TUPLE_ELEM(2, 0, tup) = BOOST_PP_TUPLE_ELEM(2, 1, tup);                           \
  template <> struct logging_level_name<BOOST_PP_TUPLE_ELEM(2, 0, tup)> {                                              \
    static inline const char *name() MX_ATTRIBUTE_ALWAYS_INLINE {                                                      \
      return BOOST_PP_STRINGIZE(BOOST_PP_TUPLE_ELEM(2, 0, tup));                                                         \
    }                                                                                                                  \
  };

BOOST_PP_SEQ_FOR_EACH(MX_define_logging_level, ~, MX_LOGGING_LEVELS_SEQ);

#undef MX_define_logging_level

/*
 * definition of logging_get_level_name
 */
static inline const char *logging_get_level_name(const unsigned int level) {
  switch (level) {
    /* generate the switch:
     *	    case DEBUG: return "DEBUG";
     *	    ...
     */
#define MX_get_logging_level_name(r, d, tup)                                                                           \
  case BOOST_PP_TUPLE_ELEM(2, 0, tup):                                                                                 \
    return BOOST_PP_STRINGIZE( \
                                BOOST_PP_TUPLE_ELEM(2, 0, tup));
    BOOST_PP_SEQ_FOR_EACH(MX_get_logging_level_name, ~, MX_LOGGING_LEVELS_SEQ)
#undef MX_get_logging_level_name
  default:
    return "UNKNOWN";
  }
}

/*
 * defintions of verbosities
 *	    static const unsigned int LOWVERBOSITY = ...;
 *
 * and specializations logging_verbosity_name<L>
 *	    logging_verbosity_name<LOWVERBOSITY>::name()
 *	    { return "LOWVERBOSITY"; }
 */
#define MX_define_logging_verbosity(r, d, tup)                                                                         \
  static const unsigned int BOOST_PP_TUPLE_ELEM(2, 0, tup) = BOOST_PP_TUPLE_ELEM(2, 1, tup);                           \
  template <> struct logging_verbosity_name<BOOST_PP_TUPLE_ELEM(2, 0, tup)> {                                          \
    static inline const char *name() MX_ATTRIBUTE_ALWAYS_INLINE {                                                      \
      return BOOST_PP_STRINGIZE(BOOST_PP_TUPLE_ELEM(2, 0, tup));                                                         \
    }                                                                                                                  \
  };

BOOST_PP_SEQ_FOR_EACH(MX_define_logging_verbosity, ~, MX_LOGGING_VERBOSITIES_SEQ)
#undef MX_define_logging_verbosity

/*
 * definition of logging_get_verbosity_name
 */
static inline const char *logging_get_verbosity_name(const unsigned int verbosity) {
  switch (verbosity) {
    /* generate the switch:
     *	    case LOWVERBOSITY: return "LOWVERBOSITY";
     *	    ...
     */
#define MX_get_logging_verbosity_name(r, d, tup)                                                                       \
  case BOOST_PP_TUPLE_ELEM(2, 0, tup):                                                                                 \
    return BOOST_PP_STRINGIZE( \
                                BOOST_PP_TUPLE_ELEM(2, 0, tup));
    BOOST_PP_SEQ_FOR_EACH(MX_get_logging_verbosity_name, ~, MX_LOGGING_VERBOSITIES_SEQ)
#undef MX_get_logging_verbosity_name
  default:
    return "UNKNOWN";
  }
}

}; // namespace consts

namespace impl {

// flags
static const unsigned int NO_FLAGS = 0;
static const unsigned int SKIP_LOGGING_TO_STREAM = 1;

/*
 * do_nothing
 *	Function to fool compiler that a variable is used even if it's
 *	not.
 */
template <typename T> inline void do_nothing(const T &) {}

extern std::string process_context_;
extern unsigned int maximal_logging_verbosity[];

/*
 * current_timestamp()
 *	-> current timestamp as uint64_t
 */
static inline boost::uint64_t current_timestamp() MX_ATTRIBUTE_ALWAYS_INLINE;
static inline boost::uint64_t current_timestamp() { return time(NULL); }

/*
 * should_log
 *	Invoked from MX_SHOULD_LOG and __MX_LOG.
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

/*
 * emit_log
 *  Write log_msg to cerr and logging stream.
 *	Invoked from MX_LOGGING_EMIT_LOG.
 */

// shortcut for writing an (already initialized) LogEntry on cerr
// and on binary logging stream
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

// specialization for DataType being NoneType (MX_LOG called
// without DATA kwarg)
inline void emit_log(const unsigned int level, const LogEntry &log_msg, const mx::logging::NoneType &,
                     unsigned int flags = 0) {
  return emit_log(level, log_msg, flags);
}

// specialization for DataType being a ProtoBuf Message
template <typename DataType>
inline typename boost::enable_if_c<boost::is_base_of<google::protobuf::Message, DataType>::value, void>::type
emit_log(const unsigned int level, LogEntry &log_msg, DataType &data_msg, unsigned int flags = 0) {
  data_msg.SerializeToString(log_msg.mutable_data());
  std::string text_format;
  google::protobuf::TextFormat::PrintToString(data_msg, &text_format);

  emit_log(level, log_msg, None, flags);
  std::cerr << text_format << "\n"
            << "\n";
}

}; // namespace impl

static inline const std::string &process_context() { return impl::process_context_; }

static inline void set_process_context(const std::string &s) { impl::process_context_ = s; }

}; // namespace logging
}; // namespace mx

#endif // MX_LIB_LOGGING_IMPL_H_
