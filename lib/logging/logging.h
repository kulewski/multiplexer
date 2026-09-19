// Structured logging: MX_LOG(level, verbosity, tokens...). Each entry is a
// LogEntry protocol buffer (Logging.proto), written as text to stderr and,
// when set_logging_fd/file was called, as a binary stream that mxcontrol
// streamlogs can forward. The Python library logs through the same
// functions via the _native binding, so both languages share one stream
// and one filter.
//
// Levels (DEBUG .. CRITICAL) and verbosities (ZERO .. CHATTERBOX) are
// separate axes: set_maximal_logging_verbosity(level, verbosity) says how
// chatty each level may be. MX_LOG checks that first and builds nothing when
// the entry would be dropped, which is what keeps per-message logging on
// the wire path affordable: a disabled entry is one comparison.
//
// The token syntax (TEXT(...) FLOW(...) DATA(...) CTX(...) ...) is a small
// preprocessor language; lib/preproc/kwargs.h walks the tokens and
// log_tokens.h turns each into a call on the Entry being built. impl.h
// holds the runtime.
#ifndef MX_LIB_LOGGING_LOGGING_H_
#define MX_LIB_LOGGING_LOGGING_H_

#include <cstdint>
#include <string>

#include "lib/preproc/common.h"

#/*
#  * MX_LOG(level, verbosity, tokens)
#  *	Emit a log entry, running as little code as possible when the entry
#  *	would not be emitted: the tokens are evaluated only after the
#  *	level's verbosity allowed it.
#  *
#  *	`tokens' is a non empty list of:
#  *
#  *	    DEFAULT				no-op
#  *
#  *	    FLOW(flow)				add workflow information
#  *
#  *	    TEXT(text)				add human readable textual
#  *                                            information
#  *
#  *	    DATA(type_id, message)		attach a protocol buffer message
#  *						as the entry's data, tagged type_id
#  *
#  *	    CTX(context)			add context to process_context
#  *
#  *	    CONTEXT(context)			override process_context with context
#  *
#  *	    SKIPFILEIF(b)			don't emit log to logging stream if `b'
#  *
#  *
#  *	Examples (see lib/repr.h for repr()):
#  *
#  *	    MX_LOG(DEBUG, VERBOSE, CONTEXT("mx.multiplexer"));
#  *
#  *	    MX_LOG(DEBUG, VERBOSE, CTX("multiplexer") FLOW(mxmsg.workflow())
#  *		    TEXT("received a message with id " +
#  *                        mx::repr(mxmsg.id))
#  *		);
#  *
#  *	    MX_LOG(DEBUG, VERBOSE, CONTEXT("mx.multiplexer")
#  *                FLOW(mxmsg.workflow())
#  *		    DATA(MALFORMED_MESSAGE_SO_SHUTDOWN, characteristics));
#  */
#define MX_LOG(level, verbosity, tokens...)                                                                            \
  do {                                                                                                                 \
    if (::mx::logging::impl::should_log(level, verbosity)) {                                                           \
      MX_LOG_ALWAYS(level, verbosity, tokens);                                                                         \
    }                                                                                                                  \
  } while (0)

#/*
#  * MX_LOG_ALWAYS(level, verbosity, tokens)
#  *	MX_LOG without the verbosity check: for an entry that must reach
#  *	the log whatever the setting, the report of a fatal error.
#  */
#define MX_LOG_ALWAYS(level, verbosity, tokens...)                                                                     \
  do {                                                                                                                 \
    ::mx::logging::Entry __mx_log_entry(level, verbosity, __FILE__, __LINE__);                                         \
    __MX_LOG_PROCESS_TOKENS(__mx_log_entry, tokens)                                                                    \
    __mx_log_entry.emit();                                                                                             \
  } while (0)

namespace mx {
namespace logging {

extern bool module_is_initialized;

namespace consts {
/*
 * logging levels
 */
static const unsigned int DEBUG = 1;
static const unsigned int INFO = 2;
static const unsigned int OK = 3;
static const unsigned int WARNING = 4;
static const unsigned int ERROR = 5;
static const unsigned int CRITICAL = 6;
const static unsigned int MAX_LEVEL = CRITICAL;

/*
 * verbosities: how chatty a level may be, set per level with
 * set_maximal_logging_verbosity(LEVEL, verbosity); ZEROVERBOSITY silences
 * the level entirely.
 */
static const unsigned int ZEROVERBOSITY = 0;
static const unsigned int LOWVERBOSITY = 1;    // messages that appear very rarely
static const unsigned int MEDIUMVERBOSITY = 2; // messages that appear sometimes
static const unsigned int HIGHVERBOSITY = 3;   // messages that are usually quite numerous
static const unsigned int CHATTERBOX = 4;      // messages that can flood you
const static unsigned int MAX_VERBOSITY = CHATTERBOX;
#define MX_LOGGING_DEFAULT_VERBOSITY() (::mx::logging::consts::HIGHVERBOSITY)

/*
 * logging_get_level_name(level), logging_get_verbosity_name(verbosity)
 *	    The name of a level or verbosity, "UNKNOWN" for none.
 */
static inline const char *logging_get_level_name(const unsigned int level) MX_ATTRIBUTE_ALWAYS_INLINE;
static inline const char *logging_get_verbosity_name(const unsigned int verbosity) MX_ATTRIBUTE_ALWAYS_INLINE;

}; // namespace consts

/*
 * process_context()
 *	    Get context of the whole process as required for
 *	    MX_LOG(., ., context).
 */
static inline const std::string &process_context() MX_ATTRIBUTE_ALWAYS_INLINE;

/*
 * set_maximal_logging_verbosity(for_level, minimal_verbosity)
 *	Define what is the minimal verbosity of LogEntries with level
 *	for_level, which are emitted.
 */
void set_maximal_logging_verbosity(const unsigned int for_level, const unsigned int minimal_verbosity);

/*
 * apply_verbosity_spec(spec, error)
 *	Sets the verbosity from a text such as "DEBUG:CHATTERBOX,INFO:LOW"
 *	(one LEVEL:VERBOSITY pair per level named) or "MEDIUM" (every
 *	level); names as in the constants, case-insensitive, with or
 *	without the VERBOSITY suffix. Nothing changes on a malformed spec,
 *	which `error` describes; true when it applied. The environment
 *	variable MX_LOG_VERBOSITY is applied through this when the library
 *	initializes, so a deployment sets a process's logging without a
 *	rebuild or a call.
 */
bool apply_verbosity_spec(const std::string &spec, std::string *error = NULL);
static const char *const VERBOSITY_ENVIRONMENT_VARIABLE = "MX_LOG_VERBOSITY";

/*
 * set_logging_fd
 *	Set to which FD logging stream is sent.
 */
void set_logging_fd(unsigned int logging_fd, bool close_on_delete = false, bool log_the_fact = true);

void set_logging_file(const std::string &file);

std::uint64_t create_log_id();

static inline const std::string &process_context();
static inline void set_process_context(const std::string &s);
void set_process_context_program_name(const std::string &s);

void die(const std::string &text);

}; // namespace logging
}; // namespace mx

/*
 * export logging constants names
 */
using namespace mx::logging::consts;

#include "lib/logging/impl.h"

#endif // MX_LIB_LOGGING_LOGGING_H_
