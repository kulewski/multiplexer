// Structured logging: MX_LOG(level, verbosity, tokens...) and MX_ENTER /
// MX_LEAVE. Each entry is a LogEntry protocol buffer (Logging.proto),
// written as text to stderr and, when set_logging_fd/file was called, as a
// binary stream that mxcontrol streamlogs can forward. The Python library
// logs through the same functions via the _native binding, so both languages
// share one stream and one filter.
//
// Levels (DEBUG .. CRITICAL) and verbosities (ZERO .. CHATTERBOX) are
// separate axes: set_maximal_logging_verbosity(level, verbosity) says how
// chatty each level may be. MX_LOG checks that first and builds nothing when
// the entry would be dropped, which is what keeps HIGHVERBOSITY logging on
// the per-message path affordable.
//
// The token syntax (TEXT(...) FLOW(...) DATA(...) CTX(...) ...) is a small
// preprocessor language; lib/preproc/kwargs.h walks the tokens and
// log_tokens.h says what each does at each stage. impl.h holds the runtime.
#ifndef MX_LIB_LOGGING_LOGGING_H_
#define MX_LIB_LOGGING_LOGGING_H_

#include <boost/cstdint.hpp>
#include <boost/preprocessor/selection/max.hpp>
#include <boost/preprocessor/stringize.hpp>
#include <string>

#include "lib/preproc/create_message.h"

#/*
#  * MX_LOG(level, verbosity, tokens)
#  *	Emit log message with proper parameters runnin as little code as
#  *    possible it the message finally wounldn't be emitted.
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
#  *	    DATA(type_id, type, setters)	add data_type(type_id) information and generate inner data msg using setters
#  *						MX_CREATE_MESSAGE(type, x, setters) must be valid
#  *
#  *	    CTX(context)			add context to process_context
#  *
#  *	    CONTEXT(context)			override process_context with context
#  *
#  *	    SKIPFILEIF(b)			don't emit log to logging stream if `b'
#  *
#  *
#  *	Examples (see lib/repr.h for information how to shorten lexical_cast):
#  *
#  *	    MX_LOG(DEBUG, VERBOSE, CONTEXT("mx.multiplexer"));
#  *
#  *	    MX_LOG(DEBUG, VERBOSE, CTX("multiplexer") FLOW(mxmsg.workflow())
#  *		    TEXT("received a message with id " +
#  *                        boost::lexical_cast<std::string>(mxmsg.id))
#  *		);
#  *
#  *	    MX_LOG(DEBUG, VERBOSE, CONTEXT("mx.multiplexer")
#  *                FLOW(mxmsg.workflow())
#  *		    DATA(MALFORMED_MESSAGE_SO_SHUTDOWN, PeerCharacteristics,
#  *                    (set_peer_id(conn->peer_id()))
#  *                        (set_peer_type(conn->peer_type())))
#  *		);
#  *
#  */
#define MX_LOG(tokens...)                                                                                              \
  __MX_LOG(MX_UNIQUE_NAME(_log_msg_), MX_UNIQUE_NAME(_data_msg_), MX_UNIQUE_NAME(_context_), tokens)

#/*
#  * helper for extracting maximal LEVEL or VERBOSITY
#  */
#define __MX_LOGGING_MAX_OF_2ND_OR_PAIR_OP(s, state, t) BOOST_PP_MAX(state, BOOST_PP_TUPLE_ELEM(2, 1, t))
#define __MX_LOGGING_MAX_LEVEL() BOOST_PP_SEQ_FOLD_LEFT(__MX_LOGGING_MAX_OF_2ND_OR_PAIR_OP, 0, MX_LOGGING_LEVELS_SEQ)
#define __MX_LOGGING_MAX_VERBOSITY()                                                                                   \
  BOOST_PP_SEQ_FOLD_LEFT(__MX_LOGGING_MAX_OF_2ND_OR_PAIR_OP, 0, MX_LOGGING_VERBOSITIES_SEQ)

#/*
#  * MX_ENTER(tokens...)
#  *    Logging macro used when entering a function.
#  *
#  *	`tokens' is a non empty list of:
#  *
#  *	    DEFAULT				no-op
#  *
#  *        LEVEL(level)                        set logging level of the
#  *                                            underlying MX_LOG call
#  *
#  *        VERBOSITY(verbosity)                set logging verbosity of the
#  *                                            underlying MX_LOG call
#  *
#  *	    CTX(context)			add context to process_context
#  *
#  *	    CONTEXT(context)			override process_context with
#  *                                            `context`
#  *
#  *	    FLOW(flow)				add workflow information
#  *
#  */
#define MX_ENTER(tokens...) __MX_ENTER(tokens)

#define MX_RETURN(value) __MX_RETURN(return, value)
#define MX_LEAVE() __MX_RETURN(__MX_LOG_CALL_return_void, '<void>')

#define __MX_LOG_CALL_return_void(void) return;

namespace mx {
namespace logging {

extern bool module_is_initialized;

namespace consts {
/*
 * logging levels
 */
#define MX_LOGGING_LEVELS_SEQ ((DEBUG, 1))((INFO, 2))((OK, 3))((WARNING, 4))((ERROR, 5))((CRITICAL, 6)) /**/

const static unsigned int MAX_LEVEL = __MX_LOGGING_MAX_LEVEL();

/*
 * verbosities
 */
#define MX_LOGGING_VERBOSITIES_SEQ                                                                                     \
  /* allows to completely disable logging of specific LEVEL */                                                         \
  /* with set_maximal_logging_verbosity(LEVEL, ZEROVERBOSITY) */                                                       \
  ((ZEROVERBOSITY, 0))                                                                                                 \
                                                                                                                       \
      /* messages that appear very rarely */                                                                           \
      ((LOWVERBOSITY, 1))                                                                                              \
                                                                                                                       \
      /* messages that appear sometimes */                                                                             \
      ((MEDIUMVERBOSITY, 2))                                                                                           \
                                                                                                                       \
      /* messages that are usually quite numerous */                                                                   \
      ((HIGHVERBOSITY, 3))                                                                                             \
                                                                                                                       \
      /* messages that can flood you */                                                                                \
      ((CHATTERBOX, 4)) /**/
#define MX_LOGGING_DEFAULT_VERBOSITY() (::mx::logging::consts::HIGHVERBOSITY)

const static unsigned int MAX_VERBOSITY = __MX_LOGGING_MAX_VERBOSITY();

/*
 * logging_level_name<L>::name()
 *	    Returns the level name known at compile time.
 *	    Checks that the level L really is defined.
 */
template <int> struct logging_level_name;

/*
 * logging_get_level_name(level)
 *	    Returns the level name not known at compile time.
 */
static inline const char *logging_get_level_name(const unsigned int level) MX_ATTRIBUTE_ALWAYS_INLINE;

/*
 * logging_verbosity_name<L>::name()
 *	    Returns the verbosity name known at compile time.
 *	    Checks that the verbosity L really is defined.
 */
template <int> struct logging_verbosity_name;

/*
 * logging_get_verbosity_name(verbosity)
 *	    Returns the verbosity name not known at compile time.
 */
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
 * set_logging_fd
 *	Set to which FD logging stream is sent.
 */
void set_logging_fd(unsigned int logging_fd, bool close_on_delete = false, bool log_the_fact = true);

void set_logging_file(const std::string &file);

boost::uint64_t create_log_id();

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
