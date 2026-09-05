// The meaning of each MX_LOG / MX_ENTER token (TEXT, FLOW, DATA, CTX,
// CONTEXT, LEVEL, VERBOSITY, MUSTLOG, SKIPFILEIF) at each stage of the
// macro expansion: before the should_log check, after it, and just before
// emitting. Adding a token means adding it to lib/preproc/kwargs.h and to
// every stage here.
#ifndef MX_LIB_LOGGING_LOG_TOKENS_H_
#define MX_LIB_LOGGING_LOG_TOKENS_H_

// vim:tw=0

#include "lib/preproc/kwargs.h"

#/*
#  * helper macro definitions for __MX_LOG and __MX_ENTER
#  */

#/*
#  * __MX_LOG_PROCESS_TOKENS_*
#  *	Called in __MX_LOG at various stages.
#  */
#define __MX_LOG_PROCESS_TOKENS_BEFORE_CHECK(data, tokens) MX_PP_KWARGS_PROCESS(__MX_LOG_KW_BEFORE_CHECK_, data, tokens)

#/*
#  *	each token AA must be implement in lib/preproc/kwargs.h
#  *	each token must provide support for all places it can occur in
#  */

#// __MX_LOG_KW_BEFORE_CHECK_ namespace
#define __MX_LOG_KW_BEFORE_CHECK_FLOW(flow, data)
#define __MX_LOG_KW_BEFORE_CHECK_TEXT(text, data)
#define __MX_LOG_KW_BEFORE_CHECK_DATA(type_id, type, setters, data)
#define __MX_LOG_KW_BEFORE_CHECK_CTX(context, data)
#define __MX_LOG_KW_BEFORE_CHECK_CONTEXT(context, data)
#define __MX_LOG_KW_BEFORE_CHECK_MUSTLOG(data) __mx_log_mustlog = true;
#define __MX_LOG_KW_BEFORE_CHECK_SKIPFILEIF(b, data)

#/*
#  * __MX_EMIT_PROCESS_TOKENS_*
#  *	Called in __MX_EMIT at various stages.
#  *
#  *    They must implement all tokens provided by callers, e.g. by __MX_LOG
#  *    and __MX_ENTER.
#  */
#define __MX_EMIT_PROCESS_TOKENS_AFTER_CHECK(data, tokens) MX_PP_KWARGS_PROCESS(__MX_EMIT_KW_AFTER_CHECK_, data, tokens)
#define __MX_EMIT_PROCESS_TOKENS_BEFORE_EMIT(data, tokens) MX_PP_KWARGS_PROCESS(__MX_EMIT_KW_BEFORE_EMIT_, data, tokens)

#// __MX_EMIT_KW_AFTER_CHECK_ namespace
#define __MX_EMIT_KW_AFTER_CHECK_FLOW(flow, data)
#define __MX_EMIT_KW_AFTER_CHECK_TEXT(text, data)
#define __MX_EMIT_KW_AFTER_CHECK_DATA(type_id, type, setters, data)
#define __MX_EMIT_KW_AFTER_CHECK_CTX(context, data) BOOST_PP_ARRAY_ELEM(2, data).append(".").append(context);
#define __MX_EMIT_KW_AFTER_CHECK_CONTEXT(context, data) BOOST_PP_ARRAY_ELEM(2, data) = (context);
#define __MX_EMIT_KW_AFTER_CHECK_MUSTLOG(data)
#define __MX_EMIT_KW_AFTER_CHECK_SKIPFILEIF(b, data)                                                                   \
  if (b) {                                                                                                             \
    __mx_log_flags |= ::mx::logging::impl::SKIP_LOGGING_TO_STREAM;                                                     \
  } else {                                                                                                             \
  }
#// added to support __MX_ENTER calls
#define __MX_EMIT_KW_AFTER_CHECK_LEVEL(level, data)
#define __MX_EMIT_KW_AFTER_CHECK_VERBOSITY(verbosity, data)

#// __MX_EMIT_KW_BEFORE_EMIT_ namespace
#define __MX_EMIT_KW_BEFORE_EMIT_FLOW(flow, data) BOOST_PP_ARRAY_ELEM(0, data).set_workflow(flow);
#define __MX_EMIT_KW_BEFORE_EMIT_TEXT(text, data) BOOST_PP_ARRAY_ELEM(0, data).set_text(text);
#define __MX_EMIT_KW_BEFORE_EMIT_DATA(type_id, type, setters, data)                                                    \
  MX_CREATE_MESSAGE(type, BOOST_PP_ARRAY_ELEM(1, data), setters);                                                      \
  BOOST_PP_ARRAY_ELEM(0, data).set_data_type(type_id);                                                                 \
  BOOST_PP_ARRAY_ELEM(0, data).set_data_class(#type);
#define __MX_EMIT_KW_BEFORE_EMIT_CTX(context, data)
#define __MX_EMIT_KW_BEFORE_EMIT_CONTEXT(context, data)
#define __MX_EMIT_KW_BEFORE_EMIT_MUSTLOG(data)
#define __MX_EMIT_KW_BEFORE_EMIT_SKIPFILEIF(b, data)
#// added to support __MX_ENTER calls
#define __MX_EMIT_KW_BEFORE_EMIT_LEVEL(level, data)
#define __MX_EMIT_KW_BEFORE_EMIT_VERBOSITY(verbosity, data)

#/*
#  * __MX_ENTER_PROCESS_TOKENS_*
#  *	Called in __MX_ENTER at various stages.
#  */
#define __MX_ENTER_PROCESS_TOKENS_BEFORE_CHECK(data, tokens)                                                           \
  MX_PP_KWARGS_PROCESS(__MX_ENTER_KW_BEFORE_CHECK_, data, tokens)

#// __MX_ENTER_KW_BEFORE_CHECK_ namespace
#define __MX_ENTER_KW_BEFORE_CHECK_LEVEL(level, data) BOOST_PP_ARRAY_ELEM(0, data) = (level);
#define __MX_ENTER_KW_BEFORE_CHECK_VERBOSITY(verbosity, data) BOOST_PP_ARRAY_ELEM(1, data) = (verbosity);
#define __MX_ENTER_KW_BEFORE_CHECK_CTX(context, data) BOOST_PP_ARRAY_ELEM(2, data).append(".").append(context)
#define __MX_ENTER_KW_BEFORE_CHECK_CONTEXT(context, data) BOOST_PP_ARRAY_ELEM(2, data) = (context);

// vim:tw=0:

#endif // MX_LIB_LOGGING_LOG_TOKENS_H_
