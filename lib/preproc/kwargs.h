// Keyword-argument tokens for macros: MX_PP_KWARGS_PROCESS(ns, data,
// TEXT(a) FLOW(b) ...) expands to ns##TEXT(a, data) ns##FLOW(b, data) ...
// The trick is that each token macro expands to the start of a call whose
// closing parenthesis comes from the next token, with SENTINEL__ closing the
// last one. Used by the logging macros; see lib/logging/log_tokens.h for
// the ns## side.
#ifndef MX_LIB_PREPROC_KWARGS_H_
#define MX_LIB_PREPROC_KWARGS_H_

#define MX_PP_KWARGS_PROCESS(namespace, data, tokens)    __MX_PP_KWARGS_TOKEN_ ## tokens SENTINEL__, namespace, data)

#// SENTINEL__ -- stop iteration
#define __MX_PP_KWARGS_TOKEN_SENTINEL__				__MX_PP_KWARGS_TOKEN_SENTINEL_I__ ( +
#define __MX_PP_KWARGS_TOKEN_SENTINEL_I__(x, namespace, data)

#// DEFAULT (no-op) token
#define __MX_PP_KWARGS_TOKEN_DEFAULT					__MX_PP_KWARGS_TOKEN_DEFAULT_I (
#define __MX_PP_KWARGS_TOKEN_DEFAULT_I(tokens, namespace, data)	__MX_PP_KWARGS_TOKEN_ ## tokens , namespace, data)

#// LEVEL(level)
#define __MX_PP_KWARGS_TOKEN_LEVEL(level)				__MX_PP_KWARGS_TOKEN_LEVEL_I ( level ,
#define __MX_PP_KWARGS_TOKEN_LEVEL_I(level, tokens, namespace, data)	namespace ## LEVEL(level, data) \
									__MX_PP_KWARGS_TOKEN_ ## tokens, namespace, data)
#// VERBOSITY(verbosity)
#define __MX_PP_KWARGS_TOKEN_VERBOSITY(verbosity)				__MX_PP_KWARGS_TOKEN_VERBOSITY_I ( verbosity ,
#define __MX_PP_KWARGS_TOKEN_VERBOSITY_I(verbosity, tokens, namespace, data)	namespace ## VERBOSITY(verbosity, data) \
									__MX_PP_KWARGS_TOKEN_ ## tokens, namespace, data)
#// FLOW(flow)
#define __MX_PP_KWARGS_TOKEN_FLOW(flow)				__MX_PP_KWARGS_TOKEN_FLOW_I ( flow ,
#define __MX_PP_KWARGS_TOKEN_FLOW_I(flow, tokens, namespace, data)	namespace ## FLOW(flow, data) \
									__MX_PP_KWARGS_TOKEN_ ## tokens, namespace, data)
#// TEXT(text)
#define __MX_PP_KWARGS_TOKEN_TEXT(text)				__MX_PP_KWARGS_TOKEN_TEXT_I ( text ,
#define __MX_PP_KWARGS_TOKEN_TEXT_I(text, tokens, namespace, data)	namespace ## TEXT(text, data) \
									__MX_PP_KWARGS_TOKEN_ ## tokens, namespace, data)
// CTX(context)
#define __MX_PP_KWARGS_TOKEN_CTX(text)				__MX_PP_KWARGS_TOKEN_CTX_I ( text ,
#define __MX_PP_KWARGS_TOKEN_CTX_I(text, tokens, namespace, data)	namespace ## CTX(text, data) \
									__MX_PP_KWARGS_TOKEN_ ## tokens, namespace, data)
// CONTEXT(context)
#define __MX_PP_KWARGS_TOKEN_CONTEXT(text)				    __MX_PP_KWARGS_TOKEN_CONTEXT_I ( text ,
#define __MX_PP_KWARGS_TOKEN_CONTEXT_I(text, tokens, namespace, data)    namespace ## CONTEXT(text, data) \
									    __MX_PP_KWARGS_TOKEN_ ## tokens, namespace, data)

#// DATA(type_id, class, setters)
#define __MX_PP_KWARGS_TOKEN_DATA(type_id, type, setters)		__MX_PP_KWARGS_TOKEN_DATA_I ( type_id, type, setters ,
#define __MX_PP_KWARGS_TOKEN_DATA_I(type_id, type, setters, tokens, namespace, data)                                    \
                                                                        namespace ## DATA(type_id, type, setters, data) \
									__MX_PP_KWARGS_TOKEN_ ## tokens, namespace, data)

#// MUSTLOG token
#define __MX_PP_KWARGS_TOKEN_MUSTLOG					__MX_PP_KWARGS_TOKEN_MUSTLOG_I (
#define __MX_PP_KWARGS_TOKEN_MUSTLOG_I(tokens, namespace, data)	namespace ## MUSTLOG(data) \
									__MX_PP_KWARGS_TOKEN_ ## tokens , namespace, data)

#// SKIPFILEIF(b)
#define __MX_PP_KWARGS_TOKEN_SKIPFILEIF(b)				    __MX_PP_KWARGS_TOKEN_SKIPFILEIF_I ( b ,
#define __MX_PP_KWARGS_TOKEN_SKIPFILEIF_I(b, tokens, namespace, data)    namespace ## SKIPFILEIF(b, data) \
									    __MX_PP_KWARGS_TOKEN_ ## tokens, namespace, data)

#endif // MX_LIB_PREPROC_KWARGS_H_
