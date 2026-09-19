// The meaning of each MX_LOG token (TEXT, FLOW, DATA, CTX, CONTEXT,
// SKIPFILEIF, DEFAULT): a member call on the mx::logging::Entry being
// built, in token order, after the should_log check has passed. Adding a
// token means adding it to lib/preproc/kwargs.h and here.
#ifndef MX_LIB_LOGGING_LOG_TOKENS_H_
#define MX_LIB_LOGGING_LOG_TOKENS_H_

#include "lib/preproc/kwargs.h"

#define __MX_LOG_PROCESS_TOKENS(entry, tokens) MX_PP_KWARGS_PROCESS(__MX_LOG_KW_, entry, tokens)

#define __MX_LOG_KW_FLOW(arg, entry) entry.flow(arg);
#define __MX_LOG_KW_TEXT(arg, entry) entry.text(arg);
#define __MX_LOG_KW_CTX(arg, entry) entry.ctx(arg);
#define __MX_LOG_KW_CONTEXT(arg, entry) entry.context(arg);
#define __MX_LOG_KW_DATA(type_id, arg, entry) entry.data(type_id, arg);
#define __MX_LOG_KW_SKIPFILEIF(arg, entry) entry.skip_file_if(arg);

#endif // MX_LIB_LOGGING_LOG_TOKENS_H_
