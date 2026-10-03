// enable_core_dump(): called first thing in mxcontrol's main.
#ifndef MX_LIB_CORE_DUMP_H_
#define MX_LIB_CORE_DUMP_H_

namespace mx {

// Raises RLIMIT_CORE of the calling process as far as it may go, so that a
// crash leaves as complete a core file as the limits allow: both limits to
// unlimited where the process is privileged, the soft limit to the hard
// one otherwise. Prints a warning to stderr, and continues, only when the
// soft limit could not reach the hard one.
void enable_core_dump();

}  // namespace mx

#endif  // MX_LIB_CORE_DUMP_H_
