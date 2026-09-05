// enable_core_dump(): called first thing in mxcontrol's main.
#ifndef MX_LIB_CORE_DUMP_H_
#define MX_LIB_CORE_DUMP_H_

namespace mx {

// Raises the soft and hard RLIMIT_CORE of the calling process to unlimited so
// that a crash leaves a complete core file. Prints a warning to stderr and
// continues if the limit cannot be raised.
void enable_core_dump();

} // namespace mx

#endif // MX_LIB_CORE_DUMP_H_
