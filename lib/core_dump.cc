// See core_dump.h.
#include "lib/core_dump.h"

#include <sys/resource.h>

#include <cerrno>
#include <cstdio>
#include <cstring>

namespace mx {

// Unlimited first, which only a privileged process gets under a finite
// hard limit; otherwise the soft limit up to the hard one, which any
// process may do. Asked for unlimited outright, an unprivileged process
// under any finite hard limit was refused, kept its soft limit, often 0,
// and warned on every run.
void enable_core_dump() {
  struct rlimit limit;
  if (getrlimit(RLIMIT_CORE, &limit) != 0) {
    fprintf(stderr, "getrlimit: %s\nWarning: core dumps may be truncated or non-existent\n", strerror(errno));
    return;
  }
  if (limit.rlim_max != RLIM_INFINITY) {
    const struct rlimit unlimited = {RLIM_INFINITY, RLIM_INFINITY};
    if (setrlimit(RLIMIT_CORE, &unlimited) == 0) {
      return;
    }
  }
  if (limit.rlim_cur == limit.rlim_max) {
    return;  // as high as it may go: nothing to do, nothing to say
  }
  const struct rlimit raised = {limit.rlim_max, limit.rlim_max};
  if (setrlimit(RLIMIT_CORE, &raised) != 0) {
    fprintf(stderr, "setrlimit: %s\nWarning: core dumps may be truncated or non-existent\n", strerror(errno));
  }
}

}  // namespace mx
