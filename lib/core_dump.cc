// See core_dump.h.
#include "lib/core_dump.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <sys/resource.h>

namespace mx {

void enable_core_dump() {
  struct rlimit core_limit;
  core_limit.rlim_cur = RLIM_INFINITY;
  core_limit.rlim_max = RLIM_INFINITY;

  if (setrlimit(RLIMIT_CORE, &core_limit) < 0) {
    fprintf(stderr,
            "setrlimit: %s\nWarning: core dumps may be truncated or "
            "non-existent\n",
            strerror(errno));
  }
}

} // namespace mx
