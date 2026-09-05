// See memory.h.
#include "lib/memory.h"

#include <malloc.h>

namespace mx {

size_t heap_in_use_bytes() {
#if defined(__GLIBC__) && __GLIBC_PREREQ(2, 33)
  struct mallinfo2 info = mallinfo2();
#else
  // glibc before 2.33 has only mallinfo(), whose fields are int: the figure
  // wraps above 2 GB of heap. Good enough for the memory events and the
  // soak tests, which look at growth, on a libc that old.
  struct mallinfo info = mallinfo();
#endif
  return static_cast<size_t>(info.uordblks) + static_cast<size_t>(info.hblkhd); // in-use chunks, plus mmapped
}

} // namespace mx
