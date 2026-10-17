// See memory.h.
#include "lib/memory.h"

#include <malloc.h>

#include <cstdlib>
#include <cstring>
#include <initializer_list>

// A sanitizer's allocator stands in for glibc's and answers mallopt with 0.
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define MX_MEMORY_SANITIZED 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer) || __has_feature(memory_sanitizer)
#define MX_MEMORY_SANITIZED 1
#endif
#endif

#if defined(__GLIBC__) && !defined(MX_MEMORY_SANITIZED)
#define MX_MEMORY_TUNES_GLIBC 1
#endif

namespace mx {

#ifdef MX_MEMORY_TUNES_GLIBC
namespace {

const int KEPT_BLOCK_BYTES = 32 * 1024 * 1024;  // a block up to this comes from the heap
const int KEPT_FREE_BYTES = 64 * 1024 * 1024;   // free memory kept at the heap's top

// Whether the environment sets glibc's malloc: a glibc.malloc tunable, or
// one of the older MALLOC_*_ variables.
bool operator_tunes_malloc() {
  const char* tunables = std::getenv("GLIBC_TUNABLES");
  if (tunables != nullptr && std::strstr(tunables, "glibc.malloc.") != nullptr) {
    return true;
  }
  for (const char* name : {"MALLOC_MMAP_THRESHOLD_", "MALLOC_TRIM_THRESHOLD_", "MALLOC_TOP_PAD_", "MALLOC_MMAP_MAX_"}) {
    if (std::getenv(name) != nullptr) {
      return true;
    }
  }
  return false;
}

}  // namespace
#endif  // MX_MEMORY_TUNES_GLIBC

size_t heap_in_use_bytes() {
#if defined(__GLIBC__) && __GLIBC_PREREQ(2, 33)
  struct mallinfo2 info = mallinfo2();
#else
  // glibc before 2.33 has only mallinfo(), whose fields are int: the figure
  // wraps above 2 GB of heap. Good enough for the memory events and the
  // soak tests, which look at growth, on a libc that old.
  struct mallinfo info = mallinfo();
#endif
  return static_cast<size_t>(info.uordblks) + static_cast<size_t>(info.hblkhd);  // in-use chunks, plus mmapped
}

KeptBuffers keep_large_buffers() {
#ifdef MX_MEMORY_TUNES_GLIBC
  if (operator_tunes_malloc()) {
    return KeptBuffers::OPERATOR;
  }
  // The threshold first: a glibc that refuses it leaves both as they were,
  // where the trim threshold set alone would turn the adjustment off.
  if (mallopt(M_MMAP_THRESHOLD, KEPT_BLOCK_BYTES) != 1) {
    return KeptBuffers::REFUSED;
  }
  mallopt(M_TRIM_THRESHOLD, KEPT_FREE_BYTES);
  return KeptBuffers::KEPT;
#else
  return KeptBuffers::OTHER;
#endif
}

}  // namespace mx
