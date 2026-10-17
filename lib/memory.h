// The process's C heap: heap_in_use_bytes(), the bytes currently allocated
// from it, and keep_large_buffers(), glibc's malloc told to keep the memory
// of large buffers for the next ones, which the multiplexer asks at its
// start.
//
// heap_in_use_bytes() is exact, from glibc's mallinfo2 (mallinfo on glibc
// before 2.33, where it wraps above 2 GB). The measure the soak tests
// compare over time: a process that leaks per message grows here, while
// caches and buffers plateau. Cheap, but not for per-message paths.
//
// keep_large_buffers(): by default glibc maps a block over 128 KiB of its
// own and unmaps it when it is freed, then raises that threshold to the
// largest block freed, and gives memory at the heap's top back once twice
// the threshold is free there. A frame of a batch is about a megabyte, and
// the multiplexer's body of one and its parse's copy of the batch, freed
// together, kept the heap at that limit: a multiplexer just started faulted
// in every page of every such frame, measured on 2026-10-15 572,000 page
// faults and twice the CPU for its first 2,000 frames of a megabyte, and up
// to a third fewer records a second, until a larger frame raised the
// thresholds. Set, a block of up to 32 MiB comes from the heap and up to
// 64 MiB of free memory stays at its top, as glibc's own adjustment would
// end at after its first 32 MiB frame; the memory the queues held at their
// fullest stays with the process. Both are set or neither, since setting
// either turns glibc's adjustment off. What the operator set wins: nothing
// is set when GLIBC_TUNABLES names a glibc.malloc tunable or a MALLOC_*_
// variable is set. A library cannot ask this for the program that uses it.
#ifndef MX_LIB_MEMORY_H_
#define MX_LIB_MEMORY_H_

#include <cstddef>

namespace mx {

size_t heap_in_use_bytes();

// What keep_large_buffers() did.
enum class KeptBuffers {
  KEPT,      // glibc keeps them now
  OPERATOR,  // the environment sets glibc's malloc, which wins
  OTHER,     // another libc, or a sanitizer's allocator, which this does not tune
  REFUSED,   // glibc refused the threshold, which an older one may
};

KeptBuffers keep_large_buffers();

}  // namespace mx

#endif  // MX_LIB_MEMORY_H_
