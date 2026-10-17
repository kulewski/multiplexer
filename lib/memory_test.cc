// mx::keep_large_buffers: what the operator set in the environment wins,
// and once it has set glibc's thresholds a megabyte comes from the heap,
// where glibc by default maps a block over 128 KiB of its own. Under a
// sanitizer's allocator or another libc nothing is set, and the tests skip.
#include "lib/memory.h"

#include <gtest/gtest.h>
#include <malloc.h>
#include <stdlib.h>

#include <cstdlib>

namespace {

// The blocks glibc has mapped of their own rather than taken from its heap.
long mapped_blocks() {
#if defined(__GLIBC__) && __GLIBC_PREREQ(2, 33)
  return static_cast<long>(mallinfo2().hblks);
#else
  return static_cast<long>(mallinfo().hblks);
#endif
}

TEST(Memory, WhatTheOperatorSetWins) {
  setenv("GLIBC_TUNABLES", "glibc.malloc.trim_threshold=1048576", 1);
  const mx::KeptBuffers tunables = mx::keep_large_buffers();
  unsetenv("GLIBC_TUNABLES");
  if (tunables == mx::KeptBuffers::OTHER) {
    GTEST_SKIP() << "an allocator this does not tune";
  }
  EXPECT_EQ(mx::KeptBuffers::OPERATOR, tunables);
  setenv("MALLOC_TOP_PAD_", "1048576", 1);
  EXPECT_EQ(mx::KeptBuffers::OPERATOR, mx::keep_large_buffers());
  unsetenv("MALLOC_TOP_PAD_");
}

TEST(Memory, AMegabyteComesFromTheHeapOnceKept) {
  const mx::KeptBuffers kept = mx::keep_large_buffers();
  if (kept == mx::KeptBuffers::OTHER) {
    GTEST_SKIP() << "an allocator this does not tune";
  }
  ASSERT_EQ(mx::KeptBuffers::KEPT, kept);
  const long before = mapped_blocks();
  void* block = std::malloc(1 << 20);
  ASSERT_NE(nullptr, block);
  EXPECT_EQ(before, mapped_blocks()) << "the megabyte was mapped of its own";
  std::free(block);
}

}  // namespace
