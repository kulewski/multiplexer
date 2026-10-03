// DescriptorTable: what a forked child closes is exactly what the table
// holds, the descriptors added and not removed, however far the table grew
// past its first block; a freed slot is taken again, so that a client
// reconnecting any number of times keeps the table at its first size. In
// this process: close_all() closes them here, as a child does its copies.
#include "multiplexer/descriptor_table.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cerrno>
#include <vector>

using multiplexer::DescriptorTable;

namespace {

// A descriptor of this process's, on /dev/null.
int open_one() { return ::open("/dev/null", O_RDONLY); }

// Whether `fd` is open in this process.
bool is_open(int fd) { return ::fcntl(fd, F_GETFD) != -1 || errno != EBADF; }

}  // namespace

TEST(DescriptorTable, ClosesWhatItHoldsAndNothingElse) {
  DescriptorTable table;
  const int held = open_one(), forgotten = open_one(), never_added = open_one();
  table.add(held);
  table.add(forgotten);
  table.remove(forgotten);
  table.close_all();
  EXPECT_FALSE(is_open(held));
  EXPECT_TRUE(is_open(forgotten)) << "removed before it was closed: left alone";
  EXPECT_TRUE(is_open(never_added));
  ::close(forgotten);
  ::close(never_added);
}

TEST(DescriptorTable, GrowsKeepingEveryDescriptor) {
  DescriptorTable table;
  const std::size_t first = table.capacity();
  std::vector<int> held;
  for (std::size_t index = 0; index < 3 * first + 1; ++index) {
    held.push_back(open_one());
    table.add(held.back());
  }
  EXPECT_GE(table.capacity(), held.size());
  table.close_all();
  for (int fd : held) {
    EXPECT_FALSE(is_open(fd)) << fd << " was added before the table grew past it";
  }
}

TEST(DescriptorTable, AFreedSlotIsTakenAgain) {
  DescriptorTable table;
  const std::size_t first = table.capacity();
  std::vector<int> held;
  for (std::size_t index = 0; index < first; ++index) {
    held.push_back(open_one());
    table.add(held.back());
  }
  // A client reconnecting: the one connection's socket closes and another
  // opens, over and over, every other slot taken.
  for (int cycle = 0; cycle < 1000 && table.capacity() == first; ++cycle) {
    table.remove(held.back());
    ::close(held.back());
    held.back() = open_one();
    table.add(held.back());
  }
  EXPECT_EQ(first, table.capacity());
  table.close_all();
  for (int fd : held) {
    EXPECT_FALSE(is_open(fd));
  }
}
