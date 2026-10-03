// DescriptorTable: see descriptor_table.h.
#include "multiplexer/descriptor_table.h"

#include <unistd.h>

#include <utility>

namespace multiplexer {

namespace {

const int FREE = -1;               // a slot that holds no descriptor
const std::size_t FIRST_SIZE = 8;  // a client's targets and those closing, usually
}  // namespace

DescriptorTable::Block::Block(std::size_t size) : size(size), slots(new std::atomic<int>[size]) {
  for (std::size_t slot = 0; slot < size; ++slot) {
    slots[slot].store(FREE, std::memory_order_relaxed);
  }
}

DescriptorTable::DescriptorTable() : owned_(new Block(FIRST_SIZE)), current_(owned_.get()) {}

void DescriptorTable::add(int fd) {
  Block& block = *owned_;
  for (std::size_t slot = 0; slot < block.size; ++slot) {
    if (block.slots[slot].load(std::memory_order_relaxed) == FREE) {
      block.slots[slot].store(fd, std::memory_order_release);
      return;
    }
  }
  // Every slot taken: a block twice the size, whole before a child can see
  // it, the slots keeping their places.
  std::unique_ptr<Block> grown(new Block(block.size * 2));
  for (std::size_t slot = 0; slot < block.size; ++slot) {
    grown->slots[slot].store(block.slots[slot].load(std::memory_order_relaxed), std::memory_order_relaxed);
  }
  grown->slots[block.size].store(fd, std::memory_order_relaxed);
  grown->outgrown = std::move(owned_);
  owned_ = std::move(grown);
  current_.store(owned_.get(), std::memory_order_release);
}

void DescriptorTable::remove(int fd) {
  Block& block = *owned_;
  for (std::size_t slot = 0; slot < block.size; ++slot) {
    if (block.slots[slot].load(std::memory_order_relaxed) == fd) {
      block.slots[slot].store(FREE, std::memory_order_release);
    }
  }
}

void DescriptorTable::close_all() const {
  const Block* block = current_.load(std::memory_order_acquire);
  for (std::size_t slot = 0; slot < block->size; ++slot) {
    const int fd = block->slots[slot].load(std::memory_order_relaxed);
    if (fd != FREE) {
      ::close(fd);
    }
  }
}

std::size_t DescriptorTable::capacity() const { return owned_->size; }

}  // namespace multiplexer
