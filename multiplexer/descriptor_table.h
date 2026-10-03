// The descriptors of a client's sockets, kept for a child forked from its
// process to close: the client's owner thread records each socket as it
// opens and forgets it before it closes, and a child reads the table in
// its copy of memory, frozen at whatever instruction the owner thread was
// on when another thread forked. So everything a child reads changes by
// one atomic store of a word: a slot, or the pointer to the block of
// slots, which a grown block replaces only once it is whole. A block
// outgrown is kept until the table is destroyed, so that no child holds a
// pointer into freed memory; each block twice the last, those kept take
// less room than the one in use. Slots freed are taken again, so the table
// never grows past the most sockets the client had open at once, however
// often it reconnects. Neither side takes a lock, and the child allocates
// nothing.
#ifndef MX_MULTIPLEXER_DESCRIPTOR_TABLE_H_
#define MX_MULTIPLEXER_DESCRIPTOR_TABLE_H_

#include <atomic>
#include <cstddef>
#include <memory>

namespace multiplexer {

class DescriptorTable {
 public:
  DescriptorTable();
  DescriptorTable(const DescriptorTable&) = delete;
  DescriptorTable& operator=(const DescriptorTable&) = delete;

  // Owner thread: records `fd`, a socket just opened, in the first free
  // slot, growing the table when every slot is taken.
  void add(int fd);
  // Owner thread: forgets `fd` before it is closed, so that a child forked
  // after this leaves the number alone, which the process may give to a
  // file next. Every slot holding it is freed.
  void remove(int fd);
  // A forked child: closes its copy of every descriptor recorded, with
  // close(2) only.
  void close_all() const;
  // Owner thread: how many slots the table has.
  std::size_t capacity() const;

 private:
  // Slots, free at first, and the block they outgrew.
  struct Block {
    explicit Block(std::size_t size);
    const std::size_t size;
    std::unique_ptr<std::atomic<int>[]> slots;
    std::unique_ptr<Block> outgrown;
  };

  std::unique_ptr<Block> owned_;       // the block in use, owning those it outgrew
  std::atomic<const Block*> current_;  // the same block, for a child to read
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_DESCRIPTOR_TABLE_H_
