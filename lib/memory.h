// heap_in_use_bytes(): the bytes currently allocated from the C heap by
// this process, exact, from glibc's mallinfo2 (mallinfo on glibc before
// 2.33, where it wraps above 2 GB). The measure the soak tests compare over
// time: a process that leaks per message grows here, while caches and
// buffers plateau. Cheap, but not for per-message paths.
#ifndef MX_LIB_MEMORY_H_
#define MX_LIB_MEMORY_H_

#include <cstddef>

namespace mx {

size_t heap_in_use_bytes();

} // namespace mx

#endif // MX_LIB_MEMORY_H_
