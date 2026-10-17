// mx::resize_uninitialized(): a string's size changed without its new bytes
// filled, for a buffer about to be written over whole, a frame's body read
// or serialized into. std::string::resize() writes a zero into every new
// byte, a pass over a megabyte for a frame of a batch, which the bytes
// written next make for nothing. Standard C++ has no way to skip it before
// C++23's resize_and_overwrite; each standard library has one of its own,
// found here at compile time, as Abseil's STLStringResizeUninitialized
// finds libc++'s: libc++'s __resize_default_init, and libstdc++'s
// __resize_and_overwrite where it has it (GCC 14's does, in C++11 and
// later). Without either, resize() fills, as before, and
// RESIZE_SKIPS_FILL says which it is.
//
// The new bytes hold whatever the memory held: nothing may read them before
// they are written. Shrinking cuts the string as resize() does. Written to
// C++11, as everything in lib/ that a build tool may link.
#ifndef MX_LIB_RESIZE_UNINITIALIZED_H_
#define MX_LIB_RESIZE_UNINITIALIZED_H_

#include <cstddef>
#include <string>
#include <type_traits>
#include <utility>

namespace mx {

namespace resize_detail {

// What __resize_and_overwrite runs on the grown buffer: it keeps every
// byte and writes none.
struct KeepAll {
  std::size_t operator()(char*, std::size_t size) const { return size; }
};

// libc++'s, the first choice: an int argument prefers it to the others.
template <typename String>
auto resize(String* string, std::size_t size, int) -> decltype(string->__resize_default_init(size)) {
  string->__resize_default_init(size);
}

// libstdc++'s.
template <typename String>
auto resize(String* string, std::size_t size, long) -> decltype(string->__resize_and_overwrite(size, KeepAll())) {
  string->__resize_and_overwrite(size, KeepAll());
}

// Neither: filled.
template <typename String>
void resize(String* string, std::size_t size, ...) {
  string->resize(size);
}

template <typename String>
auto skips(int) -> decltype(std::declval<String&>().__resize_default_init(0), std::true_type());
template <typename String>
auto skips(long) -> decltype(std::declval<String&>().__resize_and_overwrite(0, KeepAll()), std::true_type());
template <typename String>
std::false_type skips(...);

}  // namespace resize_detail

// Whether resize_uninitialized() leaves the new bytes as they were, which
// this standard library allows; false when it fills them, as resize() does.
constexpr bool RESIZE_SKIPS_FILL = decltype(resize_detail::skips<std::string>(0))::value;

// `string` resized to `size`, its new bytes not filled; see above.
inline void resize_uninitialized(std::string* string, std::size_t size) { resize_detail::resize(string, size, 0); }

}  // namespace mx

#endif  // MX_LIB_RESIZE_UNINITIALIZED_H_
