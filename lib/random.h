// Random numbers for message and instance ids; see Random64.
#ifndef MX_LIB_RANDOM_H_
#define MX_LIB_RANDOM_H_

#include <cstdint>
#include <random>

namespace mx {

// A 64-bit generator seeded from /dev/urandom once at construction. Fast
// enough for one call per message: message ids and instance ids come from
// here, and they only need to be distinct, not unpredictable (see
// docs/faq.md on the threat model).
struct Random64 {
  typedef std::uint64_t result_type;
  Random64();
  result_type operator()() { return engine_(); }

 private:
  std::mt19937_64 engine_;
};
};  // namespace mx

#endif  // MX_LIB_RANDOM_H_
