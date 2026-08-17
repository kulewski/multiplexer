// Random numbers for message and instance ids; see Random64.
#ifndef MX_LIB_RANDOM_H_
#define MX_LIB_RANDOM_H_

#include "lib/assertion.h"
#include <boost/cstdint.hpp>
#include <boost/random/linear_congruential.hpp>

namespace mx {

struct AutoSeedingRand48 : public boost::rand48 {
  AutoSeedingRand48();
};

// A 64-bit generator built from two rand48 streams, each seeded from
// /dev/urandom once at construction. Fast enough for one call per message:
// message ids and instance ids come from here, and they only need to be
// distinct, not unpredictable (see docs/faq.md on the threat model).
struct Random64 {
  typedef boost::uint64_t result_type;
  result_type operator()() { return ((boost::uint64_t)a_() << 32) | b_(); }

private:
  AutoSeedingRand48 a_, b_;
};
}; // namespace mx

#endif // MX_LIB_RANDOM_H_
