// Kwargs::check_keys, the one member that needs KwargsKeys' internals.

#include "lib/kwargs.h"

namespace mx {
namespace util {
namespace kwargs {

bool Kwargs::check_keys(const KwargsKeys& keys) {
  for (const KwValue& kw : __values) {
    if (!keys.__keys.count(kw.first)) {
      return false;
    }
  }
  return true;
}

};  // namespace kwargs
};  // namespace util
};  // namespace mx
