// mx::from_seconds(): a time in seconds, as the libraries and the
// multiplexer take every timeout and interval, as a std::chrono duration.
//
// One rule for every timeout, in every client and server class: a negative
// one sets no deadline, as an infinite one does, and NaN is no time at
// all. The places that read a timeout without this function follow it too:
// BasicClient::create_timer, the outbox's deadline_after and, in Python,
// wait_seconds() in multiplexer/mxclient.py.
#ifndef MX_LIB_SECONDS_H_
#define MX_LIB_SECONDS_H_

#include <chrono>
#include <cmath>

namespace mx {

// The longest a timeout or an interval stands for, about 31.7 years: added
// to a steady clock's now, it stays within the 64-bit count of nanoseconds
// the clock keeps, where a larger one, or an infinite one, wrapped around
// to a deadline in the past and timed out at once.
constexpr double MAX_SECONDS = 1e9;

// `seconds` as microseconds. An infinite value, one past MAX_SECONDS, and a
// negative one, no deadline either, are MAX_SECONDS: as long as it takes.
// NaN is no time at all, 0: a timer expires at once. It never throws: the
// clients convert on their io thread too, where a throw would leave a
// caller waiting.
inline std::chrono::microseconds from_seconds(double seconds) {
  if (std::isnan(seconds)) {
    return std::chrono::microseconds(0);
  }
  if (seconds > MAX_SECONDS || seconds < 0) {
    seconds = MAX_SECONDS;
  }
  return std::chrono::microseconds(static_cast<long long>(seconds * 1e6));
}

}  // namespace mx

#endif  // MX_LIB_SECONDS_H_
