// SimpleTimer: the timeout object every client call is bounded by.
#ifndef MX_LIB_TIMER_H_
#define MX_LIB_TIMER_H_

#include "lib/assertion.h"
#include "lib/logging/logging.h"
#include "lib/repr.h"
#include <asio/io_service.hpp>
#include <asio/steady_timer.hpp>
#include <chrono>
#include <memory>

namespace mx {

// A deadline for one call: expired() becomes true `time` seconds after
// construction, provided the io_service runs meanwhile (it does, inside
// every client call). The expiry flag is shared with the pending handler by
// shared_ptr, so the timer object can be destroyed before the handler
// runs, which is what makes it safe to hold in a unique_ptr on the stack.
struct SimpleTimer {
  SimpleTimer(const SimpleTimer &) = delete;
  SimpleTimer &operator=(const SimpleTimer &) = delete;
  typedef std::shared_ptr<bool> ExpiryHolder;

  /*
   * create SimpleTimer that expires in `time' seconds
   * `time' must be at least 0. If it's exactly 0, such a timer
   * expires immediately.
   */
  SimpleTimer(asio::io_service &io_service, float time)
      : timer_(io_service, std::chrono::microseconds(static_cast<long>(time * 1e6))),
        expiry_holder_(new bool(time == 0)) {
    Assert(time >= 0);
    Assert(time == 0 || !expired());
    if (!expired()) {
      ExpiryHolder holder = expiry_holder_;
      timer_.async_wait([holder](const asio::error_code &error) {
        if (error != asio::error::operation_aborted)
          *holder = true;
      });
    }
  }

  /*
   * create a SimpleTimer that will never expire
   */
  SimpleTimer(asio::io_service &io_service) : timer_(io_service), expiry_holder_() { Assert(!expired()); }

  ~SimpleTimer() {
    // well... calling cancel here doesn't trigger _expire() immediately
    // so we can't let expiry_holder_ be GCed here
    timer_.cancel();
  }

  inline bool expired() const { return expiry_holder_ && *expiry_holder_; }

private:
  asio::steady_timer timer_;
  ExpiryHolder expiry_holder_;
};

}; // namespace mx

#endif // MX_LIB_TIMER_H_
