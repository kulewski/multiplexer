// SimpleTimer: the timeout object every client call is bounded by.
#ifndef MX_LIB_TIMER_H_
#define MX_LIB_TIMER_H_

#include "lib/assertion.h"
#include "lib/intrusive_value.h"
#include "lib/logging/logging.h"
#include "lib/repr.h"
#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/bind/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/numeric/conversion/cast.hpp>

namespace mx {

// A deadline for one call: expired() becomes true `time` seconds after
// construction, provided the io_service runs meanwhile (it does, inside
// every client call). The expiry flag is shared with the pending handler by
// intrusive_ptr, so the timer object can be destroyed before the handler
// runs, which is what makes it safe to hold in a unique_ptr on the stack.
struct SimpleTimer : boost::noncopyable {
  typedef boost::intrusive_ptr<IntrusiveValue<bool>> ExpiryHolder;

  /*
   * create SimpleTimer that expires in `time' seconds
   * `time' must be at least 0. If it's exactly 0, such a timer
   * expires immediately.
   */
  SimpleTimer(boost::asio::io_service &io_service, float time)
      : timer_(io_service, boost::posix_time::microseconds(boost::numeric_cast<long>(time * 1e6))),
        expiry_holder_(new IntrusiveValue<bool>(time == 0)) {
    Assert(time >= 0);
    Assert(time == 0 || !expired());
    if (!expired()) {
      timer_.async_wait(boost::bind(&SimpleTimer::_expire, expiry_holder_, boost::asio::placeholders::error));
    }
  }

  /*
   * create a SimpleTimer that will never expire
   */
  SimpleTimer(boost::asio::io_service &io_service) : timer_(io_service), expiry_holder_() { Assert(!expired()); }

  ~SimpleTimer() {
    // well... calling cancel here doesn't trigger _expire() immediately
    // so we can't let expiry_holder_ be GCed here
    timer_.cancel();
  }

  inline bool expired() const { return expiry_holder_ && *expiry_holder_; }

private:
  static void _expire(ExpiryHolder expiry_holder, const boost::system::error_code &error) {
    if (error != boost::asio::error::operation_aborted) {
      *expiry_holder = true;
    }
  }

private:
  boost::asio::deadline_timer timer_;
  ExpiryHolder expiry_holder_;
};

}; // namespace mx

#endif // MX_LIB_TIMER_H_
