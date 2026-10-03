// RecordingControlTask, the recording subcommand's class, which its two
// files share: recording_control.cc, the options, the one-shot start, stop
// and status, and what every action uses; recording_loops.cc, start --stay
// and tap, the commands that keep running. Internal to mxcontrol: nothing
// else includes it. The subcommand itself is described in
// recording_control.cc and docs/mxcontrol.md.
#ifndef MX_MXCONTROL_RECORDING_CONTROL_H_
#define MX_MXCONTROL_RECORDING_CONTROL_H_

#include <chrono>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "lib/seconds.h"
#include "multiplexer/Recording.pb.h" /* generated */
#include "multiplexer/client.h"
#include "mxcontrol/every_address.h"
#include "mxcontrol/task.h"

namespace mxcontrol {

// See the file comment. Used on the main thread, which runs the client's
// loop inside its calls.
class RecordingControlTask : public Task {
 public:
  virtual int run();
  virtual std::string short_description() const { return "start, stop, query or tap the recording of multiplexers"; }
  virtual std::string short_synopsis(const std::string& commandname) {
    return "<" + commandname + "-options> start|stop|status|tap";
  }
  virtual void print_help(std::ostream& out) {
    out << "Drive recording on every --multiplexer given, over the protocol.\n"
        << "  start   open a file session named --label in each multiplexer's --recording-dir\n"
        << "  stop    close it\n"
        << "  status  say what each multiplexer is doing\n"
        << "  tap     receive every record as it happens and write them to --out (default stdout)\n"
        << "A host name resolves to all its addresses, one connection each. --stay keeps\n"
        << "start running, starting the session on every multiplexer it reaches that is not\n"
        << "recording, a replica that comes back or one reached later, once each, until\n"
        << "SIGINT, which then stops every session. --stay and tap look the names up again\n"
        << "every couple of seconds and connect to each new address: a replica back under\n"
        << "another. They drop an address no name has any more once its connection is down,\n"
        << "start with nothing reachable too, and fail only for a replica they could not stop\n"
        << "or untap at the end, or records the tap could not write.\n\n"
        << _options();
  }

  // A status and the connection it came through.
  typedef std::pair<multiplexer::RecordingStatus, multiplexer::ConnectionWrapper> Answer;

 protected:
  virtual void _initialize_options(mx::options::Options& options) {
    options.add("action", &action_, "start, stop, status or tap").positional("action");
    options.add("multiplexer,M", &multiplexers_,
                "a multiplexer's address, host:port or [IPv6 address]:port, an empty host meaning 127.0.0.1; a name "
                "stands for every address it resolves to; may be repeated");
    options.add("type", &peer_type_, multiplexer::RECORDING_CONTROLLER,
                "peer type to connect as (default: the reserved recording controller)");
    options.add("label", &label_, "session", "start: the session's name in the file name");
    options.add("payload-bytes", &payload_bytes_, 0,
                "start, tap: keep only the first N bytes of each payload; 0 keeps all");
    options.add("max-bytes", &max_bytes_text_, "start: close the session at this size; default 1 GiB, 0 for no cap");
    options.add("max-seconds", &max_seconds_, 0, "start: close the session after this long");
    options.add_switch("stay", &stay_,
                       "start: keep running, starting the session once on every multiplexer reached that is not "
                       "recording");
    options.add("out", &out_, "tap: write the records to this file instead of stdout");
    options.add("timeout", &timeout_, 5.0, "seconds to wait for connections and answers");
  }

 private:
  // A point in time to wait until, in seconds left.
  struct Deadline {
    explicit Deadline(float seconds) : end(std::chrono::steady_clock::now() + mx::from_seconds(seconds)) {}
    float remaining() const {
      const float left = std::chrono::duration<float>(end - std::chrono::steady_clock::now()).count();
      return left > 0 ? left : 0;
    }
    bool expired() const { return remaining() <= 0; }
    std::chrono::steady_clock::time_point end;
  };

  // How often the stay and tap loops ask every multiplexer for its status,
  // which is how a restarted replica is noticed and started or tapped again,
  // and resolve the names again, which is how one that came back under a new
  // address is connected to first.
  static constexpr float POLL_SECONDS = 2.0;

  // One status as a line: which multiplexer, what it is doing, and the error
  // if the request was refused.
  static std::string _describe(const multiplexer::RecordingStatus& status, bool with_tap);
  // Queues `control` on every connection; returns the request id.
  std::uint64_t _send(multiplexer::Client& client, const multiplexer::RecordingControl& control);
  // Queues `control` on one connection.
  void _send(multiplexer::Client& client, const multiplexer::RecordingControl& control,
             multiplexer::ConnectionWrapper connection);
  // Sends `control` everywhere and collects one status per connection,
  // printing each, and keeping each with its connection in `answers` when
  // given; returns false if any was an error or missing.
  bool _control(multiplexer::Client& client, const multiplexer::RecordingControl& control, bool with_tap = false,
                std::vector<Answer>* answers = nullptr);
  // The commands that keep running, in recording_loops.cc: start --stay,
  // `start` first on whatever is reachable, and tap.
  int _stay(multiplexer::Client& client, EveryAddress& addresses, const multiplexer::RecordingControl& start);
  int _tap(multiplexer::Client& client, EveryAddress& addresses);

  std::string action_;
  std::uint32_t peer_type_;
  std::string label_;
  unsigned int payload_bytes_;
  std::string max_bytes_text_;  // --max-bytes as typed; empty when not given
  std::optional<std::uint64_t> max_bytes_;
  unsigned int max_seconds_;
  bool stay_;
  std::string out_;
  float timeout_;
  bool unreachable_ = false;  // an address reached none of its multiplexers: a one-shot command fails
};

}  // namespace mxcontrol

#endif  // MX_MXCONTROL_RECORDING_CONTROL_H_
