// recording: start, stop and ask about recording sessions on running
// multiplexers, or tap in and receive every record live, over the protocol
// itself (RecordingControl in Recording.proto). Connects to every
// --multiplexer as a RECORDING_CONTROLLER, resolving a host name to all its
// addresses, so one command reaches every replica behind a name. See
// docs/operations.md.
#include <chrono>
#include <csignal>
#include <fstream>
#include <iostream>
#include <set>

#include <boost/asio/ip/tcp.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>

#include "lib/protobuf/stream.h"
#include "lib/repr.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/Recording.pb.h"   /* generated */
#include "multiplexer/client.h"
#include "mxcontrol/task.h"
#include "mxcontrol/tasks_holder.h"

using multiplexer::Client;
using multiplexer::MultiplexerMessage;
using multiplexer::Record;
using multiplexer::RecordingControl;
using multiplexer::RecordingStatus;
using mx::repr;

namespace mxcontrol {

namespace {

volatile std::sig_atomic_t stop_requested = 0;
void request_stop(int) { stop_requested = 1; }

// A point in time to wait until, in seconds left.
struct Deadline {
  explicit Deadline(float seconds)
      : end(std::chrono::steady_clock::now() + std::chrono::microseconds(static_cast<long>(seconds * 1e6))) {}
  float remaining() const {
    const float left = std::chrono::duration<float>(end - std::chrono::steady_clock::now()).count();
    return left > 0 ? left : 0;
  }
  bool expired() const { return remaining() <= 0; }
  std::chrono::steady_clock::time_point end;
};

// How often the stay and tap loops ask every multiplexer for its status,
// which is how a restarted replica is noticed and started or tapped again.
const float POLL_SECONDS = 2.0;

// One status as a line: which multiplexer, what it is doing, and the error
// if the request was refused.
std::string describe(const RecordingStatus &status, bool with_tap) {
  std::string line = "multiplexer " + repr(status.multiplexer_id()) + ": ";
  if (status.has_error())
    return line + "error: " + status.error();
  if (status.recording()) {
    line +=
        "recording " + status.path() + " (" + repr(status.records()) + " records, " + repr(status.bytes()) + " bytes";
    if (status.has_label())
      line += ", label " + status.label();
    line += ")";
  } else if (status.has_path()) {
    line += "not recording; last " + status.path() + " (" + repr(status.records()) + " records, " +
            repr(status.bytes()) + " bytes) " + status.stopped();
  } else {
    line += "not recording";
  }
  if (with_tap)
    line += "; " + std::string(status.tapping() ? "tapping" : "not tapping") + " (" + repr(status.taps()) + " taps, " +
            repr(status.dropped()) + " dropped)";
  return line;
}

} // namespace

class RecordingControlTask : public Task {
public:
  virtual int run();
  virtual std::string short_description() const { return "start, stop, query or tap the recording of multiplexers"; }
  virtual std::string short_synopsis(const std::string &commandname) {
    return "<" + commandname + "-options> start|stop|status|tap";
  }
  virtual void print_help(std::ostream &out) {
    out << "Drive recording on every --multiplexer given, over the protocol.\n"
        << "  start   open a file session named --label in each multiplexer's --recording-dir\n"
        << "  stop    close it\n"
        << "  status  say what each multiplexer is doing\n"
        << "  tap     receive every record as it happens and write them to --out (default stdout)\n"
        << "A host name resolves to all its addresses, one connection each. --stay keeps\n"
        << "start running, restarting the session on replicas that come back, until SIGINT,\n"
        << "which then stops every session.\n\n"
        << _options_description();
  }

protected:
  virtual void _initialize_options_description(po::options_description &options) {
    options.add_options()("action", po::value(&action_), "start, stop, status or tap")(
        "multiplexer,M", po::value(&multiplexers_)->composing(),
        "multiplexer address as host:port; a name resolves to every address; may be repeated")(
        "type", po::value(&peer_type_)->default_value(multiplexer::RECORDING_CONTROLLER),
        "peer type to connect as (default: the reserved recording controller)")(
        "label", po::value(&label_)->default_value("session"), "start: the session's name in the file name")(
        "payload-bytes", po::value(&payload_bytes_)->default_value(0),
        "start, tap: keep only the first N bytes of each payload; 0 keeps all")(
        "max-bytes", po::value(&max_bytes_), "start: close the session at this size; default 1 GiB, 0 for no cap")(
        "max-seconds", po::value(&max_seconds_)->default_value(0), "start: close the session after this long")(
        "stay", po::bool_switch(&stay_), "start: keep running and restart the session on replicas that come back")(
        "out", po::value(&out_), "tap: write the records to this file instead of stdout")(
        "timeout", po::value(&timeout_)->default_value(5.0), "seconds to wait for connections and answers");
  }
  virtual void _initialize_positional_options_description(po::positional_options_description &positional) {
    positional.add("action", 1);
  }

private:
  // Every address of every --multiplexer, resolved; returns how many connected.
  unsigned int _connect(Client &client);
  // Queues `control` on every connection; returns the request id.
  boost::uint64_t _send(Client &client, const RecordingControl &control);
  // Queues `control` on one connection.
  void _send(Client &client, const RecordingControl &control, multiplexer::ConnectionWrapper connection);
  // Sends `control` everywhere and collects one status per connection,
  // printing each; returns false if any was an error or missing.
  bool _control(Client &client, const RecordingControl &control, bool with_tap = false);
  int _stay(Client &client);
  int _tap(Client &client);

  std::string action_;
  std::vector<std::string> multiplexers_;
  boost::uint32_t peer_type_;
  std::string label_;
  unsigned int payload_bytes_;
  boost::optional<boost::uint64_t> max_bytes_;
  unsigned int max_seconds_;
  bool stay_;
  std::string out_;
  float timeout_;
};

REGISTER_MXCONTROL_SUBCOMMAND(recording, mxcontrol::RecordingControlTask);

unsigned int RecordingControlTask::_connect(Client &client) {
  unsigned int connected = 0;
  BOOST_FOREACH (const std::string &address, multiplexers_) {
    std::string::size_type colon = address.rfind(':');
    if (colon == std::string::npos) {
      std::cerr << "invalid multiplexer address " << address << " (host:port expected)\n";
      continue;
    }
    std::string host = address.substr(0, colon);
    if (host.empty())
      host = "127.0.0.1";
    const std::string port = address.substr(colon + 1);
    boost::asio::ip::tcp::resolver resolver(io_service());
    boost::asio::ip::tcp::resolver::iterator end;
    try {
      boost::asio::ip::tcp::resolver::query query(host, port);
      for (boost::asio::ip::tcp::resolver::iterator entry = resolver.resolve(query); entry != end; ++entry) {
        const boost::asio::ip::tcp::endpoint endpoint = *entry;
        if (client.connect(endpoint, timeout_)) {
          ++connected;
        } else {
          std::cerr << "cannot connect to " << endpoint << "\n";
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "cannot resolve " << address << ": " << e.what() << "\n";
    }
  }
  return connected;
}

boost::uint64_t RecordingControlTask::_send(Client &client, const RecordingControl &control) {
  MultiplexerMessage mxmsg;
  mxmsg.set_id(client.random64());
  mxmsg.set_from(client.instance_id());
  mxmsg.set_type(multiplexer::RECORDING_CONTROL);
  control.SerializeToString(mxmsg.mutable_message());
  client.schedule_all(mxmsg);
  return mxmsg.id();
}

void RecordingControlTask::_send(Client &client, const RecordingControl &control,
                                 multiplexer::ConnectionWrapper connection) {
  MultiplexerMessage mxmsg;
  mxmsg.set_id(client.random64());
  mxmsg.set_from(client.instance_id());
  mxmsg.set_type(multiplexer::RECORDING_CONTROL);
  control.SerializeToString(mxmsg.mutable_message());
  client.schedule_one(mxmsg, connection, timeout_);
}

bool RecordingControlTask::_control(Client &client, const RecordingControl &control, bool with_tap) {
  const unsigned int expected = client.connections_count();
  if (!expected) {
    std::cerr << "not connected to any multiplexer\n";
    return false;
  }
  const boost::uint64_t request = _send(client, control);
  std::set<boost::uint64_t> answered;
  bool ok = true;
  Deadline timer(timeout_);
  while (answered.size() < expected) {
    std::pair<boost::shared_ptr<MultiplexerMessage>, multiplexer::ConnectionWrapper> incoming;
    try {
      incoming = client.receive_message(timer.remaining() > 0 ? timer.remaining() : 0.01f);
    } catch (const Client::OperationTimedOut &) {
      break;
    } catch (const Client::NotConnected &) {
      break;
    }
    const MultiplexerMessage &mxmsg = *incoming.first;
    if (mxmsg.type() != multiplexer::RECORDING_STATUS || mxmsg.references() != request)
      continue;
    RecordingStatus status;
    if (!status.ParseFromString(mxmsg.message()))
      continue;
    answered.insert(status.multiplexer_id());
    std::cout << describe(status, with_tap) << "\n";
    if (status.has_error())
      ok = false;
  }
  std::cout.flush();
  if (answered.size() < expected) {
    std::cerr << (expected - answered.size()) << " of " << expected << " multiplexer(s) did not answer\n";
    ok = false;
  }
  return ok;
}

int RecordingControlTask::run() {
  if (action_ != "start" && action_ != "stop" && action_ != "status" && action_ != "tap") {
    std::cerr << "action must be start, stop, status or tap\n";
    return 2;
  }
  if (multiplexers_.empty()) {
    std::cerr << "give at least one --multiplexer host:port\n";
    return 2;
  }
  Client client(io_service(), peer_type_);
  _connect(client);
  if (!client.connections_count()) {
    std::cerr << "no multiplexer reachable, or none accepting a recording controller (--recording-dir, --allow-tap)\n";
    return 1;
  }
  RecordingControl control;
  if (action_ == "start") {
    control.set_action(RecordingControl::START);
    control.set_label(label_);
    control.set_payload_limit(payload_bytes_);
    if (max_bytes_)
      control.set_max_bytes(*max_bytes_);
    control.set_max_seconds(max_seconds_);
    const bool ok = _control(client, control);
    if (!stay_)
      return ok ? 0 : 1;
    return _stay(client);
  }
  if (action_ == "stop") {
    control.set_action(RecordingControl::STOP);
    return _control(client, control) ? 0 : 1;
  }
  if (action_ == "status") {
    control.set_action(RecordingControl::STATUS);
    return _control(client, control, true) ? 0 : 1;
  }
  return _tap(client);
}

// Until SIGINT or SIGTERM: ask every multiplexer for its status every
// POLL_SECONDS, start a session on any that has never had one (a replica
// that restarted), then stop every session on the way out.
int RecordingControlTask::_stay(Client &client) {
  std::signal(SIGINT, request_stop);
  std::signal(SIGTERM, request_stop);
  RecordingControl start;
  start.set_action(RecordingControl::START);
  start.set_label(label_);
  start.set_payload_limit(payload_bytes_);
  if (max_bytes_)
    start.set_max_bytes(*max_bytes_);
  start.set_max_seconds(max_seconds_);
  RecordingControl status_request;
  status_request.set_action(RecordingControl::STATUS);
  while (!stop_requested) {
    Deadline timer(POLL_SECONDS);
    _send(client, status_request);
    while (!stop_requested && !timer.expired()) {
      std::pair<boost::shared_ptr<MultiplexerMessage>, multiplexer::ConnectionWrapper> incoming;
      try {
        incoming = client.receive_message(std::min(timer.remaining(), 0.5f));
      } catch (const Client::OperationTimedOut &) {
        continue;
      } catch (const Client::NotConnected &) {
        continue;
      }
      const MultiplexerMessage &mxmsg = *incoming.first;
      RecordingStatus status;
      if (mxmsg.type() != multiplexer::RECORDING_STATUS || !status.ParseFromString(mxmsg.message()))
        continue;
      if (status.has_error()) {
        std::cout << describe(status, false) << "\n";
      } else if (!status.recording() && !status.has_stopped()) {
        std::cout << "multiplexer " << status.multiplexer_id() << ": no session yet; starting one\n";
        _send(client, start, incoming.second);
      }
      std::cout.flush();
    }
  }
  std::signal(SIGINT, SIG_DFL);
  std::signal(SIGTERM, SIG_DFL);
  RecordingControl stop;
  stop.set_action(RecordingControl::STOP);
  return _control(client, stop) ? 0 : 1;
}

// Until SIGINT or SIGTERM: every RECORDING_RECORD that arrives goes to the
// output as a Record; every POLL_SECONDS a status request finds the
// multiplexers not streaming to us (a replica that restarted) and taps
// them again. Statuses go to stderr, the records are the output.
int RecordingControlTask::_tap(Client &client) {
  std::ofstream file;
  std::ostream *out = &std::cout;
  if (!out_.empty()) {
    file.open(out_.c_str(), std::ios::out | std::ios::binary | std::ios::app);
    if (!file.good()) {
      std::cerr << "cannot open " << out_ << "\n";
      return 1;
    }
    out = &file;
  }
  mx::protobuf::OstreamMessageOutputStream stream(out, false);
  std::signal(SIGINT, request_stop);
  std::signal(SIGTERM, request_stop);
  RecordingControl tap;
  tap.set_action(RecordingControl::TAP);
  tap.set_payload_limit(payload_bytes_);
  RecordingControl status_request;
  status_request.set_action(RecordingControl::STATUS);
  std::set<boost::uint64_t> tapped;
  boost::uint64_t records = 0;
  _send(client, tap);
  while (!stop_requested) {
    Deadline timer(POLL_SECONDS);
    while (!stop_requested && !timer.expired()) {
      std::pair<boost::shared_ptr<MultiplexerMessage>, multiplexer::ConnectionWrapper> incoming;
      try {
        incoming = client.receive_message(std::min(timer.remaining(), 0.5f));
      } catch (const Client::OperationTimedOut &) {
        continue;
      } catch (const Client::NotConnected &) {
        continue;
      }
      const MultiplexerMessage &mxmsg = *incoming.first;
      if (mxmsg.type() == multiplexer::RECORDING_RECORD) {
        Record record;
        if (record.ParseFromString(mxmsg.message())) {
          stream.write(record);
          ++records;
        }
        continue;
      }
      RecordingStatus status;
      if (mxmsg.type() != multiplexer::RECORDING_STATUS || !status.ParseFromString(mxmsg.message()))
        continue;
      if (status.has_error()) {
        std::cerr << describe(status, true) << "\n";
        continue;
      }
      if (status.tapping()) {
        if (tapped.insert(status.multiplexer_id()).second)
          std::cerr << "multiplexer " << status.multiplexer_id() << ": tapping\n";
      } else {
        std::cerr << "multiplexer " << status.multiplexer_id() << ": not tapping; tapping again\n";
        _send(client, tap, incoming.second);
      }
    }
    out->flush();
    _send(client, status_request);
  }
  std::signal(SIGINT, SIG_DFL);
  std::signal(SIGTERM, SIG_DFL);
  RecordingControl untap;
  untap.set_action(RecordingControl::UNTAP);
  std::streambuf *cout_buffer = std::cout.rdbuf();
  if (out == &std::cout)
    std::cout.rdbuf(std::cerr.rdbuf()); // the statuses must not land among the records
  const bool ok = _control(client, untap, true);
  std::cout.rdbuf(cout_buffer);
  out->flush();
  std::cerr << records << " records written\n";
  return ok ? 0 : 1;
}

} // namespace mxcontrol
