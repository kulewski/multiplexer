// recording: start, stop and ask about recording sessions on running
// multiplexers, or tap in and receive every record live, over the protocol
// itself (RecordingControl in Recording.proto). Connects to every
// --multiplexer as a RECORDING_CONTROLLER, resolving a host name to all its
// addresses, so one command reaches every replica behind a name; --stay and
// tap resolve the names again at every poll, so they reach a replica that
// comes back under a new address too, and let go of an address that is
// gone (every_address.h). Those two start with nothing reachable as well,
// the polls finding the replicas as they come, and their exit status is
// decided at the end, by the replicas they could not stop or untap. See
// docs/operations.md.
//
// This file has the options, the one-shot actions and what every action
// uses; the two that keep running are in recording_loops.cc, and the class
// in recording_control.h.
#include "mxcontrol/recording_control.h"

#include <iostream>
#include <set>

#include "lib/repr.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "mxcontrol/tasks_holder.h"

using multiplexer::Client;
using multiplexer::ConnectionWrapper;
using multiplexer::MultiplexerMessage;
using multiplexer::RecordingControl;
using multiplexer::RecordingStatus;
using mx::repr;

namespace mxcontrol {

REGISTER_MXCONTROL_SUBCOMMAND(recording, mxcontrol::RecordingControlTask);

std::string RecordingControlTask::_describe(const RecordingStatus& status, bool with_tap) {
  std::string line = "multiplexer " + repr(status.multiplexer_id()) + ": ";
  if (status.has_error()) {
    return line + "error: " + status.error();
  }
  if (status.recording()) {
    line +=
        "recording " + status.path() + " (" + repr(status.records()) + " records, " + repr(status.bytes()) + " bytes";
    if (status.has_label()) {
      line += ", label " + status.label();
    }
    line += ")";
  } else if (status.has_path()) {
    line += "not recording; last " + status.path() + " (" + repr(status.records()) + " records, " +
            repr(status.bytes()) + " bytes) " + status.stopped();
  } else {
    line += "not recording";
  }
  if (with_tap) {
    line += "; " + std::string(status.tapping() ? "tapping" : "not tapping") + " (" + repr(status.taps()) + " taps, " +
            repr(status.dropped()) + " dropped)";
  }
  return line;
}

std::uint64_t RecordingControlTask::_send(Client& client, const RecordingControl& control) {
  MultiplexerMessage mxmsg;
  mxmsg.set_id(client.random64());
  mxmsg.set_from(client.instance_id());
  mxmsg.set_type(multiplexer::RECORDING_CONTROL);
  control.SerializeToString(mxmsg.mutable_message());
  client.schedule_all(mxmsg);
  return mxmsg.id();
}

void RecordingControlTask::_send(Client& client, const RecordingControl& control, ConnectionWrapper connection) {
  MultiplexerMessage mxmsg;
  mxmsg.set_id(client.random64());
  mxmsg.set_from(client.instance_id());
  mxmsg.set_type(multiplexer::RECORDING_CONTROL);
  control.SerializeToString(mxmsg.mutable_message());
  client.schedule_one(mxmsg, connection, timeout_);
}

bool RecordingControlTask::_control(Client& client, const RecordingControl& control, bool with_tap,
                                    std::vector<Answer>* answers) {
  const unsigned int expected = client.connections_count();
  if (!expected) {
    std::cerr << "not connected to any multiplexer\n";
    return false;
  }
  const std::uint64_t request = _send(client, control);
  std::set<std::uint64_t> answered;
  bool ok = true;
  Deadline timer(timeout_);
  while (answered.size() < expected) {
    std::pair<std::shared_ptr<MultiplexerMessage>, ConnectionWrapper> incoming;
    try {
      incoming = client.receive_message(timer.remaining() > 0 ? timer.remaining() : 0.01f);
    } catch (const Client::OperationTimedOut&) {
      break;
    } catch (const Client::NotConnected&) {
      break;
    }
    const MultiplexerMessage& mxmsg = *incoming.first;
    if (mxmsg.type() != multiplexer::RECORDING_STATUS || mxmsg.references() != request) {
      continue;
    }
    RecordingStatus status;
    if (!status.ParseFromString(mxmsg.message())) {
      continue;
    }
    answered.insert(status.multiplexer_id());
    if (answers) {
      answers->emplace_back(status, incoming.second);
    }
    std::cout << _describe(status, with_tap) << "\n";
    if (status.has_error()) {
      ok = false;
    }
  }
  std::cout.flush();
  if (answered.size() < expected) {
    std::cerr << (expected - answered.size()) << " of " << expected << " multiplexer(s) did not answer\n";
    ok = false;
  }
  return ok;
}

int RecordingControlTask::run() {
  if (!max_bytes_text_.empty()) {
    max_bytes_ = mx::from_string<std::uint64_t>(max_bytes_text_);
  }
  if (action_ != "start" && action_ != "stop" && action_ != "status" && action_ != "tap") {
    std::cerr << "action must be start, stop, status or tap\n";
    return 2;
  }
  if (multiplexers_.empty()) {
    std::cerr << "give at least one --multiplexer host:port\n";
    return 2;
  }
  // --stay and tap keep running, the polls finding the replicas as they
  // come: they start with none reachable too.
  const bool keeps_running = (action_ == "start" && stay_) || action_ == "tap";
  Client client(io_service(), peer_type_);
  EveryAddress addresses(client, io_service(), multiplexers_);
  const unsigned int reached = addresses.connect(timeout_);
  unreachable_ = reached < multiplexers_.size();
  if (!client.connections_count()) {
    if (!keeps_running) {
      std::cerr
          << "no multiplexer reachable, or none accepting a recording controller (--recording-dir, --allow-tap)\n";
      return 1;
    }
    std::cerr << "no multiplexer reachable yet; looking again every " << POLL_SECONDS << " s\n";
  } else if (unreachable_) {
    std::cerr << (multiplexers_.size() - reached) << " of " << multiplexers_.size()
              << " multiplexer address(es) could not be reached\n";
  }
  RecordingControl control;
  if (action_ == "start") {
    control.set_action(RecordingControl::START);
    control.set_label(label_);
    control.set_payload_limit(payload_bytes_);
    if (max_bytes_) {
      control.set_max_bytes(*max_bytes_);
    }
    control.set_max_seconds(max_seconds_);
    if (stay_) {
      return _stay(client, addresses, control);
    }
    return _control(client, control) && !unreachable_ ? 0 : 1;
  }
  if (action_ == "stop") {
    control.set_action(RecordingControl::STOP);
    return _control(client, control) && !unreachable_ ? 0 : 1;
  }
  if (action_ == "status") {
    control.set_action(RecordingControl::STATUS);
    return _control(client, control, true) && !unreachable_ ? 0 : 1;
  }
  return _tap(client, addresses);
}

}  // namespace mxcontrol
