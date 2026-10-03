// recording start --stay and recording tap: the recording subcommand's two
// commands that keep running until SIGINT or SIGTERM, polling every
// multiplexer for its status and following the -M names (every_address.h).
// --stay starts the session on every multiplexer it reaches that is not
// recording, once each, and stops every session on the way out; tap writes
// every record that arrives and taps again a multiplexer not streaming to
// it. Each decides its exit status at the end, from what each address's
// multiplexer said last and from any refusal on the way: a START or a TAP
// a multiplexer refused, taps being off say, is not sent to it again, and
// the command exits 1. The class and the rest of the subcommand are in
// recording_control.h and recording_control.cc.
#include <algorithm>
#include <asio/ip/tcp.hpp>
#include <csignal>
#include <fstream>
#include <iostream>
#include <map>
#include <set>

#include "lib/protobuf/stream.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "mxcontrol/recording_control.h"

using multiplexer::Client;
using multiplexer::ConnectionWrapper;
using multiplexer::MultiplexerMessage;
using multiplexer::Record;
using multiplexer::RecordingControl;
using multiplexer::RecordingStatus;

namespace mxcontrol {

namespace {

volatile std::sig_atomic_t stop_requested = 0;
void request_stop(int) { stop_requested = 1; }

typedef RecordingControlTask::Answer Answer;

// What a stay or tap loop last heard from the multiplexer at an address:
// which one it is, whether it records (stay) or streams to us (tap), and
// the connection it answered through. What their exit status is decided
// by, at the end.
struct Heard {
  std::uint64_t multiplexer_id = 0;
  bool active = false;
  ConnectionWrapper connection;
};
typedef std::map<asio::ip::tcp::endpoint, Heard> HeardByAddress;

// Keeps what `status`, through `connection`, says of its multiplexer:
// `active`, recording or streaming to us.
void note(HeardByAddress& heard, const RecordingStatus& status, const ConnectionWrapper& connection, bool active) {
  Heard& last = heard[connection.endpoint()];
  last.multiplexer_id = status.multiplexer_id();
  last.active = active;
  last.connection = connection;
}

// The ids of the multiplexers among `answers`.
std::set<std::uint64_t> ids_of(const std::vector<Answer>& answers) {
  std::set<std::uint64_t> ids;
  for (const Answer& answer : answers) {
    ids.insert(answer.first.multiplexer_id());
  }
  return ids;
}

// The end of --stay, given the answers to the STOP sent on every live
// connection: true unless a replica last heard recording was not reached,
// one whose connection lives that did not answer in time, or one whose
// connection is down at an address a -M name still resolves to, cut off
// maybe, its session going on. One at an address no name resolves to any
// more was replaced, or let go. Not an address that never answered:
// nothing says it records. Each one not reached is said on stderr.
bool every_session_stopped(const HeardByAddress& heard, const std::vector<Answer>& answers,
                           const EveryAddress& addresses) {
  const std::set<std::uint64_t> stopped = ids_of(answers);
  bool ok = true;
  for (const HeardByAddress::value_type& entry : heard) {
    const Heard& last = entry.second;
    if (!last.active || stopped.count(last.multiplexer_id)) {
      continue;
    }
    if (last.connection) {
      std::cerr << "multiplexer " << last.multiplexer_id << " at " << entry.first
                << " was recording and did not answer the stop\n";
      ok = false;
    } else if (addresses.resolved(entry.first)) {
      std::cerr << "multiplexer " << last.multiplexer_id << " at " << entry.first
                << " was recording when its connection went down: the stop did not reach it\n";
      ok = false;
    }
  }
  return ok;
}

// The end of a tap, given the answers to the UNTAP sent on every live
// connection: true unless a replica last heard streaming to us, through a
// connection that lives, did not answer in time. A tap ends with its
// connection, so one whose connection is down streams to nobody. Each one
// not untapped is said on stderr.
bool every_tap_ended(const HeardByAddress& heard, const std::vector<Answer>& answers) {
  const std::set<std::uint64_t> untapped = ids_of(answers);
  bool ok = true;
  for (const HeardByAddress::value_type& entry : heard) {
    const Heard& last = entry.second;
    if (last.active && last.connection && !untapped.count(last.multiplexer_id)) {
      std::cerr << "multiplexer " << last.multiplexer_id << " at " << entry.first
                << " was streaming and did not answer the untap\n";
      ok = false;
    }
  }
  return ok;
}

}  // namespace

// `start` on every multiplexer reachable now, then until SIGINT or
// SIGTERM: ask every multiplexer for its status every POLL_SECONDS, start a
// session on any that is not recording and that this run has not started,
// a replica that restarted or one reached since, then stop every session on
// the way out. Each multiplexer is started once: one whose session of this
// run ended, at its cap or by someone's stop, or that refused it, is not
// started again. Each poll also connects to the addresses the names
// resolve to now, which the next one asks, and lets go of those gone. What
// the answers say decides nothing until the end (every_session_stopped),
// but a START a multiplexer refused, which makes it exit 1.
int RecordingControlTask::_stay(Client& client, EveryAddress& addresses, const RecordingControl& start) {
  std::signal(SIGINT, request_stop);
  std::signal(SIGTERM, request_stop);
  HeardByAddress heard;
  std::set<std::uint64_t> started;  // every multiplexer this run sent `start` to
  bool refused = false;             // a multiplexer refused it
  if (client.connections_count()) {
    std::vector<Answer> answers;
    _control(client, start, false, &answers);
    for (const Answer& answer : answers) {
      addresses.heard(answer.second);
      note(heard, answer.first, answer.second, answer.first.recording());
      started.insert(answer.first.multiplexer_id());
      refused = refused || answer.first.has_error();
    }
  }
  RecordingControl status_request;
  status_request.set_action(RecordingControl::STATUS);
  while (!stop_requested) {
    Deadline timer(POLL_SECONDS);
    addresses.refresh();
    _send(client, status_request);
    while (!stop_requested && !timer.expired()) {
      std::pair<std::shared_ptr<MultiplexerMessage>, ConnectionWrapper> incoming;
      try {
        incoming = client.receive_message(std::min(timer.remaining(), 0.5f));
      } catch (const Client::OperationTimedOut&) {
        continue;
      } catch (const Client::NotConnected&) {
        continue;
      }
      addresses.heard(incoming.second);
      const MultiplexerMessage& mxmsg = *incoming.first;
      RecordingStatus status;
      if (mxmsg.type() != multiplexer::RECORDING_STATUS || !status.ParseFromString(mxmsg.message())) {
        continue;
      }
      note(heard, status, incoming.second, status.recording());
      if (status.has_error()) {  // the answer to a START: a STATUS never carries one
        std::cout << _describe(status, false) << "\n";
        refused = true;
      } else if (!status.recording() && started.insert(status.multiplexer_id()).second) {
        // One that had a session before, an earlier run's, gets one too.
        std::cout << "multiplexer " << status.multiplexer_id() << ": "
                  << (status.has_stopped() ? "no session of this run" : "no session yet") << "; starting one\n";
        _send(client, start, incoming.second);
      }
      std::cout.flush();
    }
  }
  std::signal(SIGINT, SIG_DFL);
  std::signal(SIGTERM, SIG_DFL);
  RecordingControl stop;
  stop.set_action(RecordingControl::STOP);
  std::vector<Answer> answers;
  _control(client, stop, false, &answers);
  const bool stopped = every_session_stopped(heard, answers, addresses);
  if (refused) {
    std::cerr << "a multiplexer refused the session; see its answer above\n";
  }
  return stopped && !refused ? 0 : 1;
}

// Until SIGINT or SIGTERM: every RECORDING_RECORD that arrives goes to the
// output as a Record; every POLL_SECONDS a status request finds the
// multiplexers not streaming to us (a replica that restarted, or one that
// came up since) and taps them again, the addresses the names resolve to
// now included, connected to at the start of the round; one that refused
// the tap is not asked again. Statuses go to stderr, the records are the
// output, appended to --out after its last whole record. The exit status
// says whether every record reached the output, none dropped here for a
// full incoming queue, the tap ended everywhere it streamed, and no
// multiplexer refused it.
int RecordingControlTask::_tap(Client& client, EveryAddress& addresses) {
  std::ofstream file;
  std::ostream* out = &std::cout;
  if (!out_.empty()) {
    Record scratch;
    const std::int64_t cut = mx::protobuf::cut_torn_tail(out_, scratch);
    if (cut < 0) {
      std::cerr << "cannot read " << out_ << " to find its last whole record\n";
      return 1;
    }
    if (cut > 0) {
      std::cerr << "cut " << cut << " bytes of a record left half written at the end of " << out_ << "\n";
    }
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
  HeardByAddress heard;
  std::set<std::uint64_t> tapped;
  std::set<std::uint64_t> refused;  // the multiplexers that refused the tap, not asked again
  std::uint64_t records = 0;
  bool written = true;  // every record reached the output
  // A record to the output, a status to stderr, a multiplexer not
  // streaming to us tapped again unless it refused.
  const auto handle = [&](const std::pair<std::shared_ptr<MultiplexerMessage>, ConnectionWrapper>& incoming) {
    addresses.heard(incoming.second);
    const MultiplexerMessage& mxmsg = *incoming.first;
    if (mxmsg.type() == multiplexer::RECORDING_RECORD) {
      Record record;
      if (record.ParseFromString(mxmsg.message())) {
        if (stream.write(record)) {
          ++records;
        } else {
          written = false;
        }
      }
      return;
    }
    RecordingStatus status;
    if (mxmsg.type() != multiplexer::RECORDING_STATUS || !status.ParseFromString(mxmsg.message())) {
      return;
    }
    note(heard, status, incoming.second, status.tapping());
    if (status.has_error()) {  // the answer to a TAP: a STATUS never carries one
      if (refused.insert(status.multiplexer_id()).second) {
        std::cerr << _describe(status, true) << "; not asking it again\n";
      }
      return;
    }
    if (status.tapping()) {
      if (tapped.insert(status.multiplexer_id()).second) {
        std::cerr << "multiplexer " << status.multiplexer_id() << ": tapping\n";
      }
    } else if (!refused.count(status.multiplexer_id())) {
      std::cerr << "multiplexer " << status.multiplexer_id() << ": not tapping; tapping again\n";
      _send(client, tap, incoming.second);
    }
  };
  _send(client, tap);
  while (!stop_requested) {
    Deadline timer(POLL_SECONDS);
    addresses.refresh();
    while (!stop_requested && !timer.expired()) {
      std::pair<std::shared_ptr<MultiplexerMessage>, ConnectionWrapper> incoming;
      try {
        incoming = client.receive_message(std::min(timer.remaining(), 0.5f));
      } catch (const Client::OperationTimedOut&) {
        continue;
      } catch (const Client::NotConnected&) {
        continue;
      }
      handle(incoming);
    }
    // What the queue holds goes out first: sending the status request reads
    // what waits in the sockets into the queue, which then has its whole
    // room for it.
    while (client.has_incoming_messages()) {
      std::pair<std::shared_ptr<MultiplexerMessage>, ConnectionWrapper> incoming;
      try {
        incoming = client.receive_message(0);
      } catch (const Client::OperationTimedOut&) {
        break;
      } catch (const Client::NotConnected&) {
        break;
      }
      handle(incoming);
    }
    out->flush();
    _send(client, status_request);
  }
  std::signal(SIGINT, SIG_DFL);
  std::signal(SIGTERM, SIG_DFL);
  std::streambuf* cout_buffer = std::cout.rdbuf();
  if (out == &std::cout) {
    std::cout.rdbuf(std::cerr.rdbuf());  // the statuses must not land among the records
  }
  RecordingControl untap;
  untap.set_action(RecordingControl::UNTAP);
  std::vector<Answer> answers;
  _control(client, untap, true, &answers);
  const bool untapped = every_tap_ended(heard, answers);
  std::cout.rdbuf(cout_buffer);
  out->flush();
  if (!written || !out->good()) {
    std::cerr << "cannot write the records to " << (out_.empty() ? "stdout" : out_) << "\n";
    written = false;
  }
  if (const std::uint64_t dropped = client.incoming_dropped()) {
    std::cerr << dropped << " messages dropped here, the incoming queue full, records among them\n";
    written = false;
  }
  if (!refused.empty()) {
    std::cerr << refused.size() << " multiplexer(s) refused the tap\n";
  }
  std::cerr << records << " records written\n";
  return untapped && written && refused.empty() ? 0 : 1;
}

}  // namespace mxcontrol
