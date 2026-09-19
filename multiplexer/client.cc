// Client: constructors, and the query algorithm with the send-and-receive
// it is built on. The design is in client.h.
#include "multiplexer/client.h"
#include "multiplexer/Multiplexer.pb.h"

using namespace multiplexer;
using std::cerr;

Client::Client(std::uint32_t client_type)
    : io_service_ptr_(new asio::io_service()), io_service_(*io_service_ptr_),
      basic_client_(BasicClient::Create(io_service_, client_type)) {}

Client::Client(std::shared_ptr<asio::io_service> io_service, std::uint32_t client_type)
    : io_service_ptr_(io_service), io_service_(*io_service_ptr_),
      basic_client_(BasicClient::Create(io_service_, client_type)) {}

Client::Client(asio::io_service &io_service, std::uint32_t client_type)
    : io_service_(io_service), basic_client_(BasicClient::Create(io_service_, client_type)) {}

// The thread that destroys the client owns it from here on, whichever
// thread drove it: at interpreter exit Python destroys on the main thread a
// client that served on a worker, and the shutdown inside must not fail the
// thread check for that. A thread still driving the client at this point is
// a bug the check cannot catch anyway.
// An orphan (inherited across a fork) gets the orphan teardown instead, and
// its io_service and BasicClient are leaked on purpose: their destructors
// would take asio's locks, which may be held by a parent thread that does
// not exist here, and close descriptor numbers the child may have reused.
Client::~Client() {
  if (basic_client_->orphaned()) {
    basic_client_->orphan_close_descriptors();
    new shared_ptr<BasicClient>(basic_client_); // leaked: keeps the object alive forever
    if (io_service_ptr_)
      new shared_ptr<asio::io_service>(io_service_ptr_); // leaked likewise
    return;
  }
  basic_client_->bind_to_current_thread();
  shutdown();
}

void Client::shutdown() {
  if (basic_client_->orphaned()) {
    basic_client_->orphan_close_descriptors();
    return;
  }
  basic_client_->shutdown();
}

namespace multiplexer {
namespace {
// A lane that is not pinned, or a pinned one still empty, takes the
// connection a message went through or a reply came through.
void adopt(const LanePtr &lane, const ConnectionWrapper &connection) {
  if (lane)
    lane->adopt(connection);
}
} // namespace

// The query algorithm; the Python Client.query in mxclient.py is the same
// steps and the two must stay in agreement. A request with `to` set is an
// addressed query, with stages of its own below.
IncomingMessage Client::_query(const MultiplexerMessage &query, float timeout, LanePtr lane, Probe probe) {
  if (query.to())
    return _query_addressed(query, timeout, lane, probe);

  std::unique_ptr<mx::SimpleTimer> timer;

  IncomingMessage result;

  try {
    timer = basic_client_->create_timer(timeout);
    result = _send_and_receive(query, *timer, false, false, 0, 0, -1, ConnectionWrapper(), lane);
    if (result.third->type() != types::DELIVERY_ERROR) {
      adopt(lane, result.second);
      return result;
    }
  } catch (OperationTimedOut &) {
  }

  // No reply, or a delivery error: ask every multiplexer who has a backend
  // of this type. The search is routed by the request type's own rule with
  // whom forced to ALL, so every live backend answers with a PING. Through
  // a pinned lane the one multiplexer behind it is asked instead.
  MultiplexerMessage mxmsg = _probe_for(query, PROBE_SEARCH);

  timer = basic_client_->create_timer(timeout);
  const bool pinned = lane && lane->pinned();
  result = _send_and_receive(mxmsg, *timer, !pinned, !pinned, query.id(), types::REQUEST_RECEIVED, -1,
                             ConnectionWrapper(), lane);

  if (result.third->type() == types::DELIVERY_ERROR) {
    // Every multiplexer reported no backend of this type, so the one that
    // took the request is gone too: nothing can answer any more.
    MXTHROW(OperationFailed());
  }
  if (result.third->references() == query.id()) {
    adopt(lane, result.second);
    return result;
  }

  if (result.third->type() != types::PING)
    MXTHROW(OperationFailed());

  // Repeat the request to the backend that answered first, by instance id
  // and through the connection its PING came on. A late reply to the
  // original request is accepted too (accept_id); a late PING from another
  // backend is ignored (ignore_id).
  MultiplexerMessage direct_query;
  direct_query.set_from(instance_id());
  direct_query.set_id(random64());
  direct_query.set_to(result.third->from());
  direct_query.set_type(query.type());
  direct_query.set_message(query.message());

  timer = basic_client_->create_timer(timeout);
  result = _send_and_receive(direct_query, *timer, false, false, query.id(), types::REQUEST_RECEIVED, mxmsg.id(),
                             result.second, lane);
  if (result.third->type() == types::DELIVERY_ERROR)
    MXTHROW(OperationFailed());
  adopt(lane, result.second);
  return result;
}

// An addressed query, docs/query.md "An addressed query": the request with
// its `to`, through the lane's connection or any; when the multiplexer
// reports the instance is not behind it, or the connection dies under the
// wait, a probe addressed to the instance on every connection (through a
// pinned lane, on its one connection) finds the multiplexer that has it;
// then the request again through that connection. Only the addressee ever
// gets the request: an instance that left is OperationFailed, never
// another instance of its type. One `timeout` covers the three stages,
// and a request that gets no answer at all within it is OperationTimedOut
// without a probe, since a silent addressee is one the multiplexer still
// has, and a probe would find the same one.
IncomingMessage Client::_query_addressed(const MultiplexerMessage &query, float timeout, LanePtr lane, Probe probe) {
  std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
  MultiplexerMessage request = query;
  if (!request.id())
    request.set_id(random64());
  if (!request.from())
    request.set_from(instance_id());
  request.set_report_delivery_error(true); // "not behind this multiplexer" must come back as a message

  std::vector<uint64_t> accept_ids;
  accept_ids.push_back(request.id());
  IncomingMessage result =
      _send_and_receive_one(request, *timer, accept_ids, types::REQUEST_RECEIVED, -1, ConnectionWrapper(), lane);
  if (result.third->type() != types::DELIVERY_ERROR) {
    adopt(lane, result.second);
    return result;
  }

  // Locate: the probe, addressed to the instance, with delivery errors
  // requested, so that a multiplexer without the instance says so.
  MultiplexerMessage mxmsg = _probe_for(request, probe);
  const bool pinned = lane && lane->pinned();
  result = _send_and_receive(mxmsg, *timer, !pinned, !pinned, request.id(), types::REQUEST_RECEIVED, -1,
                             ConnectionWrapper(), lane);
  if (result.third->type() == types::DELIVERY_ERROR)
    MXTHROW(OperationFailed()); // no multiplexer has the instance
  if (result.third->references() == request.id()) {
    adopt(lane, result.second);
    return result; // a late reply to the request after all
  }
  if (result.third->type() != types::PING)
    MXTHROW(OperationFailed());

  // The request again, a fresh id, through the connection the answer came
  // on; the lane adopts it.
  MultiplexerMessage again = request;
  again.set_id(random64());
  result = _send_and_receive(again, *timer, false, false, request.id(), types::REQUEST_RECEIVED, mxmsg.id(),
                             result.second, lane);
  if (result.third->type() == types::DELIVERY_ERROR)
    MXTHROW(OperationFailed());
  adopt(lane, result.second);
  return result;
}

// The message that locates who can take `query`: a search for a backend of
// its type, addressed to its `to` when it has one, or a PING to that
// instance.
MultiplexerMessage Client::_probe_for(const MultiplexerMessage &query, Probe probe) {
  MultiplexerMessage mxmsg;
  mxmsg.set_id(random64());
  mxmsg.set_from(instance_id());
  if (probe == PROBE_PING) {
    mxmsg.set_type(types::PING);
  } else {
    BackendForPacketSearch search;
    search.set_packet_type(query.type());
    mxmsg.set_type(types::BACKEND_FOR_PACKET_SEARCH);
    search.SerializeToString(mxmsg.mutable_message());
  }
  if (query.to()) {
    mxmsg.set_to(query.to());
    mxmsg.set_report_delivery_error(true);
  }
  return mxmsg;
}

// Sends once and waits for a message referencing it (or accept_id). With
// schedule_all and handle_delivery_errors, one DELIVERY_ERROR per
// connection is expected before giving up: that is how "every multiplexer
// said no" is detected. Messages of ignore_type (REQUEST_RECEIVED) are
// skipped; others that reference ignore_id are skipped silently, the rest
// are logged and dropped.
IncomingMessage Client::_send_and_receive(const MultiplexerMessage &mxmsg, mx::SimpleTimer &timer, bool schedule_all,
                                          bool handle_delivery_errors, std::uint64_t accept_id,
                                          std::uint32_t ignore_type, std::uint64_t ignore_id,
                                          ConnectionWrapper connection, LanePtr lane) {

  IncomingMessage result;

  std::vector<uint64_t> accept_ids;
  accept_ids.push_back(mxmsg.id());
  if (accept_id)
    accept_ids.push_back(accept_id);

  if (schedule_all) {
    int sends = this->schedule_all(mxmsg);
    if (sends == 0 && !basic_client_->wait_for_any_connection(timer))
      MXTHROW(NotConnected());
    if (sends == 0)
      sends = this->schedule_all(mxmsg);
    while (!timer.expired()) {
      result = _receive(timer, accept_ids, ignore_type, ignore_id);
      if (result.third->type() == types::DELIVERY_ERROR && handle_delivery_errors) {
        if ((--sends) > 0)
          continue;
      }
      return result;
    }
    MXTHROW(OperationTimedOut());
  }
  return _send_and_receive_one(mxmsg, timer, accept_ids, ignore_type, ignore_id, connection, lane);
}

// Sends `mxmsg` through one connection and waits for a reply. A
// synchronous client notices a dead connection only while a call runs the
// loop, which _send_one does before choosing, so this is where a
// multiplexer restart between calls is absorbed:
// if the connection used dies before the reply arrives, or no connection
// is live to begin with, the loop keeps running so the reconnect timers
// fire, and the message is sent again with a fresh id through whatever
// connection is available, until the deadline. `connection` is the one to
// prefer (a reply's origin); any other is used if it is gone. Through a
// pinned lane there is no other: the loss is NotConnected.
IncomingMessage Client::_send_and_receive_one(MultiplexerMessage mxmsg, mx::SimpleTimer &timer,
                                              std::vector<uint64_t> accept_ids, std::uint32_t ignore_type,
                                              std::uint64_t ignore_id, ConnectionWrapper connection, LanePtr lane) {
  for (;;) {
    ConnectionWrapper used = _send_one(mxmsg, timer, connection, lane);
    bool lost = false;
    IncomingMessage result = _receive(timer, accept_ids, ignore_type, ignore_id, &used, &lost);
    if (!lost)
      return result;
    if (lane && lane->pinned())
      MXTHROW(NotConnected());
    MX_LOG(WARNING, MEDIUMVERBOSITY,
           CTX("multiplexer.client")
               TEXT("connection lost while waiting for a reply to " + repr(mxmsg.id()) + "; sending again"));
    mxmsg.set_id(random64());
    accept_ids.push_back(mxmsg.id());
    connection = ConnectionWrapper();
  }
}

// Writes `mxmsg` to one connection and returns it; waits for a
// connection, or for the message to actually be written, up to the
// deadline. Throws NotConnected when no connection exists by the deadline.
// With a lane, the lane's connection is the one preferred, and the lane
// adopts the connection used; a pinned lane allows no other, so its
// connection being gone, or dying under the write, is NotConnected.
ConnectionWrapper Client::_send_one(const MultiplexerMessage &mxmsg, mx::SimpleTimer &timer,
                                    ConnectionWrapper preferred, LanePtr lane) {
  shared_ptr<const RawMessage> raw = _serialize(mxmsg);
  basic_client_->poll(); // retire what the multiplexers closed while we were idle
  if (lane) {
    if (lane->closed())
      MXTHROW(NotConnected());
    if (lane->pinned())
      raw->mark_pinned(); // never handed to another connection if this one dies under it
    if (lane->holds_connection() && !preferred)
      preferred = lane->connection(); // a connection given outright wins: where a probe was answered
  }
  const bool only_preferred = lane && lane->pinned() && lane->holds_connection();
  for (;;) {
    ConnectionWrapper used;
    BasicScheduledMessageTracker tracker;
    if (preferred) {
      if (BasicClient::Connection::pointer conn = preferred.lock())
        if (conn->living()) {
          tracker = conn->schedule(raw);
          used = preferred;
        }
      preferred = ConnectionWrapper(); // one try; any connection after that
    }
    if (!tracker && only_preferred)
      MXTHROW(NotConnected());
    if (!tracker)
      tracker = basic_client_->schedule_one(raw, &used);
    if (tracker) {
      flush(ScheduledMessageTracker(tracker), timer);
      if (ScheduledMessageTracker(tracker).is_sent()) {
        adopt(lane, used);
        return used;
      }
      // The connection died under the write; the reconnect is scheduled.
      if (only_preferred)
        MXTHROW(NotConnected());
    }
    if (timer.expired())
      MXTHROW(OperationTimedOut());
    if (!basic_client_->wait_for_any_connection(timer))
      MXTHROW(NotConnected());
  }
}

// Waits for a message referencing one of accept_ids. With `watch`, also
// stops when that connection dies, setting *lost instead of returning a
// message.
IncomingMessage Client::_receive(mx::SimpleTimer &timer, const std::vector<uint64_t> &accept_ids, uint32_t ignore_type,
                                 uint64_t ignore_id, const ConnectionWrapper *watch, bool *lost) {

  IncomingMessage result;

  while (!timer.expired()) {
    if (watch) {
      if (!basic_client_->wait_for_incoming_message_or_loss(timer, *watch)) {
        *lost = true;
        return result;
      }
      result = basic_client_->next_incoming_message();
    } else {
      result = read_raw_message(timer);
    }
    const MultiplexerMessage &mxmsg = *result.third;

    if (mxmsg.type() == ignore_type)
      continue;
    if (contains(accept_ids, mxmsg.references()))
      return result;
    if (ignore_id != mxmsg.references()) {
      MX_LOG(WARNING, HIGHVERBOSITY,
             TEXT("message (id=" + repr(mxmsg.id()) + ", type=" + repr(mxmsg.type()) + ", from=" + repr(mxmsg.from()) +
                  ", references=" + repr(mxmsg.references()) +
                  ") while waiting "
                  "for reply for " +
                  repr(accept_ids)));
    }
  }

  MXTHROW(OperationTimedOut());
}

} // namespace multiplexer
