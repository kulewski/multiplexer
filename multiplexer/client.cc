// Client: constructors, and the query algorithm with the send-and-receive
// it is built on. The design is in client.h.
#include "multiplexer/client.h"
#include "multiplexer/Multiplexer.pb.h"

using namespace multiplexer;
using std::cerr;

Client::Client(boost::uint32_t client_type)
    : io_service_ptr_(new boost::asio::io_service()), io_service_(*io_service_ptr_),
      basic_client_(BasicClient::Create(io_service_, client_type)) {}

Client::Client(boost::shared_ptr<boost::asio::io_service> io_service, boost::uint32_t client_type)
    : io_service_ptr_(io_service), io_service_(*io_service_ptr_),
      basic_client_(BasicClient::Create(io_service_, client_type)) {}

Client::Client(boost::asio::io_service &io_service, boost::uint32_t client_type)
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
      new shared_ptr<boost::asio::io_service>(io_service_ptr_); // leaked likewise
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
// The query algorithm; the Python Client.query in mxclient.py is the same
// steps and the two must stay in agreement.
IncomingMessage Client::_query(const MultiplexerMessage &query, float timeout) {

  std::unique_ptr<mx::SimpleTimer> timer;

  IncomingMessage result;

  try {
    timer = basic_client_->create_timer(timeout);
    result = _send_and_receive(query, *timer);
    if (result.third->type() != types::DELIVERY_ERROR)
      return result;
  } catch (OperationTimedOut &) {
  }

  // No reply, or a delivery error: ask every multiplexer who has a backend
  // of this type. The search is routed by the request type's own rule with
  // whom forced to ALL, so every live backend answers with a PING.
  BackendForPacketSearch search;
  search.set_packet_type(query.type());
  MX_CREATE_MESSAGE(MultiplexerMessage, mxmsg,
                    (set_from(instance_id()))(set_id(random64()))(set_type(types::BACKEND_FOR_PACKET_SEARCH)));
  search.SerializeToString(mxmsg.mutable_message());

  timer = basic_client_->create_timer(timeout);
  result = _send_and_receive(mxmsg, *timer, true, true, query.id(), types::REQUEST_RECEIVED);

  if (result.third->type() == types::DELIVERY_ERROR) {
    // Every multiplexer reported no backend of this type, so the one that
    // took the request is gone too: nothing can answer any more.
    MXTHROW(OperationFailed());
  }
  if (result.third->references() == query.id())
    return result;

  if (result.third->type() != types::PING)
    MXTHROW(OperationFailed());

  // Repeat the request to the backend that answered first, by instance id
  // and through the connection its PING came on. A late reply to the
  // original request is accepted too (accept_id); a late PING from another
  // backend is ignored (ignore_id).
  MX_CREATE_MESSAGE(MultiplexerMessage, direct_query,
                    (set_from(instance_id()))(set_id(random64()))(set_to(result.third->from()))(set_type(query.type()))(
                        set_message(query.message())));

  timer = basic_client_->create_timer(timeout);
  result = _send_and_receive(direct_query, *timer, false, false, query.id(), types::REQUEST_RECEIVED, mxmsg.id(),
                             result.second);
  if (result.third->type() == types::DELIVERY_ERROR)
    MXTHROW(OperationFailed());
  return result;
}

// Sends once and waits for a message referencing it (or accept_id). With
// schedule_all and handle_delivery_errors, one DELIVERY_ERROR per
// connection is expected before giving up: that is how "every multiplexer
// said no" is detected. Messages of ignore_type (REQUEST_RECEIVED) are
// skipped; others that reference ignore_id are skipped silently, the rest
// are logged and dropped.
IncomingMessage Client::_send_and_receive(const MultiplexerMessage &mxmsg, mx::SimpleTimer &timer, bool schedule_all,
                                          bool handle_delivery_errors, boost::uint64_t accept_id,
                                          boost::uint32_t ignore_type, boost::uint64_t ignore_id,
                                          ConnectionWrapper connection) {

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
  return _send_and_receive_one(mxmsg, timer, accept_ids, ignore_type, ignore_id, connection);
}

// Sends `mxmsg` through one connection and waits for a reply. A
// synchronous client notices a dead connection only while a call runs the
// loop, which _send_one does before choosing, so this is where a
// multiplexer restart between calls is absorbed:
// if the connection used dies before the reply arrives, or no connection
// is live to begin with, the loop keeps running so the reconnect timers
// fire, and the message is sent again with a fresh id through whatever
// connection is available, until the deadline. `connection` is the one to
// prefer (a reply's origin); any other is used if it is gone.
IncomingMessage Client::_send_and_receive_one(MultiplexerMessage mxmsg, mx::SimpleTimer &timer,
                                              std::vector<uint64_t> accept_ids, boost::uint32_t ignore_type,
                                              boost::uint64_t ignore_id, ConnectionWrapper connection) {
  for (;;) {
    ConnectionWrapper used = _send_one(mxmsg, timer, connection);
    bool lost = false;
    IncomingMessage result = _receive(timer, accept_ids, ignore_type, ignore_id, &used, &lost);
    if (!lost)
      return result;
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
ConnectionWrapper Client::_send_one(const MultiplexerMessage &mxmsg, mx::SimpleTimer &timer,
                                    ConnectionWrapper preferred) {
  shared_ptr<const RawMessage> raw = _serialize(mxmsg);
  basic_client_->poll(); // retire what the multiplexers closed while we were idle
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
    if (!tracker)
      tracker = basic_client_->schedule_one(raw, &used);
    if (tracker) {
      flush(ScheduledMessageTracker(tracker), timer);
      if (ScheduledMessageTracker(tracker).is_sent())
        return used;
      // The connection died under the write; the reconnect is scheduled.
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
