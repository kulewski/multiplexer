// Client: constructors, a leaving server's refusals, shutdown(), and the
// one way a call sends a message and waits for its write (_send_one). The
// query algorithm is in client_query.cc. The design is in client.h.
#include "multiplexer/client.h"

#include <chrono>
#include <limits>

#include "lib/seconds.h"
#include "multiplexer/Multiplexer.pb.h"
#include "multiplexer/multiplexer.constants.h"

using namespace multiplexer;

namespace {

// Refuses `incoming` for `client`, a server that is leaving (see
// Client::refuse_arrivals): DELIVERY_ERROR for a request whose sender wants
// delivery errors, queued as a reply is; nothing for a reply or for the
// protocol's own messages. Queued, never written here: this runs inside
// the client's loop. A message of a confirmed batch is confirmed REFUSED
// instead, and its producer sends it again at once.
void refuse(BasicClient& client, const IncomingMessage& incoming) {
  const MultiplexerMessage& msg = *incoming.third;
  if (msg.has_confirmation()) {
    client.confirm(msg, Confirmed::REFUSED);
    return;
  }
  if (msg.references()) {
    MX_LOG(DEBUG, LOWVERBOSITY,
           CTX("SyncClient") TEXT("reply #" + repr(msg.id()) + " of type " + repr(msg.type()) + " dropped: leaving"));
    return;
  }
  MX_LOG(DEBUG, LOWVERBOSITY,
         CTX("SyncClient") TEXT("request #" + repr(msg.id()) + " of type " + repr(msg.type()) + " refused: leaving"));
  // A rule reports delivery errors unless told not to, and so does this; a
  // sender that set the message's own flag to false hears nothing.
  const bool wanted = !msg.has_report_delivery_error() || msg.report_delivery_error();
  if (!wanted || msg.type() <= types::MAX_MULTIPLEXER_META_PACKET) {
    return;
  }
  DeliveryError error;
  error.set_packet_id(msg.id());
  error.add_failed_type(client.client_type());
  MultiplexerMessage refusal;
  refusal.set_id(client.random64());
  refusal.set_sender(client.instance_id());
  refusal.set_type(types::DELIVERY_ERROR);
  refusal.set_to(msg.sender());
  refusal.set_references(msg.id());
  refusal.set_workflow(msg.workflow());
  error.SerializeToString(refusal.mutable_message());
  client.send(std::shared_ptr<const RawMessage>(RawMessage::FromMessage(refusal)), /*all=*/false,
              std::make_shared<Lane>(incoming.second), DEFAULT_TIMEOUT);
}

}  // namespace

void Client::refuse_arrivals() {
  basic_client_->check_not_orphaned();
  BasicClient* client = basic_client_.get();  // the sink lives in it, so it outlives every call
  if (!awaited_) {
    awaited_ = std::make_shared<std::vector<std::uint64_t>>();
  }
  basic_client_->set_incoming_sink([client, awaited = awaited_](const IncomingMessage& incoming) {
    const std::uint64_t references = incoming.third->references();
    if (references && contains(*awaited, references)) {
      client->queue_incoming(incoming);  // an answer to the query under way, which reads the queue
      return;
    }
    refuse(*client, incoming);
  });
}

void Client::refuse_unread() {
  basic_client_->check_not_orphaned();
  while (basic_client_->has_incoming_messages()) {
    refuse(*basic_client_, basic_client_->next_incoming_message());
  }
}

Client::Client(std::uint32_t client_type)
    : io_service_ptr_(new asio::io_service()),
      io_service_(*io_service_ptr_),
      basic_client_(BasicClient::Create(io_service_, client_type)) {}

Client::Client(std::shared_ptr<asio::io_service> io_service, std::uint32_t client_type)
    : io_service_ptr_(io_service),
      io_service_(*io_service_ptr_),
      basic_client_(BasicClient::Create(io_service_, client_type)) {}

Client::Client(asio::io_service& io_service, std::uint32_t client_type)
    : io_service_(io_service), basic_client_(BasicClient::Create(io_service_, client_type)) {}

Client::Client(Client&& other) noexcept
    : io_service_ptr_(std::move(other.io_service_ptr_)),
      received_(std::move(other.received_)),
      awaited_(std::move(other.awaited_)),
      io_service_(other.io_service_),
      basic_client_(std::move(other.basic_client_)) {}

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
  if (!basic_client_) {
    return;  // moved from
  }
  if (basic_client_->orphaned()) {
    basic_client_->orphan_close_descriptors();
    new shared_ptr<BasicClient>(basic_client_);  // leaked: keeps the object alive forever
    if (io_service_ptr_) {
      new shared_ptr<asio::io_service>(io_service_ptr_);  // leaked likewise
    }
    return;
  }
  basic_client_->bind_to_current_thread();
  shutdown();
}

void Client::shutdown(float timeout) {
  if (basic_client_->orphaned()) {
    basic_client_->orphan_close_descriptors();
    return;
  }
  if ((timeout > 0 || timeout < 0) && !basic_client_->shuts_down()) {
    // What was sent before is written first, and so is what the loop sent
    // meanwhile, a server's refusals of what it read: a flush again while
    // anything newer was sent, `timeout` seconds in all, as long as it
    // takes for a negative one, as mx::from_seconds reads it. 0 and NaN
    // write nothing more.
    const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + mx::from_seconds(timeout);
    for (;;) {
      const std::uint64_t sent = basic_client_->last_number();
      const float left = std::chrono::duration<float>(deadline - std::chrono::steady_clock::now()).count();
      if (left <= 0) {
        break;
      }
      flush_all(left);
      if (basic_client_->last_number() == sent) {
        break;
      }
    }
  }
  basic_client_->shutdown();
  // The connections close the polite way, reading what still arrives until
  // their multiplexers' end (BasicClient::shutdown): the loop runs until
  // they have, a little past CLOSE_READ_SECONDS at most.
  std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(CLOSE_READ_SECONDS + 0.5f);
  while (basic_client_->closing() && !timer->expired()) {
    basic_client_->run_one();
  }
  basic_client_->poll();         // what the close settled, the callbacks of sends included
  basic_client_->end_follows();  // and a send still followed hears 0: nothing more runs the loop
}

namespace multiplexer {
namespace {
// A lane that is not pinned, or a pinned one still empty, takes the
// connection a message went through or a reply came through.
void adopt(const LanePtr& lane, const ConnectionWrapper& connection) {
  if (lane) {
    lane->adopt(connection);
  }
}
}  // namespace

// Writes `mxmsg` to one connection and returns the one that wrote it,
// within the deadline: see send(). A connection given outright, where a
// search or a locating PING was answered, is preferred for this message
// over the lane's, unless the lane is pinned; the lane adopts the
// connection that wrote it.
ConnectionWrapper Client::_send_one(const MultiplexerMessage& mxmsg, mx::SimpleTimer& timer,
                                    ConnectionWrapper preferred, LanePtr lane,
                                    BasicClient::BasicScheduledMessageTracker* placed) {
  return _send_one(_serialize(mxmsg), timer, preferred, lane, placed);
}

ConnectionWrapper Client::_send_one(std::shared_ptr<const RawMessage> raw, mx::SimpleTimer& timer,
                                    ConnectionWrapper preferred, LanePtr lane,
                                    BasicClient::BasicScheduledMessageTracker* placed) {
  basic_client_->check_not_orphaned();
  if (lane && lane->closed()) {
    MXTHROW(NotConnected());
  }
  const LanePtr through = preferred && !(lane && lane->pinned()) ? std::make_shared<Lane>(preferred) : lane;
  ConnectionWrapper used;
  bool taken = false, lost = false;
  if (_send_and_wait(raw, false, through, timer, &used, &taken, &lost, placed)) {
    if (through != lane) {
      adopt(lane, used);
    }
    return used;
  }
  if (!taken) {
    MXTHROW(NotConnected());  // the client shut down, or the pinned lane's connection is gone
  }
  _raise_for_nothing_written(lane, lost);
}

unsigned int Client::_send_and_wait(std::shared_ptr<const RawMessage> raw, bool all, const LanePtr& lane,
                                    mx::SimpleTimer& timer, ConnectionWrapper* used, bool* taken, bool* lost,
                                    BasicClient::BasicScheduledMessageTracker* placed) {
  basic_client_->poll();  // retire what the multiplexers closed while we were idle
  // The message waits for room, or for a connection, as long as the call
  // has left and a moment more, so that the call's own deadline comes
  // first: a message still waiting then is the call timing out. A call
  // with no deadline gives its message none either.
  const float room = _room_for(timer.remaining());
  std::vector<BasicScheduledMessageTracker> copies;
  ConnectionWrapper first;
  *taken = basic_client_->send(raw, all, lane, room, 0, &copies, &first);
  *lost = false;
  if (!*taken) {
    return 0;
  }
  if (placed && !copies.empty()) {
    *placed = copies.front();
  }
  for (;;) {
    bool queued = false;
    for (const BasicScheduledMessageTracker& copy : copies) {
      if (*copy == SendState::SENT) {
        if (used) {
          *used = basic_client_->followed(copy, first);  // where a dead connection handed it, if one did
        }
        return 1;
      }
      queued = queued || *copy == SendState::QUEUED;
    }
    if (!queued) {
      *lost = true;
      return 0;
    }
    if (timer.expired()) {
      return 0;
    }
    if (!basic_client_->connection_live_or_coming()) {
      // No connection live or coming: nothing could write it, however long
      // the call waited, where a wait with no deadline spun. Given up on
      // now, as its message would be at the call's deadline.
      for (const BasicScheduledMessageTracker& copy : copies) {
        basic_client_->drop_held(copy);
      }
      *lost = true;
      return 0;
    }
    basic_client_->run_one();
  }
}

float Client::_room_for(float left) {
  return left < 0 ? std::numeric_limits<float>::infinity() : left + ROOM_GRACE_SECONDS;
}

void Client::_raise_for_nothing_written(const LanePtr& lane, bool lost) {
  if (lost || (lane && lane->closed()) || basic_client_->connections_count(true) == 0) {
    MXTHROW(NotConnected());
  }
  MXTHROW(OperationTimedOut());
}

}  // namespace multiplexer
