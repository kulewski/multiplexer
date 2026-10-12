// ThreadedClient's queries: the state machine every query runs on the io
// thread, from the request through the search to a late reply, and the
// calls that start one. Part of ThreadedClient; see threaded_client.cc.
#include <algorithm>
#include <chrono>
#include <cmath>
#include <future>
#include <limits>
#include <stdexcept>

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "lib/seconds.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client_core.h"

namespace multiplexer {

using mx::repr;

// One query in flight. Owned by the io thread; the shared_ptr keeps it alive
// for the timer callback.
struct ThreadedClient::Core::InFlight {
  // WAITING: no connection was live when a stage's message had to go out;
  // it goes out as soon as one registers, the stage's deadline still
  // running, and `resume` says which: the request, the search or the
  // direct request. SEARCH is the locate phase of an addressed query too:
  // the PING addressed to the instance out, the delivery errors counted the
  // same way. LATE: the query can only wait, until the stage's deadline,
  // for a late reply to an attempt a backend may have (`pending`): the
  // search found nobody, the direct request drew a delivery error, or its
  // connection was lost. A query sends its request at most twice, the
  // request and the direct request, and never goes back a stage.
  enum Stage { REQUEST, SEARCH, DIRECT, WAITING, LATE } stage = REQUEST;
  Stage resume = REQUEST;       // WAITING: the stage whose message goes out when a connection comes up
  std::uint64_t direct_to = 0;  // the instance whose PING answered the search: the direct request's `to`
  ConnectionWrapper sent_via;   // REQUEST and DIRECT: the connection used
  // REQUEST and DIRECT: the attempt's tracker, which says whether it was
  // written when sent_via died: one still queued is handed to another
  // connection by the outbox, or held for the next, and is not lost.
  BasicClient::BasicScheduledMessageTracker sent_state;
  // The request's tracker and the direct request's, kept for the late
  // wait: an attempt the client gave up on, reading LOST, went nowhere.
  BasicClient::BasicScheduledMessageTracker request_state, direct_state;
  MultiplexerMessage prototype;  // the request; id and sender set per attempt
  float timeout = 0;
  // An addressed query's one deadline across its stages; a typed query
  // arms each stage with `timeout`.
  std::chrono::steady_clock::time_point deadline;
  LanePtr lane;  // held until the query ends, then released
  // The request's and the direct request's ids, its two attempts, each
  // kept when its message waits for a connection, nothing having gone out
  // under it, and the current search's.
  std::uint64_t request_id = 0, search_id = 0, direct_id = 0;
  // Whether a backend may have the request and answer it late: set when
  // the request's stage runs out, or its connection is lost, and cleared
  // by a delivery error for the request, nobody taking it, at any stage.
  bool taken = false;
  // LATE: the attempts a backend may still have, whose reply the query
  // waits for; a delivery error for one strikes it off, and once none is
  // left nothing can answer any more.
  std::vector<std::uint64_t> pending;
  // During SEARCH: the connections the search went through that have not
  // answered it yet, with a delivery error or by going down. Only these
  // count: another connection ending, a failed connect for instance, says
  // nothing about the search.
  std::vector<ConnectionWrapper> searched;
  std::vector<std::uint64_t> search_ids;  // every search the query made, the current one last
  unsigned int generation = 0;            // bumped per stage so stale deadlines are ignored
  bool listed = false;                    // in waiting_queries_
  bool started = false;                   // in in_flight_, at `place`
  std::list<InFlightPtr>::iterator place;
  Callback callback;
  ReceivedCallback received;  // told of each REQUEST_RECEIVED for an attempt, until the query ends
  std::unique_ptr<asio::steady_timer> timer;
  bool addressed() const { return prototype.to() != 0; }
  bool pinned() const { return lane && lane->pinned(); }
};

// Hands `raw` to BasicClient through `lane`: on the lane's connection while
// it is live, waiting for its room when it is full, else on any live
// connection, which the lane adopts; with no lane, on any. Null when no
// connection is live; *refused is set when a pinned lane's connection is
// gone, so the caller stops trying.
BasicClient::BasicScheduledMessageTracker ThreadedClient::Core::_schedule(const std::shared_ptr<const RawMessage>& raw,
                                                                          const LanePtr& lane, ConnectionWrapper* used,
                                                                          bool* refused, float timeout,
                                                                          std::uint64_t number) {
  if (lane && lane->pinned()) {
    raw->mark_pinned();  // never handed to another connection if this one dies under it
  }
  if (lane && lane->holds_connection()) {
    ConnectionWrapper held = lane->connection();
    if (Tracker tracker = basic_client_->schedule_on(raw, held, timeout, number, lane)) {
      *used = held;
      return tracker;
    }
    if (lane->pinned()) {
      *refused = true;  // its connection is gone
      return Tracker();
    }
  }
  Tracker tracker = basic_client_->schedule_one(raw, used, timeout, number, lane);
  if (tracker && lane) {
    lane->adopt(*used);
  }
  return tracker;
}

void ThreadedClient::Core::query(const std::string& payload, std::uint32_t type, Callback callback, float timeout,
                                 LanePtr lane, ReceivedCallback received) {
  MultiplexerMessage msg;
  msg.set_type(type);
  msg.set_message(payload);
  query(msg, callback, timeout, lane, received);
}

ThreadedClient::Result ThreadedClient::Core::query(const std::string& payload, std::uint32_t type, float timeout,
                                                   LanePtr lane, ReceivedCallback received) {
  MultiplexerMessage msg;
  msg.set_type(type);
  msg.set_message(payload);
  return query(msg, timeout, lane, received);
}

void ThreadedClient::Core::query(const MultiplexerMessage& msg, Callback callback, float timeout, LanePtr lane,
                                 ReceivedCallback received) {
  basic_client_->check_not_orphaned();
  if (!callback) {
    // A caller who wants the request delivered, not its reply: the query
    // runs as any other, its stages and its deadline, and its outcome goes
    // nowhere. Left empty it would read as over from the start, an empty
    // callback being what marks a query's end.
    callback = [](const Result&) {};
  }
  if (lane) {
    lane->check_not_inherited();
    basic_client_->check_ours(lane);  // here, not on the io thread
  }
  InFlightPtr in_flight(new InFlight());
  in_flight->prototype = msg;  // the request itself, an empty sender filled in; each attempt gets an id of its own
  if (!msg.sender()) {
    in_flight->prototype.set_sender(instance_id_);
  }
  if (msg.to()) {
    in_flight->prototype.set_report_delivery_error(true);  // "not behind this multiplexer" must come back
  }
  // Here, on the caller's thread, the request as the io thread frames it:
  // each attempt only gets its id, and a typed query's request sent again
  // (_direct) the `to` of the backend its search found and a delivery
  // error asked for, all measured at their largest.
  in_flight->prototype.set_id(std::numeric_limits<std::uint64_t>::max());
  const bool typed = !msg.to();
  const bool asked = in_flight->prototype.has_report_delivery_error();
  const bool asked_value = in_flight->prototype.report_delivery_error();
  if (typed) {
    in_flight->prototype.set_to(std::numeric_limits<std::uint64_t>::max());
    in_flight->prototype.set_report_delivery_error(true);
  }
  check_message_size(in_flight->prototype.ByteSizeLong());
  if (typed) {
    in_flight->prototype.clear_to();
    if (asked) {
      in_flight->prototype.set_report_delivery_error(asked_value);
    } else {
      in_flight->prototype.clear_report_delivery_error();
    }
  }
  in_flight->timeout = timeout;
  in_flight->deadline = std::chrono::steady_clock::now() + mx::from_seconds(timeout);
  in_flight->lane = std::move(lane);
  in_flight->callback = std::move(callback);
  in_flight->received = std::move(received);
  in_flight->timer.reset(new asio::steady_timer(io_service_));
  const bool posted = _post_unless_stopped([this, in_flight] {
    MX_DCHECK_RUN_ON(&io_thread_);
    in_flight->place = in_flight_.insert(in_flight_.end(), in_flight);
    in_flight->started = true;
    _start_query(in_flight, /*keep_deadline=*/false);
  });
  if (!posted) {
    Result result;
    result.outcome = SHUT_DOWN;
    in_flight->callback(result);
  }
}

ThreadedClient::Result ThreadedClient::Core::query(const MultiplexerMessage& msg, float timeout, LanePtr lane,
                                                   ReceivedCallback received) {
  basic_client_->check_not_orphaned();
  if (io_thread_.is_current()) {
    throw std::logic_error(
        "blocking ThreadedClient::query() called on the io thread, from a callback; "
        "use the callback form there");
  }
  std::shared_ptr<std::promise<Result>> promise(new std::promise<Result>());
  std::future<Result> future = promise->get_future();
  query(msg, [promise](const Result& result) { promise->set_value(result); }, timeout, lane, received);
  return future.get();
}

void ThreadedClient::Core::query(const MultiplexerMessage& msg, const ConnectionWrapper& connection, Callback callback,
                                 float timeout, ReceivedCallback received) {
  query(msg, callback, timeout, std::make_shared<Lane>(connection), received);
}

ThreadedClient::Result ThreadedClient::Core::query(const MultiplexerMessage& msg, const ConnectionWrapper& connection,
                                                   float timeout, ReceivedCallback received) {
  return query(msg, timeout, std::make_shared<Lane>(connection), received);
}

// A finished query's `id`, kept `until` a late reply to it can no longer
// be expected; the ids whose time is up go first, from the front, where the
// oldest ended, and the oldest whatever their time when there are too many.
void ThreadedClient::Core::_remember_finished(std::uint64_t id, std::chrono::steady_clock::time_point until) {
  const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  while (!finished_order_.empty() &&
         (finished_order_.front().second <= now || finished_order_.size() >= FINISHED_IDS_KEPT)) {
    finished_ids_.erase(finished_order_.front().first);
    finished_order_.pop_front();
  }
  if (!id || !finished_ids_.insert(id).second) {
    return;
  }
  finished_order_.emplace_back(id, until);
}

void ThreadedClient::Core::_track(InFlightPtr in_flight, std::uint64_t id) { by_id_[id] = in_flight; }

// A connection came up or went, for the queries. Up, the queries that wait
// for a connection go now (_post_fill). Down, a query whose request or
// direct request went out through it moves on (_lost), and one whose
// attempt still waited there unwritten follows it to where the outbox
// hands it (_follow_handover); the deadline keeps running throughout.
void ThreadedClient::Core::_queries_on_connection(const ConnectionWrapper& connection, bool up) {
  if (up) {
    _post_fill();
    return;
  }
  std::vector<InFlightPtr> queries(in_flight_.begin(), in_flight_.end());  // a copy: the loop may finish queries
  for (auto& in_flight : queries) {
    switch (in_flight->stage) {
      case InFlight::REQUEST:
      case InFlight::DIRECT:
        if (in_flight->sent_via.is_same_connection(connection)) {
          if (!in_flight->pinned() && in_flight->sent_state && *in_flight->sent_state == SendState::QUEUED) {
            _follow_handover(in_flight);  // not written yet: it goes on, not lost
          } else {
            _lost(in_flight);
          }
        }
        break;
      case InFlight::SEARCH:
        // The search may have been routed before the connection went, and a
        // PING may still come back another way: the search waits on, a loss
        // counting as no answer. Through a pinned lane there is no other
        // way, and the lane's contract is NotConnected.
        if (in_flight->pinned() && _answered(in_flight, connection)) {
          _finish(in_flight, NOT_CONNECTED, NULL);
        }
        break;
      case InFlight::LATE:
        // A late reply may come back another way, but through a pinned
        // lane only the lane's: its connection gone, nothing can come, and
        // the lane's contract is NotConnected.
        if (in_flight->pinned() && in_flight->lane->connection().is_same_connection(connection)) {
          _finish(in_flight, NOT_CONNECTED, NULL);
        }
        break;
      case InFlight::WAITING:
        break;
    }
  }
}

// One _fill() on its way, however many events asked for it.
void ThreadedClient::Core::_post_fill() {
  if (fill_posted_) {
    return;
  }
  fill_posted_ = true;
  _post([this] {
    MX_DCHECK_RUN_ON(&io_thread_);
    fill_posted_ = false;
    if (!shut_down_) {
      _fill();
    }
  });
}

// A connection came up: the queries that waited for one start again. What
// was sent meanwhile BasicClient placed itself.
void ThreadedClient::Core::_fill() {
  if (!waiting_queries_.empty()) {
    _restart_waiting_queries();
  }
}

// The connection the query's attempt was queued on is going while the
// attempt waits there unwritten: the outbox hands it to another connection
// once the connection is down, or holds it for the next, as it does every
// client's unwritten message, so the attempt goes out once and is not
// lost. Once that is done the query follows it to the connection that took
// it, whose loss then counts; an attempt the outbox gave up on went
// nowhere, as one a delivery error answered. One found on no live
// connection is lost under the query: the frame being written as the
// connection went, set aside and so still queued when the loss was
// reported, may have been written whole; and one held for the next
// connection, or handed to one that went too, has no loss left to tell
// the query.
void ThreadedClient::Core::_follow_handover(InFlightPtr in_flight) {
  const unsigned int generation = in_flight->generation;
  const InFlight::Stage stage = in_flight->stage;
  io_service_.post([this, in_flight, generation, stage] {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (!in_flight->callback || in_flight->generation != generation || in_flight->stage != stage) {
      return;  // over, or moved on meanwhile
    }
    if (*in_flight->sent_state == SendState::LOST) {
      if (in_flight->stage == InFlight::REQUEST) {
        _search(in_flight, /*keep_deadline=*/false);  // nobody took it: on to the search, no late reply
      } else {
        _wait_late(in_flight, /*direct=*/false);  // the direct request went nowhere
      }
      return;
    }
    const ConnectionWrapper where = basic_client_->followed(in_flight->sent_state, in_flight->sent_via);
    if (!basic_client_->live(where)) {
      _lost(in_flight);
      return;
    }
    in_flight->sent_via = where;
  });
}

// The connection a request or a direct request went through is gone. The
// attempt may have reached a backend first, routed before the connection
// went, and its reply may still come back another way, so it is accepted
// to the query's end. The request goes out at most twice and a query
// never goes back a stage: from the request it moves on to the search, or
// for an addressed query to the locate, at once; from the direct request
// it waits for a late reply until the stage's deadline. Through a pinned
// lane there is nothing else to try.
void ThreadedClient::Core::_lost(InFlightPtr in_flight) {
  if (in_flight->pinned()) {
    _finish(in_flight, NOT_CONNECTED, NULL);
    return;
  }
  const bool request = in_flight->stage == InFlight::REQUEST;
  MX_LOG(WARNING, MEDIUMVERBOSITY,
         CTX("ThreadedClient") TEXT("connection lost under query " + repr(in_flight->request_id) +
                                    (!request                 ? "; waiting for a late reply"
                                     : in_flight->addressed() ? "; locating the addressee"
                                                              : "; searching for a backend")));
  if (request) {
    in_flight->taken = true;
    _search(in_flight, /*keep_deadline=*/false);
  } else {
    _wait_late(in_flight, /*direct=*/true);
  }
}

// The message of the query's stage `resume` found no connection live: it
// waits, and goes out when one comes up, within the stage's deadline.
void ThreadedClient::Core::_wait_for_connection(const InFlightPtr& in_flight, int resume) {
  in_flight->stage = InFlight::WAITING;
  in_flight->resume = static_cast<InFlight::Stage>(resume);
  if (!in_flight->listed) {
    in_flight->listed = true;
    waiting_queries_.push_back(in_flight);
  }
}

// A connection came up: the waiting queries send the message of the stage
// they wait in, in order; one that finds none live after all waits on.
void ThreadedClient::Core::_restart_waiting_queries() {
  std::deque<InFlightPtr> pass;
  pass.swap(waiting_queries_);
  waiting_ended_ = 0;
  for (const InFlightPtr& in_flight : pass) {
    in_flight->listed = false;
    if (in_flight->stage != InFlight::WAITING || !in_flight->callback) {
      continue;  // moved on, or finished
    }
    ++retries_;
    switch (in_flight->resume) {
      case InFlight::SEARCH:
        _search(in_flight, /*keep_deadline=*/true);
        break;
      case InFlight::DIRECT:
        _send_direct(in_flight, ConnectionWrapper(), /*keep_deadline=*/true);
        break;
      default:
        _start_query(in_flight, /*keep_deadline=*/true);
        break;
    }
  }
}

// What the next stage's timer gets: the whole `timeout` for a typed query,
// what is left of the one deadline for an addressed one.
float ThreadedClient::Core::_stage_timeout(const InFlightPtr& in_flight) const {
  if (!in_flight->addressed()) {
    return in_flight->timeout;
  }
  std::chrono::steady_clock::duration left = in_flight->deadline - std::chrono::steady_clock::now();
  return std::max(0.0f, std::chrono::duration<float>(left).count());
}

void ThreadedClient::Core::_start_query(InFlightPtr in_flight, bool keep_deadline) {
  if (shut_down_) {
    _finish(in_flight, SHUT_DOWN, NULL);
    return;
  }
  if (!in_flight->request_id) {
    in_flight->request_id = random64();  // kept by a request that waits for a connection
  }
  MultiplexerMessage request = in_flight->prototype;
  request.set_id(in_flight->request_id);
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(request));
  ConnectionWrapper used;
  bool refused = false;
  Tracker tracker = _schedule(raw, in_flight->lane, &used, &refused, _stage_timeout(in_flight));
  if (!tracker) {
    if (refused) {
      _finish(in_flight, NOT_CONNECTED, NULL);  // the pinned lane's connection is gone
      return;
    }
    // Nothing to send through: wait for a connection, within the deadline.
    _wait_for_connection(in_flight, InFlight::REQUEST);
    if (!keep_deadline) {
      _arm(in_flight, in_flight->timeout);
    }
    return;
  }
  in_flight->sent_via = used;
  in_flight->sent_state = tracker;
  in_flight->request_state = tracker;
  MX_LOG(DEBUG, CHATTERBOX,  // per message: off by default, MX_LOG_VERBOSITY=DEBUG:CHATTERBOX shows the traffic
         CTX("ThreadedClient") TEXT("request " + repr(in_flight->request_id) + " via " + repr(used.endpoint_)));
  _track(in_flight, in_flight->request_id);
  in_flight->stage = InFlight::REQUEST;
  if (!keep_deadline) {
    _arm(in_flight, in_flight->timeout);
  }
}

// A backend acknowledged an attempt of `in_flight` (notify_start()): its
// on_received, if any, is told which, and the query goes on as before. A
// search is no attempt: nothing acknowledges one.
void ThreadedClient::Core::_acknowledged(const InFlightPtr& in_flight, const MultiplexerMessage& msg) {
  const std::vector<std::uint64_t>& searches = in_flight->search_ids;
  if (!in_flight->received || std::find(searches.begin(), searches.end(), msg.references()) != searches.end()) {
    return;
  }
  _guarded([&] { in_flight->received(msg.sender()); },
           [&] { return "the on_received of a query of type " + repr(in_flight->prototype.type()); });
}

void ThreadedClient::Core::_advance(InFlightPtr in_flight, const IncomingMessage& incoming) {
  const MultiplexerMessage& msg = *incoming.third;
  // What the message answers: one of the query's searches, whose answers
  // are PINGs and delivery errors, or one of the request's two attempts
  // (the request and the direct request), whose answer is the reply,
  // accepted in any stage, or a delivery error, which strikes the attempt
  // off in any stage (_refused). A search's answer is never the reply:
  // arriving after the query moved on, it is dropped.
  const std::vector<std::uint64_t>& searches = in_flight->search_ids;
  const bool to_a_search = std::find(searches.begin(), searches.end(), msg.references()) != searches.end();
  const bool delivery_error = msg.type() == types::DELIVERY_ERROR;
  if (!to_a_search) {
    if (delivery_error) {
      _refused(in_flight, msg.references());
    } else {
      _finish(in_flight, REPLIED, &incoming);
    }
    return;
  }
  if (in_flight->stage != InFlight::SEARCH || msg.references() != in_flight->search_id) {
    return;  // a search's answer after the query moved on: a later PING from another backend
  }
  if (delivery_error) {
    // Every multiplexer the search went through reported no backend of
    // the type: see _nobody.
    _answered(in_flight, incoming.second);
    if (in_flight->searched.empty()) {
      _nobody(in_flight);
    }
  } else if (msg.type() == types::PING) {
    _direct(in_flight, incoming);
  }
}

// A delivery error for one of the query's attempts: nobody has it, and it
// is struck off. The request's moves the query from its first stage on to
// the search, and leaves a search that finds nobody no late reply to wait
// for; the direct request's, in its stage, leaves only a late reply to the
// request, if a backend may have that; in the late wait, the last attempt
// struck off leaves nothing that can answer.
void ThreadedClient::Core::_refused(InFlightPtr in_flight, std::uint64_t id) {
  if (id == in_flight->request_id) {
    in_flight->taken = false;
  }
  switch (in_flight->stage) {
    case InFlight::REQUEST:
      if (id == in_flight->request_id) {
        _search(in_flight, /*keep_deadline=*/false);  // nobody took the request
      }
      return;
    case InFlight::DIRECT:
      if (id == in_flight->direct_id) {
        _wait_late(in_flight, /*direct=*/false);  // the instance that answered the search is gone
      }
      return;
    case InFlight::LATE: {
      std::vector<std::uint64_t>& pending = in_flight->pending;
      pending.erase(std::remove(pending.begin(), pending.end(), id), pending.end());
      if (pending.empty()) {
        _finish(in_flight, FAILED, NULL);
      }
      return;
    }
    case InFlight::SEARCH:
    case InFlight::WAITING:
      return;
  }
}

// The middle stage: a search for a backend of the type on every
// connection, or, for an addressed query, a PING addressed to the
// instance, which the server classes and ThreadedClient answer whatever
// their search policy, with delivery errors requested so that a
// multiplexer without the instance says so. Through a pinned lane the one
// multiplexer behind it is asked instead of all.
void ThreadedClient::Core::_search(InFlightPtr in_flight, bool keep_deadline) {
  float left = _stage_timeout(in_flight);
  if (in_flight->addressed() && left <= 0) {
    _finish(in_flight, TIMED_OUT, NULL);
    return;
  }
  if (!keep_deadline) {
    _arm(in_flight, left);  // the stage's time runs from here, a wait for a connection included
  }
  MultiplexerMessage msg;
  if (in_flight->addressed()) {
    msg = new_message(types::PING, std::string());
    msg.set_to(in_flight->prototype.to());
    msg.set_report_delivery_error(true);
  } else {
    BackendForPacketSearch search;
    search.set_packet_type(in_flight->prototype.type());
    msg = new_message(types::BACKEND_FOR_PACKET_SEARCH, std::string());
    search.SerializeToString(msg.mutable_message());
    if (in_flight->prototype.has_key()) {
      msg.set_key(in_flight->prototype.key());  // only a backend that takes the key may be found
    }
  }
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(msg));
  std::vector<ConnectionWrapper> sent;
  if (in_flight->pinned()) {
    ConnectionWrapper used;
    bool refused = false;
    if (_schedule(raw, in_flight->lane, &used, &refused, left > 0 ? left : in_flight->timeout)) {
      sent.push_back(used);
    } else if (refused) {
      _finish(in_flight, NOT_CONNECTED, NULL);
      return;
    }
  } else {
    basic_client_->schedule_all(raw, &sent, left > 0 ? left : in_flight->timeout);
  }
  if (sent.empty()) {
    _wait_for_connection(in_flight, InFlight::SEARCH);  // the search goes out when a connection is back
    return;
  }
  in_flight->search_id = msg.id();
  in_flight->search_ids.push_back(msg.id());
  in_flight->searched = sent;
  _track(in_flight, in_flight->search_id);
  in_flight->stage = InFlight::SEARCH;
}

// The search went through `connection` and has its answer from it now, a
// delivery error or the connection going down: it counts no more. False
// when the search never went through it.
bool ThreadedClient::Core::_answered(const InFlightPtr& in_flight, const ConnectionWrapper& connection) {
  std::vector<ConnectionWrapper>& searched = in_flight->searched;
  for (std::vector<ConnectionWrapper>::iterator it = searched.begin(); it != searched.end(); ++it) {
    if (it->is_same_connection(connection)) {
      searched.erase(it);
      return true;
    }
  }
  return false;
}

// The search, or the locate, found nobody: every multiplexer it went
// through answered with a delivery error. A request a backend may have
// can still be answered late, and the query waits for that reply until
// the stage's deadline; one that drew a delivery error itself cannot, and
// nothing can answer any more.
void ThreadedClient::Core::_nobody(InFlightPtr in_flight) { _wait_late(in_flight, /*direct=*/false); }

// The query can only wait, until the stage's deadline, for a late reply to
// an attempt a backend may have: the request when it may have been taken,
// the direct request when its connection went (`direct`), unless the
// client gave it up, its tracker reading LOST: it went nowhere. The
// trackers are read once, here: a connection writes in order, so the
// search every multiplexer answered went out behind the request on each,
// and the request was written or given up on by then. With neither,
// nothing can answer any more.
void ThreadedClient::Core::_wait_late(InFlightPtr in_flight, bool direct) {
  std::vector<std::uint64_t>& pending = in_flight->pending;
  pending.clear();
  auto given_up = [](const BasicClient::BasicScheduledMessageTracker& state) {
    return state && *state == SendState::LOST;
  };
  if (in_flight->taken && !given_up(in_flight->request_state)) {
    pending.push_back(in_flight->request_id);
  }
  if (direct && !given_up(in_flight->direct_state)) {
    pending.push_back(in_flight->direct_id);
  }
  if (pending.empty()) {
    _finish(in_flight, FAILED, NULL);
    return;
  }
  in_flight->stage = InFlight::LATE;
}

// The search found the instance that sent `ping`: the direct request goes
// to it, through the connection the PING came on.
void ThreadedClient::Core::_direct(InFlightPtr in_flight, const IncomingMessage& ping) {
  in_flight->direct_to = ping.third->sender();
  in_flight->direct_id = random64();  // kept by a direct request that waits for a connection
  _send_direct(in_flight, ping.second, /*keep_deadline=*/false);
}

// The direct request, the request again to `direct_to`: through
// `preferred`, the connection the PING came on, which is known to reach
// that instance; if it died meanwhile, or there is none, after a wait for
// a connection, any connection, since `to` is set, but through a pinned
// lane only the lane's. The lane adopts it: what follows the query goes
// the same way.
void ThreadedClient::Core::_send_direct(InFlightPtr in_flight, const ConnectionWrapper& preferred, bool keep_deadline) {
  MultiplexerMessage request = in_flight->prototype;
  request.set_id(in_flight->direct_id);
  request.set_to(in_flight->direct_to);
  request.set_report_delivery_error(true);  // a backend gone since its PING says so
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(request));
  if (in_flight->pinned()) {
    raw->mark_pinned();  // never handed to another connection if this one dies under it
  }
  const float left = _stage_timeout(in_flight);
  if (!keep_deadline) {
    _arm(in_flight, left);  // the stage's time runs from here, a wait for a connection included
  }
  ConnectionWrapper used = preferred;
  BasicClient::BasicScheduledMessageTracker tracker;
  if (preferred) {
    tracker = basic_client_->schedule_on(raw, preferred, left);
  }
  if (tracker && in_flight->lane) {
    in_flight->lane->adopt(used);
  }
  if (!tracker) {
    bool refused = false;
    tracker = _schedule(raw, in_flight->lane, &used, &refused, left);
    if (refused) {
      _finish(in_flight, NOT_CONNECTED, NULL);
      return;
    }
  }
  if (!tracker) {
    _wait_for_connection(in_flight, InFlight::DIRECT);
    return;
  }
  in_flight->sent_via = used;
  in_flight->sent_state = tracker;
  in_flight->direct_state = tracker;
  _track(in_flight, in_flight->direct_id);
  in_flight->stage = InFlight::DIRECT;
}

void ThreadedClient::Core::_arm(InFlightPtr in_flight, float timeout) {
  unsigned int generation = ++in_flight->generation;
  in_flight->timer->expires_after(mx::from_seconds(timeout));
  in_flight->timer->async_wait([this, in_flight, generation](const asio::error_code& error) {
    MX_DCHECK_RUN_ON(&io_thread_);
    _on_deadline(in_flight, generation, error);
  });
}

void ThreadedClient::Core::_on_deadline(InFlightPtr in_flight, unsigned int generation, const asio::error_code& error) {
  if (error == asio::error::operation_aborted || generation != in_flight->generation || !in_flight->callback) {
    return;
  }
  if (in_flight->stage == InFlight::REQUEST) {
    in_flight->taken = true;  // no answer in time: a backend may have the request
    _search(in_flight, /*keep_deadline=*/false);
  } else if (in_flight->stage == InFlight::WAITING) {
    _finish(in_flight, NOT_CONNECTED, NULL);
  } else {
    _finish(in_flight, TIMED_OUT, NULL);
  }
}

void ThreadedClient::Core::_finish(InFlightPtr in_flight, Outcome outcome, const IncomingMessage* reply) {
  asio::error_code ignored;
  in_flight->timer->cancel(ignored);
  ++in_flight->generation;
  // Every id the query was tracked under: the current request's and
  // direct request's, every search's, every earlier attempt's.
  // Kept until a late reply can no longer be expected: twice the longer of
  // the query's timeout and DEFAULT_TIMEOUT, a query with no deadline, or
  // none to wait (0, NaN), counting as DEFAULT_TIMEOUT.
  const float timeout = in_flight->timeout;
  const float kept = 2 * (std::isfinite(timeout) && timeout > DEFAULT_TIMEOUT ? timeout : DEFAULT_TIMEOUT);
  const std::chrono::steady_clock::time_point until = std::chrono::steady_clock::now() + mx::from_seconds(kept);
  by_id_.erase(in_flight->request_id);
  _remember_finished(in_flight->request_id, until);
  if (in_flight->direct_id) {
    by_id_.erase(in_flight->direct_id);
    _remember_finished(in_flight->direct_id, until);
  }
  for (std::uint64_t id : in_flight->search_ids) {
    by_id_.erase(id);
    _remember_finished(id, until);
  }
  if (in_flight->started) {
    in_flight_.erase(in_flight->place);
    in_flight->started = false;
  }
  Callback callback;
  callback.swap(in_flight->callback);  // a finished query never reports twice
  Result result;
  result.outcome = outcome;
  if (reply) {
    result.reply = *reply;
    if (in_flight->lane) {
      in_flight->lane->adopt(reply->second);  // what follows the query goes where the answer came from
    }
  }
  in_flight->lane.reset();                   // the query held it only for its own duration
  in_flight->received = ReceivedCallback();  // nothing more is told
  if (callback) {
    _guarded([&] { callback(result); },
             [&] { return "the callback of a query of type " + repr(in_flight->prototype.type()); });
  }
  if (in_flight->listed) {
    in_flight->prototype.Clear();  // its request goes now, its entry when the list is cleared out
    ++waiting_ended_;
    _clear_out_waiting();
  }
}

// The waiting queries that ended are cleared out once they are as many as
// those still waiting and 64 more, or none waits any more: however long no
// connection comes up, the list holds what waits and a bounded rest, at an
// amortized constant cost per query.
void ThreadedClient::Core::_clear_out_waiting() {
  const std::size_t live = waiting_queries_.size() - waiting_ended_;
  if (live != 0 && waiting_ended_ < live + 64) {
    return;
  }
  waiting_queries_.erase(std::remove_if(waiting_queries_.begin(), waiting_queries_.end(),
                                        [](const InFlightPtr& in_flight) {
                                          if (in_flight->callback) {
                                            return false;  // waiting still
                                          }
                                          in_flight->listed = false;
                                          return true;
                                        }),
                         waiting_queries_.end());
  waiting_ended_ = 0;
}

// Every query still in flight ends with SHUT_DOWN, in the order they
// began, and none waits for a connection any more.
void ThreadedClient::Core::_finish_every_query() {
  std::vector<InFlightPtr> pending(in_flight_.begin(), in_flight_.end());
  for (auto& in_flight : pending) {
    if (in_flight->callback) {
      _finish(in_flight, SHUT_DOWN, NULL);
    }
  }
  waiting_queries_.clear();
  waiting_ended_ = 0;
}

}  // namespace multiplexer
