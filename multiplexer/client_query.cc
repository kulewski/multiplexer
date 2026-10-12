// Client's queries: the query algorithm, every stage reading what comes
// back by one table (_await), an addressed query's stages, and the search.
// Part of Client; see client.cc. The design is in client.h.
#include <limits>
#include <utility>

#include "multiplexer/Multiplexer.pb.h"
#include "multiplexer/client.h"
#include "multiplexer/multiplexer.constants.h"

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

namespace {
// Throws std::length_error, before anything goes out, when the request
// sent again, a copy of `request` with an id of its own, for a typed query
// the `to` of the backend found, and a delivery error asked for, would be
// over MAX_MESSAGE_SIZE: measured at its largest, as ThreadedClient
// measures it. `request` is as it was afterwards.
void check_size_sent_again(MultiplexerMessage* request) {
  const std::uint64_t id = request->id();
  const bool had_to = request->has_to();
  const std::uint64_t to = request->to();
  const bool asked = request->has_report_delivery_error();
  const bool asked_value = request->report_delivery_error();
  request->set_id(std::numeric_limits<std::uint64_t>::max());
  if (!to) {
    request->set_to(std::numeric_limits<std::uint64_t>::max());
  }
  request->set_report_delivery_error(true);
  const std::size_t size = request->ByteSizeLong();
  request->set_id(id);
  if (had_to) {
    request->set_to(to);
  } else {
    request->clear_to();
  }
  if (asked) {
    request->set_report_delivery_error(asked_value);
  } else {
    request->clear_report_delivery_error();
  }
  check_message_size(size);
}
}  // namespace

std::vector<uint64_t> Client::_ids(const std::vector<Attempt>& attempts) {
  std::vector<uint64_t> ids;
  ids.reserve(attempts.size());
  for (const Attempt& attempt : attempts) {
    ids.push_back(attempt.id);
  }
  return ids;
}

Client::Attempt* Client::Ledger::attempt(std::uint64_t id) {
  for (Attempt& each : attempts) {
    if (each.id == id) {
      return &each;
    }
  }
  return nullptr;
}

bool Client::Ledger::any_left() const {
  for (const Attempt& each : attempts) {
    if (!each.struck) {
      return true;
    }
  }
  return false;
}

void Client::Ledger::strike_given_up() {
  for (Attempt& each : attempts) {
    if (!each.state || *each.state == SendState::LOST) {
      each.struck = true;
    }
  }
}

bool Client::Ledger::answered(const ConnectionWrapper& connection) {
  for (std::vector<ConnectionWrapper>::iterator it = searched.begin(); it != searched.end(); ++it) {
    if (it->is_same_connection(connection)) {
      searched.erase(it);
      return true;
    }
  }
  return false;
}

// The query algorithm; the Python Client.query in mxclient.py is the same
// steps and the two must stay in agreement, and every stage reads what
// comes back by the one table every client follows (_await). A request
// with `to` set is an addressed query, with stages of its own below.
IncomingMessage Client::_query(const MultiplexerMessage& query, float timeout, LanePtr lane,
                               ReceivedCallback received) {
  // The query's on_received for its duration, where _receive meets the
  // acknowledgements; whatever was there before is back afterwards.
  struct Holding {
    ReceivedCallback& slot;
    ReceivedCallback previous;
    ~Holding() { slot = std::move(previous); }
  } holding{received_, std::exchange(received_, std::move(received))};
  if (query.to()) {
    return _query_addressed(query, timeout, lane);
  }
  // The request itself, an empty sender filled in, and an id of its own, as
  // every attempt gets: ids belong to attempts, so that one message may be
  // queried again and again, which a receiver that drops a repeated id
  // would otherwise answer once.
  MultiplexerMessage request = query;
  request.set_id(random64());
  if (!request.sender()) {
    request.set_sender(instance_id());
  }
  check_size_sent_again(&request);

  // Stage 1: the request through one connection. A reply ends the query
  // here; a delivery error says nobody took it; its time running out, or
  // its connection lost, leaves it with a backend perhaps, routed before
  // the connection went, its reply able to come back another way.
  Ledger ledger;
  std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
  try {
    const ConnectionWrapper used = _send_attempt(request, *timer, ConnectionWrapper(), lane, &ledger);
    const Outcome outcome = _await(ledger, *timer, Stage::REQUEST, &used, lane);
    if (outcome.kind == Outcome::ANSWER) {
      adopt(lane, outcome.message.second);
      return outcome.message;
    }
  } catch (OperationTimedOut&) {
  }

  // Stage 2: ask every multiplexer who has a backend of this type, at
  // once. The request is not sent again: it goes out at most twice, and a
  // query never goes back a stage. The search is routed by the request
  // type's own rule with whom forced to ALL, so every live backend answers
  // with a PING. Through a pinned lane the one multiplexer behind it is
  // asked instead.
  const MultiplexerMessage search = _locator_for(request);
  timer = basic_client_->create_timer(timeout);
  const bool pinned = lane && lane->pinned();
  const ConnectionWrapper pinned_connection = _send_search(search, *timer, lane, &ledger);
  Outcome outcome = _await(ledger, *timer, Stage::SEARCH, pinned ? &pinned_connection : NULL, lane);
  if (outcome.kind != Outcome::ANSWER && outcome.kind != Outcome::FOUND) {
    // Nobody has a backend of this type: only a backend that may have
    // taken the request can still answer it, and the stage waits for that.
    outcome = Outcome{Outcome::ANSWER, _late(ledger, *timer, lane)};
  }
  if (outcome.kind == Outcome::ANSWER) {
    adopt(lane, outcome.message.second);
    return outcome.message;
  }

  // Stage 3: the request again, to the backend that answered first, by
  // instance id and through the connection its PING came on, asking for a
  // delivery error, which says that backend is gone: a copy of the
  // request, its workflow and every other field kept, with a fresh id.
  const IncomingMessage ping = outcome.message;
  MultiplexerMessage direct_query = request;
  direct_query.set_id(random64());
  direct_query.set_to(ping.third->sender());
  direct_query.set_report_delivery_error(true);
  timer = basic_client_->create_timer(timeout);
  const ConnectionWrapper used = _send_attempt(direct_query, *timer, ping.second, lane, &ledger);
  outcome = _await(ledger, *timer, Stage::DIRECT, &used, lane);
  if (outcome.kind != Outcome::ANSWER) {
    // The backend found is gone, or the direct request's connection went
    // under it: a reply can come only to an attempt a backend may have,
    // and nothing goes out again.
    outcome.message = _late(ledger, *timer, lane);
  }
  adopt(lane, outcome.message.second);
  return outcome.message;
}

// An addressed query, docs/query.md "An addressed query": the request with
// its `to`, through the lane's connection or any; when the multiplexer
// reports the instance is not behind it, or the connection dies under the
// wait, a PING addressed to the instance on every connection (through a
// pinned lane, on its one connection) finds the multiplexer that has it;
// then the request again through that connection, its second and last
// time out. Only the addressee ever gets the request: an instance that
// left is OperationFailed, never another instance of its type, unless a
// request the addressee may have taken can still be answered, which the
// query then waits for. One `timeout` covers the three stages, and a
// request that gets no answer at all within it is OperationTimedOut
// without a PING, since a silent addressee is one the multiplexer still
// has, and a PING would find the same one.
IncomingMessage Client::_query_addressed(const MultiplexerMessage& query, float timeout, LanePtr lane) {
  std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
  MultiplexerMessage request = query;
  request.set_id(random64());  // the attempt's own, as every attempt gets
  if (!request.sender()) {
    request.set_sender(instance_id());
  }
  request.set_report_delivery_error(true);  // "not behind this multiplexer" must come back as a message
  check_size_sent_again(&request);

  Ledger ledger;
  const ConnectionWrapper used = _send_attempt(request, *timer, ConnectionWrapper(), lane, &ledger);
  Outcome outcome = _await(ledger, *timer, Stage::REQUEST, &used, lane);
  if (outcome.kind == Outcome::ANSWER) {
    adopt(lane, outcome.message.second);
    return outcome.message;
  }

  // Locate: a PING, addressed to the instance, with delivery errors
  // requested, so that a multiplexer without the instance says so. The
  // request is not sent again on a lost connection: it goes out at most
  // twice, and a query never goes back a stage.
  const MultiplexerMessage locate = _locator_for(request);
  const bool pinned = lane && lane->pinned();
  const ConnectionWrapper pinned_connection = _send_search(locate, *timer, lane, &ledger);
  outcome = _await(ledger, *timer, Stage::SEARCH, pinned ? &pinned_connection : NULL, lane);
  if (outcome.kind != Outcome::ANSWER && outcome.kind != Outcome::FOUND) {
    // No multiplexer has the instance: a request it may have taken can
    // still be answered, and the query waits for that reply.
    outcome = Outcome{Outcome::ANSWER, _late(ledger, *timer, lane)};
  }
  if (outcome.kind == Outcome::ANSWER) {
    adopt(lane, outcome.message.second);
    return outcome.message;
  }

  // The request again, a fresh id, through the connection the answer came
  // on; the lane adopts it.
  MultiplexerMessage again = request;
  again.set_id(random64());
  const ConnectionWrapper again_used = _send_attempt(again, *timer, outcome.message.second, lane, &ledger);
  outcome = _await(ledger, *timer, Stage::DIRECT, &again_used, lane);
  if (outcome.kind != Outcome::ANSWER) {
    // The addressee is gone, or the request's connection went under it:
    // the query waits for a reply to an attempt it may still have.
    outcome.message = _late(ledger, *timer, lane);
  }
  adopt(lane, outcome.message.second);
  return outcome.message;
}

// The message that locates who can take `query`: when it has a `to`, a
// PING addressed to that instance, with delivery errors requested, which
// the server classes and ThreadedClient answer whatever their search
// policy; else a search for a backend of its type.
MultiplexerMessage Client::_locator_for(const MultiplexerMessage& query) {
  MultiplexerMessage mxmsg;
  mxmsg.set_id(random64());
  mxmsg.set_sender(instance_id());
  if (query.to()) {
    mxmsg.set_type(types::PING);
    mxmsg.set_to(query.to());
    mxmsg.set_report_delivery_error(true);
  } else {
    BackendForPacketSearch search;
    search.set_packet_type(query.type());
    mxmsg.set_type(types::BACKEND_FOR_PACKET_SEARCH);
    search.SerializeToString(mxmsg.mutable_message());
    if (query.has_key()) {
      mxmsg.set_key(query.key());  // only a backend that takes the key may be found
    }
  }
  return mxmsg;
}

// Sends one attempt of a query, the request or the request sent again,
// through one connection, `preferred` while it is live, waiting for a
// connection if none is: see _send_one. The ledger records it before the
// send, its tracker once it is placed: a send that runs out of time leaves
// the message queued, to be written later, and a reply to it still
// answers. Returns the connection that wrote it.
ConnectionWrapper Client::_send_attempt(const MultiplexerMessage& mxmsg, mx::SimpleTimer& timer,
                                        ConnectionWrapper preferred, const LanePtr& lane, Ledger* ledger) {
  ledger->attempts.push_back(Attempt{mxmsg.id(), BasicClient::BasicScheduledMessageTracker()});
  return _send_one(mxmsg, timer, preferred, lane, &ledger->attempts.back().state);
}

// Sends the search, or the locating PING: a copy on every live
// connection, waiting for one when none is (_place_search), or through a
// pinned lane its one connection. The copies wait for room, or for a connection, as long
// as the stage has left and a moment more, as an attempt's send lets it
// wait (_send_and_wait), none for a stage with no deadline: a copy left
// after the stage is an answer nobody waits for. The ledger records its id
// and the connections it went through. Returns the pinned lane's
// connection, empty without one.
ConnectionWrapper Client::_send_search(const MultiplexerMessage& mxmsg, mx::SimpleTimer& timer, const LanePtr& lane,
                                       Ledger* ledger) {
  ledger->search_id = mxmsg.id();
  ledger->searched.clear();
  if (lane && lane->pinned()) {
    const ConnectionWrapper used = _send_one(mxmsg, timer, ConnectionWrapper(), lane);
    ledger->searched.push_back(used);
    return used;
  }
  basic_client_->check_not_orphaned();
  _place_search(_serialize(mxmsg), timer, &ledger->searched);
  return ConnectionWrapper();
}

// The search's copies on every live connection, the connections in
// `searched`; with none placed, a wait for a connection within the
// stage's time and the search placed then, as often as one counted up
// goes before the search is placed on it, its end the next thing the
// loop runs. NotConnected when none came within the stage's time.
void Client::_place_search(std::shared_ptr<const RawMessage> raw, mx::SimpleTimer& timer,
                           std::vector<ConnectionWrapper>* searched) {
  for (bool waited = false;; waited = true) {
    basic_client_->poll();
    if (basic_client_->schedule_all(raw, searched, _room_for(timer.remaining())) != 0) {
      return;
    }
    if (waited && (timer.expired() || basic_client_->run_one() == 0)) {
      // None came within the stage's time, or nothing can come any more;
      // else the connection counted went before the search was placed on
      // it, and the loop has run what came next.
      MXTHROW(NotConnected());
    }
    if (!basic_client_->wait_for_any_connection(timer)) {
      MXTHROW(NotConnected());
    }
  }
}

// Waits for what ends a query's stage, reading each message by the one
// table every client follows (docs/query.md), as ThreadedClient does: a
// reply to any attempt is the answer; a delivery error for an attempt
// strikes it off, and ends the stage only when it is the stage's own, the
// last attempt sent in the request's stage or the direct request's, the
// late wait ending once no attempt is left; in the search's stage a PING
// for it is the instance found, and a delivery error for it is nobody
// behind the connection it came on, counted once per connection the
// search went through, nobody anywhere once every one of them has said
// so. Anything else for the search is nothing, and what answers nothing
// the query sent is logged and dropped (_receive). With `watch`, an
// attempt's connection or a pinned lane's, its loss ends the wait,
// NotConnected through a pinned lane, which is what the pin means.
// OperationTimedOut when `timer` runs out.
Client::Outcome Client::_await(Ledger& ledger, mx::SimpleTimer& timer, Stage stage, const ConnectionWrapper* watch,
                               const LanePtr& lane) {
  std::vector<uint64_t> accept_ids = _ids(ledger.attempts);
  if (stage == Stage::SEARCH) {
    accept_ids.push_back(ledger.search_id);
  }
  // The search's late answers are skipped silently; none before it exists.
  const std::uint64_t search = ledger.search_id ? ledger.search_id : ~std::uint64_t(0);
  for (;;) {
    bool gone = false;
    IncomingMessage incoming = _receive(timer, accept_ids, types::REQUEST_RECEIVED, search, watch, &gone);
    if (gone) {
      if (lane && lane->pinned()) {
        MXTHROW(NotConnected());
      }
      MX_LOG(WARNING, MEDIUMVERBOSITY,
             CTX("multiplexer.client") TEXT("connection lost while waiting for a reply to " +
                                            repr(ledger.attempts.empty() ? 0 : ledger.attempts.back().id)));
      return Outcome{Outcome::LOST, IncomingMessage()};
    }
    const MultiplexerMessage& mxmsg = *incoming.third;
    if (Attempt* attempt = ledger.attempt(mxmsg.references())) {
      if (mxmsg.type() != types::DELIVERY_ERROR) {
        return Outcome{Outcome::ANSWER, incoming};
      }
      attempt->struck = true;  // nobody has it
      const bool own = (stage == Stage::REQUEST || stage == Stage::DIRECT) && attempt == &ledger.attempts.back();
      if (own) {
        return Outcome{Outcome::REFUSED, incoming};
      }
      if (stage == Stage::LATE && !ledger.any_left()) {
        return Outcome{Outcome::NOBODY, incoming};
      }
      continue;  // an earlier attempt's: struck off, and the stage goes on
    }
    // The search's own answer: accept_ids hold its id only in its stage.
    if (mxmsg.type() == types::PING) {
      return Outcome{Outcome::FOUND, incoming};
    }
    if (mxmsg.type() == types::DELIVERY_ERROR && ledger.answered(incoming.second) && ledger.searched.empty()) {
      return Outcome{Outcome::NOBODY, incoming};
    }
  }
}

// The query's last wait: a reply to an attempt a backend may still have,
// one placed and neither refused nor given up on, until `timer` runs out;
// none left, nothing can answer any more, OperationFailed. The trackers
// are read once, as the wait begins, as ThreadedClient reads them
// (_wait_late): a connection writes in order, so after a search that every
// multiplexer answered the request went out, or was given up on, before
// it. A lost connection says nothing about an attempt, which may have
// been routed before it went; but through a pinned lane the reply can
// come only the lane's way, and its connection gone is NotConnected,
// which is what the pin means.
IncomingMessage Client::_late(Ledger& ledger, mx::SimpleTimer& timer, const LanePtr& lane) {
  ledger.strike_given_up();
  if (!ledger.any_left()) {
    MXTHROW(OperationFailed());
  }
  const bool pinned = lane && lane->pinned();
  const ConnectionWrapper watch = pinned ? lane->connection() : ConnectionWrapper();
  const Outcome outcome = _await(ledger, timer, Stage::LATE, pinned ? &watch : NULL, lane);
  if (outcome.kind != Outcome::ANSWER) {
    MXTHROW(OperationFailed());  // the last attempt a backend might have had drew a delivery error
  }
  return outcome.message;
}

// Waits for a message referencing one of accept_ids. With `watch`, also
// stops when that connection dies, setting *lost instead of returning a
// message.
IncomingMessage Client::_receive(mx::SimpleTimer& timer, const std::vector<uint64_t>& accept_ids, uint32_t ignore_type,
                                 uint64_t ignore_id, const ConnectionWrapper* watch, bool* lost) {
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
    const MultiplexerMessage& mxmsg = *result.third;

    if (mxmsg.type() == ignore_type) {
      if (received_ && mxmsg.type() == types::REQUEST_RECEIVED && mxmsg.references() != ignore_id &&
          contains(accept_ids, mxmsg.references())) {
        // A backend acknowledged the request, or a retry of it: the query's
        // on_received hears which, and the query goes on as before, whatever
        // the callback does.
        try {
          received_(mxmsg.sender());
        } catch (const std::exception& error) {
          MX_LOG(ERROR, LOWVERBOSITY,
                 CTX("SyncClient") TEXT(std::string("the on_received of a query threw: ") + error.what()));
        }
      }
      continue;
    }
    if (contains(accept_ids, mxmsg.references())) {
      return result;
    }
    if (ignore_id != mxmsg.references()) {
      MX_LOG(WARNING, HIGHVERBOSITY,
             TEXT("message (id=" + repr(mxmsg.id()) + ", type=" + repr(mxmsg.type()) +
                  ", sender=" + repr(mxmsg.sender()) + ", references=" + repr(mxmsg.references()) +
                  ") while waiting "
                  "for reply for " +
                  repr(accept_ids)));
    }
  }

  MXTHROW(OperationTimedOut());
}

}  // namespace multiplexer
