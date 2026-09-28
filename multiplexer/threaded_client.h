// A client with a thread of its own: the connections live on an io thread
// that runs all the time, so heartbeats flow and reconnects happen whether
// or not the program is calling in. Any thread may send and query.
//
// This is the general form of what a peer needs from the library: the io
// thread waits on the network, on messages to hand to a processing thread,
// and on work posted by other threads. A peer built on it need not be
// passive in the rules file (see docs/handshake.md); the synchronous Client
// remains for programs that prefer no thread.
//
// Queries run as a state machine on the io thread, the same three stages as
// Client::_query (request; search for a backend on every connection; the
// request again to the backend found), each stage with its own deadline
// timer. A request with `to` set is an addressed query, Client::_query_addressed
// in shape: the middle stage locates that one instance, with a PING
// addressed to it, and the three stages share one deadline. A
// connection dying under a query does not cost the query its timeout: the
// request is sent again through another connection, or as soon as one
// comes back, the way the synchronous Client does inside a call (a request
// resent to the only multiplexer before its backend is back fails, as
// docs/semantics.md says). Many
// queries may be in flight at once, from any number of threads: replies
// are matched by the ids they reference, never by arrival order. The
// asynchronous form calls back on the io thread; the synchronous form is
// the same thing behind a future.
//
// A Lane (basic_client.h) given to a send or a query keeps a stream on one
// connection: the io thread writes the connection it used into the lane
// and reads it back for the next message; a pinned lane refuses any other
// connection. A ConnectionWrapper given instead is the connection to
// prefer for one message.
//
// Everything else that arrives is handled here or handed to the on_message
// callback given at construction: a late reply to a query that already
// ended is dropped (the ids of recently finished queries are remembered, a
// bounded number of them; so `references` means "this is the reply", and a
// follow-up that is not the reply must not reference the request but be
// addressed to the peer and correlated in the payload), REQUEST_RECEIVED
// for an unknown id is dropped, a PING without references is answered
// with a PING carrying its payload back (BACKEND_ERROR when that echo
// would be over MAX_MESSAGE_SIZE), a BACKEND_FOR_PACKET_SEARCH is dropped
// unless a search policy is set (set_search_policy), and the rest, events,
// requests addressed to this peer and the DELIVERY_ERRORs for messages
// that were not queries, go to on_message, or are logged and dropped when
// there is none. Nothing is ever queued for a reader that may
// never come.
//
// Threading, as declared: everything under io_thread_ runs on the io thread
// only, reached from other threads through io_service::post. Callbacks and
// on_message run on the io thread and must return quickly; they may call
// the asynchronous query() and send() but not the blocking query(), which
// throws std::logic_error there rather than deadlock the thread it runs on.
// One that throws is logged and the client goes on. shutdown() may be
// called there, and the destructor may run there, when the last reference
// to the client goes in a callback: neither waits for the thread, which
// ends on its own, the state it uses held until then (threaded_client_core.h).
#ifndef MX_MULTIPLEXER_THREADED_CLIENT_H_
#define MX_MULTIPLEXER_THREADED_CLIENT_H_

#include <asio/io_service.hpp>
#include <asio/steady_timer.hpp>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "lib/mutex.h"
#include "lib/random.h"
#include "lib/thread_checker.h"
#include "multiplexer/basic_client.h"
#include "multiplexer/client.h"
#include "multiplexer/defaults.h"

namespace multiplexer {

class ThreadedClient : public ExceptionDefinitions {
 public:
  // How a query ended. REPLIED: `reply` holds the answer. TIMED_OUT: a stage
  // ran out of time. FAILED: every multiplexer reported no backend of the
  // type, or the backend found is gone and no other took the request, or
  // the addressee is gone. NOT_CONNECTED: no live
  // connection to send through. SHUT_DOWN: shutdown() ran first.
  enum Outcome { REPLIED, TIMED_OUT, FAILED, NOT_CONNECTED, SHUT_DOWN };
  struct Result {
    Outcome outcome;
    IncomingMessage reply;  // set when outcome == REPLIED
    // The reply, or the exception the synchronous Client would have thrown.
    const IncomingMessage& check() const;
  };
  typedef std::function<void(const Result&)> Callback;
  typedef std::function<void(const IncomingMessage&)> MessageSink;

  // `on_message` receives, on the io thread, every message that is not a
  // reply to a query, the DELIVERY_ERRORs for messages that were not
  // queries included, and none of the protocol's own (see the file
  // comment); without it such messages are logged and dropped.
  explicit ThreadedClient(std::uint32_t peer_type, MessageSink on_message = MessageSink());
  ~ThreadedClient();  // shutdown() if not done

  // A backend built on this client answers the search clients use to find
  // a backend: with a policy set, every BACKEND_FOR_PACKET_SEARCH, routed
  // by type or addressed to this instance, is answered with a PING when
  // `answer()` returns true (on the io thread, so it must be quick) and
  // dropped otherwise, the way BaseThreadedMultiplexerServer declines when
  // saturated, with decline_searches_when_full. Without a policy, a
  // ThreadedClient answers no search. Call before connecting.
  typedef std::function<bool()> SearchPolicy;
  void set_search_policy(SearchPolicy answer);
  // Every message the program sent that the client gives up on, each copy
  // of one sent to ALL, is told to `observer` with its id and why
  // (DropReason), on the io thread, so it must be quick; dropped() counts
  // them, from any thread. The observer goes with the io thread.
  typedef BasicClient::DropObserver DropObserver;
  void set_drop_observer(DropObserver observer);
  std::uint64_t dropped();
  // How host names become addresses; for tests. See BasicClient::Resolver.
  void set_resolver(BasicClient::Resolver resolver);

  // Which of a multiplexer's routing paths reach this peer, told to every
  // multiplexer and carried in every welcome from now on; see
  // BasicClient::set_routing. A backend draining sets `any` and `all` off.
  // From any thread; in effect on the io thread right after.
  void set_routing(const Routing& routing);
  // Whether every connected multiplexer has the current routing in effect;
  // see BasicClient::routing_acknowledged. Not from the io thread.
  bool routing_acknowledged();
  // Waits until everything sent before the call has been written or given
  // up on, or `timeout` seconds; true when every one was written, false
  // when one was given up on, which the drop observer names, or the time
  // ran out. A send still waiting for a connection or for room counts, as
  // does what is queued on a connection; what is sent after the call does
  // not, so a flush ends however busy the client is, a message a dying
  // connection hands to another included. After shutdown() it returns
  // true at once, nothing being left to wait for. What shutdown() does
  // first. Not from the io thread.
  bool flush_all(float timeout);
  // flush_all() with `done(flushed)` on the io thread instead of the wait,
  // safe from any thread including the io thread: what an asyncio layer
  // awaits.
  typedef std::function<void(bool)> FlushCallback;
  void flush_all_with_callback(float timeout, FlushCallback done);

  std::uint64_t instance_id() const { return instance_id_; }
  std::uint32_t peer_type() const { return peer_type_; }
  std::uint64_t random64();  // thread-safe

  // Connects and waits up to `timeout` for the handshake; true when the
  // connection is registered, false as soon as it failed. False is not
  // final: the io thread keeps reconnecting every AUTO_RECONNECT_TIME
  // seconds on its own. Not from the io thread.
  bool connect(const std::string& host, std::uint16_t port, float timeout = DEFAULT_TIMEOUT);
  // Drops the multiplexer connect() was given with this host and port, as
  // SyncClient::disconnect() does, on the io thread: no reconnect to it
  // any more, unless connect() is called again; a live connection to it
  // closed, what it had not written going to the other connections or
  // held, and queries through it sent again elsewhere, as for a lost
  // connection; a connect() waiting for it returns false. Returns once
  // done, whether the client had it. Throws NotConnected after shutdown(),
  // as connect() does. Not from the io thread.
  bool disconnect(const std::string& host, std::uint16_t port);
  unsigned int connections_count();
  // How many message ids the client watches for an answer: those of the
  // queries in flight, every attempt's and every search's. Zero once every
  // query has ended; for tests. Not from the io thread.
  std::size_t watched_ids();
  // How many times the io thread has tried a message again after it could
  // not be queued at once, sends and queries: what waiting for room costs.
  // It grows with the messages that waited, never with their square; for
  // tests. Not from the io thread.
  std::uint64_t retries();
  // How many queries, and how many messages, the client keeps for a
  // connection to come up or for room, those that ended there and are not
  // cleared out yet included: a bounded few more than what still waits,
  // however long no multiplexer is up; for tests. Not from the io thread.
  std::size_t waiting_queries();
  std::size_t waiting_messages();

  // Sending. The message must carry its id and from; new_message() fills
  // those in. send() queues it on one live connection (round robin),
  // send_all() on every one, and both return at once: the write happens on
  // the io thread right after, so they are safe from callbacks. A message
  // that cannot be queued yet waits on the io thread, behind those sent
  // before it, for a connection to come up, or, when the connections'
  // queues are full, for room, within DEFAULT_TIMEOUT, and is dropped and
  // reported after that (set_drop_observer); send_all() gives every live
  // connection its copy, a full one as soon as it has room. With a lane,
  // on the lane's connection, which takes the connection chosen when it
  // has none or lost its own, and waits for room on its own while that
  // lives. Through a pinned lane already closed() it throws NotConnected,
  // as the Python client raises it; one whose connection goes after the
  // call has the message dropped and reported. `done`, when given, hears
  // how the message ended, once, on the io thread: 1 once it was written,
  // the first copy for send_all(), 0 once it was given up on or shutdown()
  // came first, as the Python send_message(callback=) does; send(msg,
  // LanePtr(), done) for a message with no lane.
  typedef std::function<void(unsigned int)> SendCallback;
  void send(const MultiplexerMessage& msg);
  void send(const MultiplexerMessage& msg, LanePtr lane, SendCallback done = SendCallback());
  void send_all(const MultiplexerMessage& msg, SendCallback done = SendCallback());
  // Through `connection`, the one a reply came through, while it is live,
  // another when it is gone.
  void send(const MultiplexerMessage& msg, const ConnectionWrapper& connection, SendCallback done = SendCallback());
  // The flushing forms, from any thread but the io thread: wait until the
  // message reached the socket, on one connection (handed to another if
  // the first dies before writing it, as every message is), on the lane's,
  // on `connection` or another; send_all() until one copy is written, the
  // others going out from their connections' queues, so that a
  // multiplexer frozen with its socket open holds nobody to the timeout;
  // or until `timeout` passes. Return 1 once a copy is written, 0 when none
  // was in time or the message was given up on, a pinned lane's
  // connection being gone for instance, or every send_all() copy lost
  // with its connection: a copy is held only when no connection is live,
  // and dropped otherwise. Throw NotConnected after shutdown(), as every
  // send does. flush_all() waits for every copy.
  unsigned int send(const MultiplexerMessage& msg, float timeout);
  unsigned int send(const MultiplexerMessage& msg, LanePtr lane, float timeout);
  unsigned int send(const MultiplexerMessage& msg, const ConnectionWrapper& connection, float timeout);
  unsigned int send_all(const MultiplexerMessage& msg, float timeout);
  // The same for an already serialized MultiplexerMessage (the Python
  // side), waiting for a connection or for room within `timeout`. `done`,
  // when given, hears how the message ended: 1 once it reached a socket,
  // the first copy for ALL, 0 once it was given up on, and reported, or
  // shutdown() came first.
  void send_serialized(std::string serialized, LanePtr lane = LanePtr(), float timeout = DEFAULT_TIMEOUT,
                       SendCallback done = SendCallback());
  void send_all_serialized(std::string serialized, float timeout = DEFAULT_TIMEOUT, SendCallback done = SendCallback());
  // The flushing send for the serialized form; `not_connected`, when
  // given, says why one that returned 0 wrote nothing, as the synchronous
  // client tells it: true when the message was given up on, a pinned
  // lane's connection being gone or the client shutting down, or when no
  // connection was live as the time ran out, false when it ran out with
  // one live; a message out of time waits ROOM_GRACE_SECONDS more and is
  // then dropped. NotConnected and OperationTimedOut, for the Python
  // clients, decided here on the io thread at the deadline.
  unsigned int send_serialized_and_wait(std::string serialized, bool all, float timeout, LanePtr lane = LanePtr(),
                                        bool* not_connected = NULL);
  // The flushing send with a callback instead of a wait, safe from any
  // thread including the io thread: `done(written)` runs on the io thread
  // with 1 once a copy reached a socket, the first for ALL, or with 0 when
  // the message was given up on, `timeout` passed or the client shut down
  // first. What an asyncio layer awaits.
  void send_serialized_with_callback(std::string serialized, bool all, float timeout, SendCallback done,
                                     LanePtr lane = LanePtr());
  // The same, `done` also hearing why a send that wrote nothing did, as
  // send_serialized_and_wait()'s `not_connected` says.
  typedef std::function<void(unsigned int written, bool not_connected)> FlushedCallback;
  void send_serialized_and_notify(std::string serialized, bool all, float timeout, FlushedCallback done,
                                  LanePtr lane = LanePtr());
  MultiplexerMessage new_message(std::uint32_t type, const std::string& payload);

  // A request with a reply, see the file comment. The callback runs on the
  // io thread; after shutdown(), at once, on the calling thread, with
  // SHUT_DOWN, so it must not query again then, nor take a lock its caller
  // holds. The blocking form throws std::logic_error when called on the
  // io thread, that is from a callback, where it would deadlock. The
  // message forms take the request as a whole, `to` included, and set its
  // id and from per attempt; an addressed one locates its addressee with a
  // PING addressed to it. With a lane the request goes through the lane's
  // connection and the lane adopts the connection the reply came through, a
  // pinned lane allowing no other; with a connection, through that one
  // while it is live. `received`, when given, is told on the io thread the
  // instance id of each backend that acknowledges an attempt with
  // REQUEST_RECEIVED (notify_start()): once, normally, or again when a
  // retry reached a backend, the same or another; nothing about the query
  // changes for it.
  void query(const std::string& payload, std::uint32_t type, Callback callback, float timeout = DEFAULT_TIMEOUT,
             LanePtr lane = LanePtr(), ReceivedCallback received = ReceivedCallback());
  Result query(const std::string& payload, std::uint32_t type, float timeout = DEFAULT_TIMEOUT,
               LanePtr lane = LanePtr(), ReceivedCallback received = ReceivedCallback());
  void query(const MultiplexerMessage& msg, Callback callback, float timeout = DEFAULT_TIMEOUT,
             LanePtr lane = LanePtr(), ReceivedCallback received = ReceivedCallback());
  Result query(const MultiplexerMessage& msg, float timeout = DEFAULT_TIMEOUT, LanePtr lane = LanePtr(),
               ReceivedCallback received = ReceivedCallback());
  void query(const MultiplexerMessage& msg, const ConnectionWrapper& connection, Callback callback,
             float timeout = DEFAULT_TIMEOUT, ReceivedCallback received = ReceivedCallback());
  Result query(const MultiplexerMessage& msg, const ConnectionWrapper& connection, float timeout = DEFAULT_TIMEOUT,
               ReceivedCallback received = ReceivedCallback());

  // Ends every in-flight query with SHUT_DOWN, writes what was sent before
  // the call, and what the io thread sends meanwhile, a server's refusal of
  // what still arrives say, `timeout` seconds in all, any other send
  // meanwhile throwing NotConnected, then closes the connections and
  // stops the io thread; what is still unwritten is dropped and reported,
  // at once with 0. Idempotent; the destructor calls it. On the io thread
  // itself it does not wait for the thread, which ends once its handlers
  // are done. The callbacks the client was given, on_message and the
  // search policy, are destroyed when the io thread ends, on that thread,
  // and in a forked child by shutdown() itself.
  void shutdown(float timeout = CLOSE_FLUSH_SECONDS);
  // Whether this client was inherited across a fork: its calls then throw
  // UsedAfterFork, and shutdown() and the destructor only close the
  // child's descriptor copies, once; see BasicClient::orphaned.
  bool orphaned() const;
  // The io thread's lines about messages that went nowhere
  // (BasicClient::drop_lines, LogSummary), for a message callback, which
  // runs there: its own lines about such messages, of kinds numbered from
  // BasicClient::OWN_LINES, go at the library's rate, the first of a kind
  // at once and the rest as a count about once a second. The io thread
  // only.
  LogSummary& drop_lines();

 private:
  class Core;  // the state, which the io thread co-owns: threaded_client_core.h
  std::shared_ptr<Core> core_;
  const std::uint32_t peer_type_;
  const std::uint64_t instance_id_;
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_THREADED_CLIENT_H_
