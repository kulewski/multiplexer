// The multiplexer itself: accepts connections and routes every message it
// receives according to the rules file, to one peer, to all peers of a type,
// or to the instance id named in the message.
//
// Server is a ConnectionsManager whose connections are peers of any type.
// It has one thread and one io_service, declared: every member is guarded
// by the owner thread's checker, which the calls from outside check and
// the helpers require, so that --config=clang proves no other thread
// touches the state. All the work per message is _handle_message in
// server.cc. A frame is forwarded as the RawMessage it arrived in, never
// re-serialized: routing costs the frame's checksum, its parse, which copies
// the payload once, a few map lookups and the writes. Everything is in one
// translation unit, so the compiler inlines the per-message path as it did
// when it lived in this header.
//
// The routing order, first match wins: a `to` field; override_rrules
// carried by the message; protocol (meta) types below 100; the rules file's
// entries for the type. Nobody to deliver to produces a DELIVERY_ERROR back
// to the sender when the rule asks for it. A peer's Routing (its welcome,
// then PEER_CONTROL) says which rule-routed paths reach it, and its key
// filters (KeyPatterns, multiplexer/key_filters.h) which keys of a type
// do; `to` always does. docs/wire_format.md describes the same order from
// the outside, docs/keys.md the keys.
#ifndef MX_MULTIPLEXER_SERVER_H_
#define MX_MULTIPLEXER_SERVER_H_

#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/steady_timer.hpp>
#include <chrono>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "lib/functors.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/config.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/defaults.h"
#include "multiplexer/io/connection.h"
#include "multiplexer/key_filters.h"
#include "multiplexer/log_summary.h"
#include "multiplexer/multiplexer.constants.h" /* generated */
#include "multiplexer/recorder.h"

namespace multiplexer {

class Server;  // forward

// The multiplexer's queues hold bare frames and nobody tracks their fate:
// scheduling a frame answers true or false, and that is all a sender learns.
template <>
struct ConnectionsManagerTraits<Server> : public DefaultConnectionsManagerTraits {
  // Every frame is parsed into one message the server keeps
  // (Server::parse_target).
  static constexpr bool REUSES_PARSE_TARGET = true;

  struct MessagesBufferTraits : public DefaultConnectionsManagerTraits::MessagesBufferTraits {
    typedef std::shared_ptr<const RawMessage> value_type;
    typedef mx::ReferencingFunctor<value_type> ToRawMessagePointerConverter;
    typedef mx::ReferencingFunctor<value_type> ToBufferRepresentationConverter;

    typedef mx::ConstructingFunctor<bool, value_type> SchedulingResultFunctor;
  };

  typedef multiplexer::Connection<Server> Connection;
};

// See the file comment. Created with Create(), given its rules file with
// set_rules_file() and load_rules(), started with start(), driven by
// io_service.run() in mxcontrol/start_multiplexer_server.cc.
class Server : public ConnectionsManager<Server>, public std::enable_shared_from_this<Server> {
 private:
  Server(asio::io_service& io_service, const std::string& host, unsigned short port);

 public:
  typedef ConnectionsManager<Server> Base;
  typedef std::shared_ptr<Server> pointer;
  typedef std::weak_ptr<Server> weak_pointer;
  static pointer Create(asio::io_service& io_service, const std::string& host, unsigned short port) {
    return pointer(new Server(io_service, host, port));
  }

  // ConnectionsManager interface
  std::shared_ptr<const RawMessage> get_welcome_message() {
    MX_DCHECK_RUN_ON(&owner_thread());
    if (!welcome_message_) {
      welcome_message_ = create_welcome_message(multiplexer::peers::MULTIPLEXER);
    }
    return welcome_message_;
  }

  // Entry point for every routed message, called by a Connection once the
  // frame is parsed. Drops messages that claim to come from this multiplexer
  // and hands the rest to _handle_message. The pointers are the caller's,
  // held for the whole call: routing takes them by reference down to the
  // queue that keeps the frame, and changes no reference count before.
  void handle_message(const Connection::pointer& conn, const std::shared_ptr<const RawMessage>& raw,
                      const std::shared_ptr<MultiplexerMessage>& msg);

  // The message the connections parse every frame into, kept from frame to
  // frame and cleared, so that its strings keep their capacity and the
  // payload or the batch of a frame is copied into memory already there:
  // the parse of a frame of a batch, a megabyte, allocates nothing, where
  // it allocated a megabyte and freed it after routing. Routing keeps
  // nothing of a message past handle_message; when something still holds
  // the last one after all, or its strings hold more than KEPT_PARSE_BYTES,
  // or it held override rules or fields this multiplexer does not know,
  // which a clear keeps the room of too, a fresh one, so that an odd
  // frame's memory goes.
  std::shared_ptr<MultiplexerMessage> parse_target();
  // The most a kept parse target's strings may hold: its payload, batch,
  // key and workflow.
  static constexpr std::size_t KEPT_PARSE_BYTES = 4 * 1024 * 1024;

  // Starts accepting connections, and checking the rules file.
  void start();

  // The rules file. set_rules_file() names it and load_rules() reads it:
  // when the text differs from the rules in use, it is parsed and put in
  // use whole, so a message type added to the file is routed and a peer
  // type added is accepted from then on, without a restart. A file that is
  // missing or does not parse leaves the rules in use as they were, with
  // `error` set and kept for the status; the multiplexer serves on. Called
  // at start by run_multiplexer, then every `rules_check_interval` seconds
  // by a timer (an edit on disk, which is how a Kubernetes ConfigMap
  // arrives), on SIGHUP, and on a peer's RULES_CONTROL RELOAD.
  enum class RulesLoad { LOADED, UNCHANGED, FAILED };
  void set_rules_file(const std::string& path) {
    MX_DCHECK_RUN_ON(&owner_thread());
    rules_file_ = path;
  }
  RulesLoad load_rules(std::string* error);
  // How often start() then checks the file, in seconds; 0, a negative
  // interval or NaN never. The check puts a changed file in use once two
  // checks in a row have read the same new bytes, so a file caught in the
  // middle of being written is never applied; load_rules(), an operator's
  // explicit ask, applies at once. A positive interval below
  // MIN_RULES_CHECK_INTERVAL, which would read the file over and over on
  // the io thread, is refused with std::invalid_argument, the interval
  // set before kept.
  void set_rules_check_interval(float seconds);
  static constexpr float MIN_RULES_CHECK_INTERVAL = 0.01;
  // Why `seconds` cannot be the rules check interval, or empty when it
  // can: the one check, which set_rules_check_interval() and
  // run_multiplexer's --rules-check-interval apply.
  static std::string rules_check_interval_refused(float seconds);
  // Whether stop() has run: the acceptor is closed and nothing re-arms.
  bool stopped() const {
    MX_DCHECK_RUN_ON(&owner_thread());
    return stopping_;
  }
  // The CRC-32 of the rules in use, as the generated constants carry it.
  std::string rules_fingerprint() const {
    MX_DCHECK_RUN_ON(&owner_thread());
    return rules_fingerprint_;
  }

  // For the soak tests: after every `every` routed messages, log the C heap
  // in use (mx::heap_in_use_bytes) as "memory: heap_in_use=<bytes>
  // messages=<count>". Zero, the default, logs nothing and costs nothing.
  void set_memory_log_every(unsigned int every) {
    MX_DCHECK_RUN_ON(&owner_thread());
    memory_log_every_ = every;
  }

  // Recording (Recording.proto): every peer event and delivery attempt, to
  // a file session and to the peers that tapped in. Off, the default, costs
  // nothing per message. A file session is opened by start_recording(), at
  // start for --record, or by a peer's RECORDING_CONTROL START once
  // --recording-dir names where such sessions go; a peer taps in with TAP
  // once --allow-tap is set. The rules fingerprint goes into the header
  // record that opens every recording session.
  void set_recording_dir(const std::string& dir) {
    MX_DCHECK_RUN_ON(&owner_thread());
    recording_dir_ = dir;
  }
  void set_allow_tap(bool allow) {
    MX_DCHECK_RUN_ON(&owner_thread());
    allow_tap_ = allow;
  }
  bool remote_recording_enabled() const {
    MX_DCHECK_RUN_ON(&owner_thread());
    return !recording_dir_.empty() || allow_tap_;
  }

  // Opens a file session on `path`, with the peers connected right now
  // written first, and returns true; false with `error` set when a session
  // is open or the file cannot be opened. `label` names the session in the
  // header and the status; `max_bytes` and `max_seconds` close it on their
  // own, 0 means never. While it is open, its buffer goes to the file
  // every RECORDING_FLUSH_INTERVAL.
  bool start_recording(const std::string& path, const std::string& label, unsigned int payload_limit,
                       std::uint64_t max_bytes, unsigned int max_seconds, std::string* error);
  // Closes the file session, if one is open, noting `reason` for the status.
  void stop_recording(const std::string& reason);
  bool recording() const {
    MX_DCHECK_RUN_ON(&owner_thread());
    return recorder_ != nullptr;
  }

  // --peers-file: written atomically at start(), and on every
  // registration and unregistration, one line per connected peer:
  // "<instance id> <peer type name> <peer type>". Empty, the default,
  // writes nothing.
  void set_peers_file(const std::string& path) {
    MX_DCHECK_RUN_ON(&owner_thread());
    peers_file_ = path;
  }
  // How many peer types the routing index holds: those some peer has
  // connected as, a type a rule only names never entering it. For tests;
  // on the io thread.
  std::size_t peer_types_indexed() const {
    MX_DCHECK_RUN_ON(&owner_thread());
    return connections_by_type_.size();
  }
  // How many connections are accepted and not ended yet, registered or
  // not. For tests; on the io thread.
  std::size_t accepted_count() const {
    MX_DCHECK_RUN_ON(&owner_thread());
    return accepted_.size();
  }

  // Port the acceptor is bound to; meaningful when constructed with port 0.
  unsigned short local_port() const {
    MX_DCHECK_RUN_ON(&owner_thread());
    return acceptor_.local_endpoint().port();
  }

  // Stops the multiplexer, so that the io loop drains and run() returns;
  // used on SIGTERM and SIGINT. The acceptor closes, and so does every
  // connection that has not sent its welcome; a welcome is refused from
  // then on. With `drain_seconds` above 0, every registered connection goes
  // on as before, read, routed and routed to, until what is queued for it
  // is written, and then ends the way a leaving client's does
  // (Connection::close_gracefully): this end of the stream goes out after
  // everything written, and what the peer still sends is read and dropped
  // until its end, CLOSE_READ_SECONDS at most. A connection still holding
  // messages after `drain_seconds`, a peer that does not read, is shut
  // down and its queue dropped. With 0, or called again while draining,
  // every connection is shut down at once and its queue dropped. The
  // log says what each stop dropped. `stopped` runs once every connection
  // has ended.
  void stop(float drain_seconds = 0, std::function<void()> stopped = nullptr);

  // ConnectionsManager hooks. A welcome after stop() is refused, and so is
  // one whose key filters KeyPatterns refuses; the filters of one that is
  // accepted are in effect before anything is routed. A connection that
  // ended leaves the set stop() closes, and a stop that was waiting for it
  // may be over. While stopping, what a connection ending dropped, from
  // its queue or as it read on, is counted for the stop's last line.
  void register_connection(Connection::pointer conn, const WelcomeMessage& welcome);
  void connection_closed(Connection* conn);
  void handle_orphaned_outgoing_messages(Connection*, Connection::MessagesBuffer& queue) {
    MX_DCHECK_RUN_ON(&owner_thread());
    if (stopping_) {
      stop_dropped_queued_ += queue.size();
    }
  }

  // Peers may not announce a reserved type (1 to 99), and must be in the
  // rules file; the exceptions are a rules controller, always accepted,
  // and a recording controller, accepted when remote recording is on.
  bool inline accept_peer_type(std::uint32_t peer_type) const {
    if (peer_type == RULES_CONTROLLER) {
      return true;
    }
    if (peer_type == RECORDING_CONTROLLER) {
      return remote_recording_enabled();
    }
    return peer_type > peers::MAX_MULTIPLEXER_SPECIAL_PEER_TYPE && Base::accept_peer_type(peer_type);
  }

  // The two controllers, which call in when they have something to ask:
  // passive whatever a rules file that names their types says, when they
  // register and at every reload.
  static bool controller(std::uint32_t peer_type) {
    return peer_type == RECORDING_CONTROLLER || peer_type == RULES_CONTROLLER;
  }

  // The peer's welcome was accepted: now send ours and arm the heartbeats,
  // and note the arrival for the recording and the peers file, with the
  // routing its welcome carried when that turns anything off. A
  // controller is passive (controller()).
  void after_connection_registration(Connection::pointer new_connection, const WelcomeMessage&) {
    MX_DCHECK_RUN_ON(&owner_thread());
    if (controller(new_connection->peer_type())) {
      new_connection->set_is_passive(true);
    }
    new_connection->start_rest();
    _emit_peer(PeerEvent::CONNECTED, new_connection->peer_id(), new_connection->peer_type());
    if (restricted(new_connection->routing())) {
      _emit_peer_routing(*new_connection);
    }
    _peers_changed();
  }

  // A registered peer's connection ended for routing; a tap it held ends
  // with it, and its key filters' patterns are given back. Its
  // DISCONNECTED is recorded when the connection closes
  // (connection_closed): after a failed write it reads on to the peer's
  // end, delivering what it reads, whose records come before it.
  void connection_unregistered(Connection* conn) {
    MX_DCHECK_RUN_ON(&owner_thread());
    departed_.insert(conn);
    _untap(conn);
    key_patterns_.remove_owner(conn->key_owner());
    conn->set_key_filters(google::protobuf::RepeatedPtrField<KeyFilter>(), KeyPatterns::NO_OWNER);
    _peers_changed();
  }

 private:
  void _start_accept() MX_RUN_ON(owner_thread_);
  void _handle_accept(Connection::pointer new_connection, const asio::error_code& error);
  // An accept that failed for want of a descriptor or memory is tried
  // again after ACCEPT_RETRY_SECONDS: no event says one was freed, and
  // trying again at once spins. Other failures, such as a peer that reset
  // before its accept, try again at once.
  static constexpr float ACCEPT_RETRY_SECONDS = 0.1;
  void _accept_later() MX_RUN_ON(owner_thread_);
  // stop() is over once every connection it waits for has ended: the
  // timers go, the counts and the recording are closed, and `stopped` runs.
  void _stop_if_done() MX_RUN_ON(owner_thread_);
  static void _on_drain_deadline(weak_pointer server, const asio::error_code& error);

  // Everything about the message being routed, passed down the _schedule
  // calls. Collects the delivery failures as they happen; at the end,
  // _handle_delivery_errors turns them into one DELIVERY_ERROR if any.
  struct MessageMetaHandler {
    MessageMetaHandler(const MessageMetaHandler&) = delete;
    MessageMetaHandler& operator=(const MessageMetaHandler&) = delete;
    MessageMetaHandler(const MultiplexerMessage& message, const Connection::pointer& connection,
                       const std::shared_ptr<const RawMessage>& raw_message)
        : msg(message), conn(connection), raw(raw_message) {}

    // Nobody of `type` received it under `rule`.
    void failed(const MultiplexerMessageDescription::RoutingRule& rule, std::uint32_t type);
    // The instance id `to` is not connected, or could not take it.
    void failed(std::uint64_t to);
    // The message type has no entry in the rules file.
    void unknown();
    // The message type has an entry but no routing rule, and no `to`.
    void unroutable();

   private:
    void __create_delivery_error_message(bool include_original_packet_in_report);

   public:
    std::unique_ptr<DeliveryError> delivery_error_message;
    // The caller's, as handle_message says, outliving the handler.
    const MultiplexerMessage& msg;
    const Connection::pointer& conn;
    const std::shared_ptr<const RawMessage>& raw;
    // A BACKEND_FOR_PACKET_SEARCH, forwarded to every peer of the type
    // that takes rule-routed requests (Routing.any), not every one.
    bool search = false;
    // A keyed message routed by the rules of a type some peer filters
    // (KeyPatterns::match): `key_type` is the type the filters are for, the
    // message's own or, for a search, the searched-for one, and
    // `key_evaluation` the match of its key against them, which
    // KeyPatterns::takes() answers for each peer. Unset, the filters do not
    // apply, as for a message without a key.
    bool keyed = false;
    std::uint32_t key_type = 0;
    std::uint64_t key_evaluation = 0;
  };

  // Whether a peer takes a keyed message (MessageMetaHandler::keyed): a
  // key filter is a filter on what its peer receives, so the peer takes
  // the message when its filter for the type takes the key, and when it has
  // none for the type, which is every key. Routing passes over a peer that
  // does not, and treats the rest as it treats every peer.
  bool _takes_key(const Connection& connection, const MessageMetaHandler& meta_handler) const MX_RUN_ON(owner_thread_) {
    return key_patterns_.takes(connection.key_owner(), meta_handler.key_type, meta_handler.key_evaluation);
  }
  // A keyed message's MessageMetaHandler fields for the filters of `type`;
  // nothing for a type no peer filters.
  void _match_key(MessageMetaHandler& meta_handler, std::uint32_t type) MX_RUN_ON(owner_thread_);

  // The per-message path; see the file comment for the order of the cases.
  void _handle_message(const Connection::pointer& conn, const MultiplexerMessage& msg,
                       const std::shared_ptr<const RawMessage>& raw) MX_RUN_ON(owner_thread_);
  void _handle_delivery_errors(MessageMetaHandler& meta_handler) MX_RUN_ON(owner_thread_);
  bool _handle_message_inlined_rules(MessageMetaHandler& meta_handler) MX_RUN_ON(owner_thread_);
  bool _handle_meta_message(MessageMetaHandler& meta_handler) MX_RUN_ON(owner_thread_);

  // Applying rules. The overloads narrow from a message description to its
  // list of rules to one rule to the connections of one peer type; each
  // returns how many connections the frame was queued on. Zero, with
  // report_delivery_error set, records a failure in the meta handler.
  unsigned int _schedule(MessageMetaHandler& meta_handler, const MultiplexerMessageDescription& desc)
      MX_RUN_ON(owner_thread_);
  unsigned int _schedule(MessageMetaHandler& meta_handler,
                         const ::google::protobuf::RepeatedPtrField<MultiplexerMessageDescription::RoutingRule>& rules)
      MX_RUN_ON(owner_thread_);
  unsigned int _schedule(MessageMetaHandler& meta_handler, const MultiplexerMessageDescription::RoutingRule& rule)
      MX_RUN_ON(owner_thread_);
  unsigned int _schedule(MessageMetaHandler& meta_handler, ConnectionsList& connections,
                         const MultiplexerMessageDescription::RoutingRule& rule) MX_RUN_ON(owner_thread_);
  unsigned int _schedule(MessageMetaHandler& meta_handler, ConnectionsList& connections,
                         const MultiplexerMessageDescription::RoutingRule& rule, std::uint32_t peer_type)
      MX_RUN_ON(owner_thread_);

  // whom: ALL and whom: ANY over one peer type's connections, to the peers
  // whose Routing takes the path; the last resorts among the others get
  // the message when no such peer could. A keyed message skips the peers
  // whose filter does not take its key (_takes_key).
  unsigned int send_to_all(MessageMetaHandler& meta_handler, ConnectionsList& connections) MX_RUN_ON(owner_thread_);
  unsigned int send_to_one(MessageMetaHandler& meta_handler, ConnectionsList& connections) MX_RUN_ON(owner_thread_);

  // Why a rule queued a message nowhere: no living peer of the type, no
  // filter of one that takes its key, routing off on every one that does,
  // or the queue full on every one that takes it. `by_any` names the
  // Routing flag the rule tests. Only on that failure path.
  // _unrouted_text() is the log line for it.
  enum Unrouted : unsigned int { NONE_PRESENT, ROUTING_OFF, ALL_FULL, KEY_NOT_TAKEN };
  Unrouted _unrouted(const MessageMetaHandler& meta_handler, const MultiplexerMessageDescription::RoutingRule& rule,
                     bool by_any) const MX_RUN_ON(owner_thread_);
  std::string _unrouted_text(Unrouted why, std::uint32_t peer_type, const MessageMetaHandler& meta_handler) const;
  // How a recording says it: NO_RECIPIENT, NOT_ACCEPTED, QUEUE_FULL or KEY_NOT_TAKEN.
  static RoutedMessage::Disposition _unrouted_disposition(Unrouted why);

  // What drops_ tells apart, besides the four above: the kinds of line
  // about a message that went nowhere (LogSummary::Kind::reason).
  enum Dropped : unsigned int {
    NOT_CONNECTED = KEY_NOT_TAKEN + 1,  // `to` names no connected peer
    QUEUE_FULL,                         // one peer's copy, to it or under whom ALL
    UNKNOWN_TYPE,
    NO_RULE,
    UNKNOWN_PROTOCOL_TYPE,
    BAD_SEARCH,
    ACCEPT_FAILED,  // not a message: the accept loop out of descriptors
    KEY_TOO_LONG,   // a key over MAX_KEY_SIZE
  };
  // A peer's copy its full queue refused; the line names the peer.
  void _dropped_queue_full(const MessageMetaHandler& meta_handler, const Connection& connection)
      MX_RUN_ON(owner_thread_);

  // Recording. A record is built once per delivery attempt, one for each
  // recipient of a message, and goes to the file session and to every
  // tap; nothing is built while neither exists. Of the payload it copies
  // what the sink that keeps the most keeps (_payload_kept), not more.
  bool _recording() const MX_RUN_ON(owner_thread_) { return recorder_ || !taps_.empty(); }
  void _record(const MessageMetaHandler& meta_handler, std::uint64_t recipient, std::uint32_t recipient_type,
               RoutedMessage::Disposition disposition, bool error_reported) MX_RUN_ON(owner_thread_) {
    if (!_recording()) {
      return;
    }
    // A message of the multiplexer's own, a DELIVERY_ERROR, is routed
    // through the connection of the peer it answers; it is still ours.
    const std::uint32_t from_peer_type =
        meta_handler.msg.sender() == instance_id_ ? peers::MULTIPLEXER : meta_handler.conn->peer_type();
    Record record;
    recording::fill_routed(record, meta_handler.msg, from_peer_type, recipient, recipient_type, disposition,
                           error_reported, _payload_kept());
    _emit(record);
  }
  // The most of a payload a sink keeps: the file session's limit or a
  // tap's, the largest, 0 when one keeps all of it.
  unsigned int _payload_kept() const MX_RUN_ON(owner_thread_);
  void _emit_peer(PeerEvent::Kind kind, std::uint64_t peer_id, std::uint32_t peer_type) MX_RUN_ON(owner_thread_);
  void _emit_peer_routing(const Connection& conn) MX_RUN_ON(owner_thread_);
  // Stamps `record`, writes it to the file session, closing the session at
  // its cap, and streams it to every tap. A routed message's payload is cut
  // in place for each sink's limit, the longest first, so that it is never
  // copied, and the record serialized once for each length the taps keep.
  void _emit(Record& record) MX_RUN_ON(owner_thread_);
  // `record` serialized as the taps get it, stamped with this multiplexer's
  // id, cut to fit a tap's frame within MAX_MESSAGE_SIZE when it would not:
  // see the definition.
  std::string _tap_record(Record& record) MX_RUN_ON(owner_thread_);
  MultiplexerMessage _tap_frame(std::uint64_t peer_id) MX_RUN_ON(owner_thread_);

  // A peer receiving every record as RECORDING_RECORD messages.
  struct Tap {
    Connection::weak_pointer conn;
    std::uint64_t peer_id;
    unsigned int payload_limit;
    std::uint64_t dropped;  // records its full outgoing queue lost
  };
  typedef std::vector<Tap> Taps;
  // One record, `serialized`, to `tap` as a RECORDING_RECORD message; one
  // too big for MAX_MESSAGE_SIZE even so, or that its full outgoing queue
  // refuses, is counted as dropped.
  void _stream(Tap& tap, const std::string& serialized) MX_RUN_ON(owner_thread_);
  Taps::iterator _find_tap(const Connection* conn) MX_RUN_ON(owner_thread_);
  void _untap(const Connection* conn) MX_RUN_ON(owner_thread_);

  // What a file session was, kept after it closed for the status.
  struct Session {
    std::string label;
    std::string path;
    std::uint64_t started_us = 0;
    std::uint64_t max_bytes = 0;
    std::uint64_t bytes = 0;
    std::uint64_t records = 0;
    std::string stopped;  // why it ended; empty while open or before the first
  };

  // RECORDING_CONTROL from a peer: carry it out and answer RECORDING_STATUS.
  void _handle_recording_control(MessageMetaHandler& meta_handler) MX_RUN_ON(owner_thread_);
  void _fill_status(RecordingStatus& status, const Connection* requester) MX_RUN_ON(owner_thread_);
  // Queues `payload` as a message of `type` on the sender's connection,
  // referencing the message being handled.
  void _reply(const MessageMetaHandler& meta_handler, std::uint32_t type, const ::google::protobuf::Message& payload)
      MX_RUN_ON(owner_thread_);
  // The frame _reply sends, its id `id`.
  MultiplexerMessage _answer(const MessageMetaHandler& meta_handler, std::uint32_t type,
                             const ::google::protobuf::Message& payload, std::uint64_t id) const
      MX_RUN_ON(owner_thread_);
  // Whether the PEER_STATUS that would answer `control` once applied fits
  // a frame: measured on that frame, its id the largest, which no id the
  // answer gets makes longer; `control` is as it was after.
  bool _answer_fits(const MessageMetaHandler& meta_handler, PeerControl& control) const MX_RUN_ON(owner_thread_);
  // The deadline of file session number `session`: stops it unless it
  // ended already, its completion queued before a stop's cancel came.
  static void _on_session_deadline(weak_pointer server, std::uint64_t session, const asio::error_code& error);
  // While a file session is open, what its buffer holds goes to the file
  // every RECORDING_FLUSH_INTERVAL, on the io thread that writes the
  // records: the file is at most about that much behind, and a multiplexer
  // that dies loses at most about that much. Never per record, which would
  // be a system call each. start_recording() arms the timer and
  // stop_recording() cancels it: nothing ticks without a session.
  static constexpr std::chrono::seconds RECORDING_FLUSH_INTERVAL{1};
  void _arm_recording_flush() MX_RUN_ON(owner_thread_);
  static void _on_recording_flush(weak_pointer server, const asio::error_code& error);

  // RULES_CONTROL from a peer: reload if asked, answer RULES_STATUS.
  void _handle_rules_control(MessageMetaHandler& meta_handler) MX_RUN_ON(owner_thread_);
  // PEER_CONTROL from a peer: its routing and key filters from now on,
  // answered with PEER_STATUS once in effect, or with the error that left
  // both as they were.
  void _handle_peer_control(MessageMetaHandler& meta_handler) MX_RUN_ON(owner_thread_);
  // A message type's name for a log line: from the rules, or its number.
  std::string _type_name(std::uint32_t type) const;
  void _fill_rules_status(RulesStatus& status) MX_RUN_ON(owner_thread_);
  // The file's bytes, or false with `error` set: no file named,
  // unreadable, or empty (nothing at all, or caught between a truncate
  // and the write that follows).
  bool _read_rules_file(std::string* text, std::string* error) MX_RUN_ON(owner_thread_);
  // The rest of load_rules(): `text` compared with the rules in use,
  // parsed apart and put in use when it differs and is good.
  RulesLoad _apply_rules_text(const std::string& text, std::string* error) MX_RUN_ON(owner_thread_);
  // The file could not be put in use: `why` goes to the caller's `error`
  // and is kept for the status, logged when it is news.
  RulesLoad _rules_failed(const std::string& why, std::string* error) MX_RUN_ON(owner_thread_);
  // The rules check timer: _check_rules_file() every rules_check_interval_.
  void _arm_rules_check() MX_RUN_ON(owner_thread_);
  static void _on_rules_check(weak_pointer server, const asio::error_code& error);
  void _check_rules_file() MX_RUN_ON(owner_thread_);

  // The peers file follows the connections at most PEERS_FILE_INTERVAL
  // behind: a change arms one write, at once when the last was longer ago,
  // and the write covers every arrival and departure until it runs, N at
  // once in a reconnect storm or a stop(), where each rewrote the whole
  // file, O(N^2) on the io thread. A stop that ends writes what is pending
  // itself.
  static constexpr std::chrono::milliseconds PEERS_FILE_INTERVAL{10};
  void _peers_changed() MX_RUN_ON(owner_thread_);
  // The peers file, if configured: every living registered peer.
  void _write_peers_file() MX_RUN_ON(owner_thread_);
  // A write of the peers file that failed, `why` with its reason: logged
  // at ERROR when it is news, the first failure or another reason than the
  // last one's, and counted for the line that says the file is written
  // again. A file that stays unwritable while peers come and go logged
  // the same line, with no reason, at every arrival and departure.
  void _peers_file_failed(const std::string& why) MX_RUN_ON(owner_thread_);
  // A peer type's name for the peers file: from the rules, or the reserved name.
  std::string _peer_name(std::uint32_t peer_type) const;

 private:
  asio::ip::tcp::acceptor acceptor_ MX_GUARDED_BY(owner_thread_);
  std::shared_ptr<const RawMessage> welcome_message_ MX_GUARDED_BY(owner_thread_);
  asio::io_service& io_service_;
  unsigned int memory_log_every_ MX_GUARDED_BY(owner_thread_) = 0;
  unsigned long routed_messages_ MX_GUARDED_BY(owner_thread_) = 0;
  std::shared_ptr<MultiplexerMessage> parse_target_ MX_GUARDED_BY(owner_thread_);  // see parse_target()
  std::string rules_file_ MX_GUARDED_BY(owner_thread_);
  std::string rules_fingerprint_ MX_GUARDED_BY(owner_thread_);
  std::uint64_t rules_loaded_us_ MX_GUARDED_BY(owner_thread_) = 0;
  std::string rules_last_error_ MX_GUARDED_BY(owner_thread_);  // why the file on disk is not in use; empty while it is
  std::string rules_failed_fingerprint_
      MX_GUARDED_BY(owner_thread_);  // the content that failed last, not parsed again while it stays
  std::string rules_pending_fingerprint_
      MX_GUARDED_BY(owner_thread_);  // a change the timer saw once; applied when seen again
  float rules_check_interval_ MX_GUARDED_BY(owner_thread_) = 0;
  asio::steady_timer rules_timer_ MX_GUARDED_BY(owner_thread_);
  std::string recording_dir_ MX_GUARDED_BY(owner_thread_);
  bool allow_tap_ MX_GUARDED_BY(owner_thread_) = false;
  std::unique_ptr<Recorder> recorder_ MX_GUARDED_BY(owner_thread_);
  Session session_ MX_GUARDED_BY(owner_thread_);
  std::uint64_t sessions_ MX_GUARDED_BY(owner_thread_) = 0;  // file sessions started, the last one's number
  asio::steady_timer session_timer_ MX_GUARDED_BY(owner_thread_);
  asio::steady_timer flush_timer_ MX_GUARDED_BY(owner_thread_);  // the session's flush, armed while it is open
  Taps taps_ MX_GUARDED_BY(owner_thread_);
  std::string peers_file_ MX_GUARDED_BY(owner_thread_);
  bool peers_write_pending_ MX_GUARDED_BY(owner_thread_) = false;  // a write of the peers file is armed on peers_timer_
  std::chrono::steady_clock::time_point peers_written_at_ MX_GUARDED_BY(owner_thread_);
  std::string peers_file_error_ MX_GUARDED_BY(owner_thread_);  // the last failed write's line, empty after a good one
  std::uint64_t peers_file_failures_ MX_GUARDED_BY(owner_thread_) = 0;  // failed writes since the last good one
  asio::steady_timer peers_timer_ MX_GUARDED_BY(owner_thread_);

  // Every connection accepted and not yet ended, registered or not, for
  // stop(); keyed by address, which connection_closed() erases.
  std::map<const Connection*, Connection::weak_pointer> accepted_ MX_GUARDED_BY(owner_thread_);
  // Unregistered, their DISCONNECTED recorded when they close.
  std::set<const Connection*> departed_ MX_GUARDED_BY(owner_thread_);
  asio::steady_timer accept_timer_ MX_GUARDED_BY(owner_thread_);
  // stop(): whether it ran, its deadline for the drain, its callback, and
  // what it dropped, said in its last line.
  bool stopping_ MX_GUARDED_BY(owner_thread_) = false;
  bool stop_done_ MX_GUARDED_BY(owner_thread_) = false;
  std::chrono::steady_clock::time_point stop_started_ MX_GUARDED_BY(owner_thread_);
  asio::steady_timer drain_timer_ MX_GUARDED_BY(owner_thread_);
  std::function<void()> on_stopped_ MX_GUARDED_BY(owner_thread_);
  std::uint64_t stop_dropped_queued_ MX_GUARDED_BY(owner_thread_) = 0;
  std::uint64_t stop_dropped_read_ MX_GUARDED_BY(owner_thread_) = 0;
  // The lines about messages that went nowhere: see LogSummary.
  LogSummary drops_ MX_GUARDED_BY(owner_thread_);
  // The patterns of every connected peer's key filters, by message type.
  KeyPatterns key_patterns_ MX_GUARDED_BY(owner_thread_);
};  // class Server

};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_SERVER_H_
