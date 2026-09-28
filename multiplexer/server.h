// The multiplexer itself: accepts connections and routes every message it
// receives according to the rules file, to one peer, to all peers of a type,
// or to the instance id named in the message.
//
// Server is a ConnectionsManager whose connections are peers of any type.
// It has one thread and one io_service; all the work per message is
// _handle_message in server.cc. A frame is forwarded as the RawMessage it
// arrived in, never re-serialized: routing costs a few map lookups and the
// writes. Everything is in one translation unit, so the compiler inlines
// the per-message path as it did when it lived in this header.
//
// The routing order, first match wins: a `to` field; override_rrules
// carried by the message; protocol (meta) types below 100; the rules file's
// entries for the type. Nobody to deliver to produces a DELIVERY_ERROR back
// to the sender when the rule asks for it. A peer's Routing (its welcome,
// then PEER_CONTROL) says which rule-routed paths reach it; `to` always
// does. docs/wire_format.md describes the same order from the outside.
#ifndef MX_MULTIPLEXER_SERVER_H_
#define MX_MULTIPLEXER_SERVER_H_

#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/steady_timer.hpp>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "lib/functors.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/config.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/defaults.h"
#include "multiplexer/io/connection.h"
#include "multiplexer/log_summary.h"
#include "multiplexer/multiplexer.constants.h" /* generated */
#include "multiplexer/recorder.h"

namespace multiplexer {

class Server;  // forward

// The multiplexer's queues hold bare frames and nobody tracks their fate:
// scheduling a frame answers true or false, and that is all a sender learns.
template <>
struct ConnectionsManagerTraits<Server> : public DefaultConnectionsManagerTraits {
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
    if (!welcome_message_) {
      welcome_message_ = create_welcome_message(multiplexer::peers::MULTIPLEXER);
    }
    return welcome_message_;
  }

  // Entry point for every routed message, called by a Connection once the
  // frame is parsed. Drops messages that claim to come from this multiplexer
  // and hands the rest to _handle_message.
  void handle_message(Connection::pointer conn, std::shared_ptr<const RawMessage> raw,
                      std::shared_ptr<MultiplexerMessage> msg);

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
  void set_rules_file(const std::string& path) { rules_file_ = path; }
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
  bool stopped() const { return stopping_; }
  // The CRC-32 of the rules in use, as the generated constants carry it.
  const std::string& rules_fingerprint() const { return rules_fingerprint_; }

  // For the soak tests: after every `every` routed messages, log the C heap
  // in use (mx::heap_in_use_bytes) as "memory: heap_in_use=<bytes>
  // messages=<count>". Zero, the default, logs nothing and costs nothing.
  void set_memory_log_every(unsigned int every) { memory_log_every_ = every; }

  // Recording (Recording.proto): every peer event and delivery attempt, to
  // a file session and to the peers that tapped in. Off, the default, costs
  // nothing per message. A file session is opened by start_recording(), at
  // start for --record, or by a peer's RECORDING_CONTROL START once
  // --recording-dir names where such sessions go; a peer taps in with TAP
  // once --allow-tap is set. The rules fingerprint goes into the header
  // record that opens every recording session.
  void set_recording_dir(const std::string& dir) { recording_dir_ = dir; }
  void set_allow_tap(bool allow) { allow_tap_ = allow; }
  bool remote_recording_enabled() const { return !recording_dir_.empty() || allow_tap_; }

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
  bool recording() const { return recorder_ != nullptr; }

  // --peers-file: rewritten atomically on every registration and
  // unregistration, one line per connected peer: "<instance id> <peer type
  // name> <peer type>". Empty, the default, writes nothing.
  void set_peers_file(const std::string& path) { peers_file_ = path; }
  // How many peer types the routing index holds: those some peer has
  // connected as, a type a rule only names never entering it. For tests;
  // on the io thread.
  std::size_t peer_types_indexed() const { return connections_by_type_.size(); }

  // Port the acceptor is bound to; meaningful when constructed with port 0.
  unsigned short local_port() const { return acceptor_.local_endpoint().port(); }

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

  // ConnectionsManager hooks. A welcome after stop() is refused; a
  // connection that ended leaves the set stop() closes, and a stop that
  // was waiting for it may be over. While stopping, what a connection
  // ending dropped, from its queue or as it read on, is counted for the
  // stop's last line.
  void register_connection(Connection::pointer conn, const WelcomeMessage& welcome) {
    if (stopping_) {
      conn->shutdown();
      return;
    }
    Base::register_connection(conn, welcome);
  }
  void connection_closed(Connection* conn);
  void handle_orphaned_outgoing_messages(Connection::MessagesBuffer& queue) {
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

  // A registered peer's connection ended; a tap it held ends with it.
  void connection_unregistered(Connection* conn) {
    _emit_peer(PeerEvent::DISCONNECTED, conn->peer_id(), conn->peer_type());
    _untap(conn);
    _peers_changed();
  }

 private:
  void _start_accept();
  void _handle_accept(Connection::pointer new_connection, const asio::error_code& error);
  // An accept that failed for want of a descriptor or memory is tried
  // again after ACCEPT_RETRY_SECONDS: no event says one was freed, and
  // trying again at once spins. Other failures, such as a peer that reset
  // before its accept, try again at once.
  static constexpr float ACCEPT_RETRY_SECONDS = 0.1;
  void _accept_later();
  // stop() is over once every connection it waits for has ended: the
  // timers go, the counts and the recording are closed, and `stopped` runs.
  void _stop_if_done();
  static void _on_drain_deadline(weak_pointer server, const asio::error_code& error);

  // Everything about the message being routed, passed down the _schedule
  // calls. Collects the delivery failures as they happen; at the end,
  // _handle_delivery_errors turns them into one DELIVERY_ERROR if any.
  struct MessageMetaHandler {
    MessageMetaHandler(const MessageMetaHandler&) = delete;
    MessageMetaHandler& operator=(const MessageMetaHandler&) = delete;
    MessageMetaHandler(const MultiplexerMessage& message, Connection::pointer connection,
                       std::shared_ptr<const RawMessage> raw_message)
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
    const MultiplexerMessage& msg;
    const Connection::pointer conn;
    const std::shared_ptr<const RawMessage> raw;
    // A BACKEND_FOR_PACKET_SEARCH, forwarded to every peer of the type
    // that takes rule-routed requests (Routing.any), not every one.
    bool search = false;
  };

  // The per-message path; see the file comment for the order of the cases.
  void _handle_message(Connection::pointer conn, const MultiplexerMessage& msg, std::shared_ptr<const RawMessage> raw);
  void _handle_delivery_errors(MessageMetaHandler& meta_handler);
  bool _handle_message_inlined_rules(MessageMetaHandler& meta_handler);
  bool _handle_meta_message(MessageMetaHandler& meta_handler);

  // Applying rules. The overloads narrow from a message description to its
  // list of rules to one rule to the connections of one peer type; each
  // returns how many connections the frame was queued on. Zero, with
  // report_delivery_error set, records a failure in the meta handler.
  unsigned int _schedule(MessageMetaHandler& meta_handler, const MultiplexerMessageDescription& desc);
  unsigned int _schedule(MessageMetaHandler& meta_handler,
                         const ::google::protobuf::RepeatedPtrField<MultiplexerMessageDescription::RoutingRule>& rules);
  unsigned int _schedule(MessageMetaHandler& meta_handler, const MultiplexerMessageDescription::RoutingRule& rule);
  unsigned int _schedule(MessageMetaHandler& meta_handler, ConnectionsList& connections,
                         const MultiplexerMessageDescription::RoutingRule& rule);
  unsigned int _schedule(MessageMetaHandler& meta_handler, ConnectionsList& connections,
                         const MultiplexerMessageDescription::RoutingRule& rule, std::uint32_t peer_type);

  // whom: ALL and whom: ANY over one peer type's connections, to the peers
  // whose Routing takes the path; the last resorts among the others get
  // the message when no such peer could.
  unsigned int send_to_all(MessageMetaHandler& meta_handler, ConnectionsList& connections);
  unsigned int send_to_one(MessageMetaHandler& meta_handler, ConnectionsList& connections);

  // Why a rule queued a message nowhere: no living peer of the type,
  // routing off on every one, or the queue full on every one that takes it.
  // `by_any` names the Routing flag the rule tests. Only on that failure
  // path. _unrouted_text() is the log line for it.
  enum Unrouted : unsigned int { NONE_PRESENT, ROUTING_OFF, ALL_FULL };
  Unrouted _unrouted(const MultiplexerMessageDescription::RoutingRule& rule, bool by_any) const;
  std::string _unrouted_text(Unrouted why, std::uint32_t peer_type) const;

  // What drops_ tells apart, besides the three above: the kinds of line
  // about a message that went nowhere (LogSummary::Kind::reason).
  enum Dropped : unsigned int {
    NOT_CONNECTED = ALL_FULL + 1,  // `to` names no connected peer
    QUEUE_FULL,                    // one peer's copy, to it or under whom ALL
    UNKNOWN_TYPE,
    NO_RULE,
    UNKNOWN_PROTOCOL_TYPE,
    BAD_SEARCH,
    ACCEPT_FAILED,  // not a message: the accept loop out of descriptors
  };
  // A peer's copy its full queue refused; the line names the peer.
  void _dropped_queue_full(const MessageMetaHandler& meta_handler, const Connection& connection);

  // Recording. A record is built once and goes to the file session and to
  // every tap; nothing is built while neither exists.
  void _record(const MessageMetaHandler& meta_handler, std::uint64_t recipient, std::uint32_t recipient_type,
               RoutedMessage::Disposition disposition, bool error_reported) {
    if (!recorder_ && taps_.empty()) {
      return;
    }
    // A message of the multiplexer's own, a DELIVERY_ERROR, is routed
    // through the connection of the peer it answers; it is still ours.
    const std::uint32_t from_peer_type =
        meta_handler.msg.from() == instance_id_ ? peers::MULTIPLEXER : meta_handler.conn->peer_type();
    Record record;
    recording::fill_routed(record, meta_handler.msg, from_peer_type, recipient, recipient_type, disposition,
                           error_reported);
    _emit(record);
  }
  void _emit_peer(PeerEvent::Kind kind, std::uint64_t peer_id, std::uint32_t peer_type);
  void _emit_peer_routing(const Connection& conn);
  // Stamps `record`, writes it to the file session, closing the session at
  // its cap, and streams it to every tap.
  void _emit(Record& record);

  // A peer receiving every record as RECORDING_RECORD messages.
  struct Tap {
    Connection::weak_pointer conn;
    std::uint64_t peer_id;
    unsigned int payload_limit;
    std::uint64_t dropped;  // records its full outgoing queue lost
  };
  typedef std::vector<Tap> Taps;
  Taps::iterator _find_tap(const Connection* conn);
  void _untap(const Connection* conn);

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
  void _handle_recording_control(MessageMetaHandler& meta_handler);
  void _fill_status(RecordingStatus& status, const Connection* requester);
  // Queues `payload` as a message of `type` on the sender's connection,
  // referencing the message being handled.
  void _reply(const MessageMetaHandler& meta_handler, std::uint32_t type, const ::google::protobuf::Message& payload);
  static void _on_session_deadline(weak_pointer server, const asio::error_code& error);
  // While a file session is open, what its buffer holds goes to the file
  // every RECORDING_FLUSH_INTERVAL, on the io thread that writes the
  // records: the file is at most about that much behind, and a multiplexer
  // that dies loses at most about that much. Never per record, which would
  // be a system call each. start_recording() arms the timer and
  // stop_recording() cancels it: nothing ticks without a session.
  static constexpr std::chrono::seconds RECORDING_FLUSH_INTERVAL{1};
  void _arm_recording_flush();
  static void _on_recording_flush(weak_pointer server, const asio::error_code& error);

  // RULES_CONTROL from a peer: reload if asked, answer RULES_STATUS.
  void _handle_rules_control(MessageMetaHandler& meta_handler);
  // PEER_CONTROL from a peer: its routing from now on, answered with
  // PEER_STATUS once in effect.
  void _handle_peer_control(MessageMetaHandler& meta_handler);
  void _fill_rules_status(RulesStatus& status);
  // The file's bytes, or false with `error` set: no file named,
  // unreadable, or empty (nothing at all, or caught between a truncate
  // and the write that follows).
  bool _read_rules_file(std::string* text, std::string* error);
  // The rest of load_rules(): `text` compared with the rules in use,
  // parsed apart and put in use when it differs and is good.
  RulesLoad _apply_rules_text(const std::string& text, std::string* error);
  // The file could not be put in use: `why` goes to the caller's `error`
  // and is kept for the status, logged when it is news.
  RulesLoad _rules_failed(const std::string& why, std::string* error);
  // The rules check timer: _check_rules_file() every rules_check_interval_.
  void _arm_rules_check();
  static void _on_rules_check(weak_pointer server, const asio::error_code& error);
  void _check_rules_file();

  // The peers file follows the connections at most PEERS_FILE_INTERVAL
  // behind: a change arms one write, at once when the last was longer ago,
  // and the write covers every arrival and departure until it runs, N at
  // once in a reconnect storm or a stop(), where each rewrote the whole
  // file, O(N^2) on the io thread. A stop that ends writes what is pending
  // itself.
  static constexpr std::chrono::milliseconds PEERS_FILE_INTERVAL{10};
  void _peers_changed();
  // The peers file, if configured: every living registered peer.
  void _write_peers_file();
  // A peer type's name for the peers file: from the rules, or the reserved name.
  std::string _peer_name(std::uint32_t peer_type) const;

 private:
  asio::ip::tcp::acceptor acceptor_;
  std::shared_ptr<const RawMessage> welcome_message_;
  asio::io_service& io_service_;
  unsigned int memory_log_every_ = 0;
  unsigned long routed_messages_ = 0;
  std::string rules_file_;
  std::string rules_fingerprint_;
  std::uint64_t rules_loaded_us_ = 0;
  std::string rules_last_error_;           // why the file on disk is not in use; empty while it is
  std::string rules_failed_fingerprint_;   // the content that failed last, not parsed again while it stays
  std::string rules_pending_fingerprint_;  // a change the timer saw once; applied when seen again
  float rules_check_interval_ = 0;
  asio::steady_timer rules_timer_;
  std::string recording_dir_;
  bool allow_tap_ = false;
  std::unique_ptr<Recorder> recorder_;
  Session session_;
  asio::steady_timer session_timer_;
  asio::steady_timer flush_timer_;  // the session's flush, armed while it is open
  Taps taps_;
  std::string peers_file_;
  bool peers_write_pending_ = false;  // a write of the peers file is armed on peers_timer_
  std::chrono::steady_clock::time_point peers_written_at_;
  asio::steady_timer peers_timer_;

  // Every connection accepted and not yet ended, registered or not, for
  // stop(); keyed by address, which connection_closed() erases.
  std::map<const Connection*, Connection::weak_pointer> accepted_;
  asio::steady_timer accept_timer_;
  // stop(): whether it ran, its deadline for the drain, its callback, and
  // what it dropped, said in its last line.
  bool stopping_ = false;
  bool stop_done_ = false;
  std::chrono::steady_clock::time_point stop_started_;
  asio::steady_timer drain_timer_;
  std::function<void()> on_stopped_;
  std::uint64_t stop_dropped_queued_ = 0;
  std::uint64_t stop_dropped_read_ = 0;
  // The lines about messages that went nowhere: see LogSummary.
  LogSummary drops_;
};  // class Server

};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_SERVER_H_
