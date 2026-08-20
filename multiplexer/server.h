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
// to the sender when the rule asks for it. docs/wire_format.md describes the
// same order from the outside.
#ifndef MX_MULTIPLEXER_SERVER_H_
#define MX_MULTIPLEXER_SERVER_H_

#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>

#include <memory>
#include <string>
#include <vector>

#include "lib/functors.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/config.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/defaults.h"
#include "multiplexer/io/connection.h"
#include "multiplexer/multiplexer.constants.h" /* generated */
#include "multiplexer/recorder.h"

namespace multiplexer {

class Server; // forward

// The multiplexer's queues hold bare frames and nobody tracks their fate:
// scheduling a frame answers true or false, and that is all a sender learns.
template <> struct ConnectionsManagerTraits<Server> : public DefaultConnectionsManagerTraits {

  struct MessagesBufferTraits : public DefaultConnectionsManagerTraits::MessagesBufferTraits {
    typedef boost::shared_ptr<const RawMessage> value_type;
    typedef mx::ReferencingFunctor<value_type> ToRawMessagePointerConverter;
    typedef mx::ReferencingFunctor<value_type> ToBufferRepresentationConverter;

    typedef mx::ConstructingFunctor<bool, value_type> SchedulingResultFunctor;
  };

  typedef multiplexer::Connection<Server> Connection;
};

// See the file comment. Created with Create(), started with start(),
// driven by io_service.run() in mxcontrol/start_multiplexer_server.cc.
class Server : public ConnectionsManager<Server>, public boost::enable_shared_from_this<Server> {

private:
  Server(boost::asio::io_service &io_service, const std::string &host, unsigned short port);

public:
  typedef ConnectionsManager<Server> Base;
  typedef boost::shared_ptr<Server> pointer;
  typedef boost::weak_ptr<Server> weak_pointer;
  static pointer Create(boost::asio::io_service &io_service, const std::string &host, unsigned short port) {
    return pointer(new Server(io_service, host, port));
  }

  // ConnectionsManager interface
  boost::shared_ptr<const RawMessage> get_welcome_message() {
    if (!welcome_message_) {
      welcome_message_ = create_welcome_message(multiplexer::peers::MULTIPLEXER);
    }
    return welcome_message_;
  }

  // Entry point for every routed message, called by a Connection once the
  // frame is parsed. Drops messages that claim to come from this multiplexer
  // and hands the rest to _handle_message.
  void handle_message(Connection::pointer conn, boost::shared_ptr<const RawMessage> raw,
                      boost::shared_ptr<MultiplexerMessage> msg);

  // Starts accepting connections.
  void start();

  // For the soak tests: after every `every` routed messages, log the C heap
  // in use (mx::heap_in_use_bytes) as "memory: heap_in_use=<bytes>
  // messages=<count>". Zero, the default, logs nothing and costs nothing.
  void set_memory_log_every(unsigned int every) { memory_log_every_ = every; }

  // Recording (Recording.proto): every peer event and delivery attempt, to
  // a file session and to the peers that tapped in. Off, the default, costs
  // nothing per message. A file session is opened by start_recording(), at
  // start for --record, or by a peer's RECORDING_CONTROL START once
  // --recording-dir names where such sessions go; a peer taps in with TAP
  // once --allow-tap is set. The rules file's SHA-1 goes into every header.
  void set_rules_sha1(const std::string &sha1) { rules_sha1_ = sha1; }
  void set_recording_dir(const std::string &dir) { recording_dir_ = dir; }
  void set_allow_tap(bool allow) { allow_tap_ = allow; }
  bool remote_recording_enabled() const { return !recording_dir_.empty() || allow_tap_; }

  // Opens a file session on `path`, with the peers connected right now
  // written first, and returns true; false with `error` set when a session
  // is open or the file cannot be opened. `label` names the session in the
  // header and the status; `max_bytes` and `max_seconds` close it on their
  // own, 0 means never.
  bool start_recording(const std::string &path, const std::string &label, unsigned int payload_limit,
                       boost::uint64_t max_bytes, unsigned int max_seconds, std::string *error);
  // Closes the file session, if one is open, noting `reason` for the status.
  void stop_recording(const std::string &reason);
  bool recording() const { return recorder_ != nullptr; }

  // --peers-file: rewritten atomically on every registration and
  // unregistration, one line per connected peer: "<instance id> <peer type
  // name> <peer type>". Empty, the default, writes nothing.
  void set_peers_file(const std::string &path) { peers_file_ = path; }

  // Port the acceptor is bound to; meaningful when constructed with port 0.
  unsigned short local_port() const { return acceptor_.local_endpoint().port(); }

  // Closes the acceptor and shuts every connection down, so that the io loop
  // drains and run() returns. Used to exit cleanly on SIGTERM and SIGINT.
  void stop();

  // Peers may not announce a reserved type (1 to 99), and must be in the
  // rules file; the one exception is a recording controller, accepted when
  // remote recording is on.
  bool inline accept_peer_type(boost::uint32_t peer_type) const {
    if (peer_type == RECORDING_CONTROLLER)
      return remote_recording_enabled();
    return peer_type > peers::MAX_MULTIPLEXER_SPECIAL_PEER_TYPE && Base::accept_peer_type(peer_type);
  }

  // The peer's welcome was accepted: now send ours and arm the heartbeats,
  // and note the arrival for the recording and the peers file. A recording
  // controller is passive: it calls in when it has something to ask.
  void after_connection_registration(Connection::pointer new_connection, const WelcomeMessage &) {
    if (new_connection->peer_type() == RECORDING_CONTROLLER)
      new_connection->set_is_passive(true);
    new_connection->start_rest();
    _emit_peer(PeerEvent::CONNECTED, new_connection->peer_id(), new_connection->peer_type());
    _write_peers_file();
  }

  // A registered peer's connection ended; a tap it held ends with it.
  void connection_unregistered(Connection *conn) {
    _emit_peer(PeerEvent::DISCONNECTED, conn->peer_id(), conn->peer_type());
    _untap(conn);
    _write_peers_file(conn);
  }

private:
  void _start_accept();
  void _handle_accept(Connection::pointer new_connection, const boost::system::error_code &error);

  // Everything about the message being routed, passed down the _schedule
  // calls. Collects the delivery failures as they happen; at the end,
  // _handle_delivery_errors turns them into one DELIVERY_ERROR if any.
  struct MessageMetaHandler : boost::noncopyable {
    MessageMetaHandler(const MultiplexerMessage &message, Connection::pointer connection,
                       boost::shared_ptr<const RawMessage> raw_message)
        : msg(message), conn(connection), raw(raw_message) {}

    // Nobody of `type` received it under `rule`.
    void failed(const MultiplexerMessageDescription::RoutingRule &rule, boost::uint32_t type);
    // The instance id `to` is not connected, or could not take it.
    void failed(boost::uint64_t to);
    // The message type has no entry in the rules file.
    void unknown();
    // The message type has an entry but no routing rule, and no `to`.
    void unroutable();

  private:
    void __create_delivery_error_message(bool include_original_packet_in_report);

  public:
    boost::scoped_ptr<DeliveryError> delivery_error_message;
    const MultiplexerMessage &msg;
    const Connection::pointer conn;
    const boost::shared_ptr<const RawMessage> raw;
  };

  // The per-message path; see the file comment for the order of the cases.
  void _handle_message(Connection::pointer conn, const MultiplexerMessage &msg,
                       boost::shared_ptr<const RawMessage> raw);
  void _handle_delivery_errors(MessageMetaHandler &meta_handler);
  bool _handle_message_inlined_rules(MessageMetaHandler &meta_handler);
  bool _handle_meta_message(MessageMetaHandler &meta_handler);

  // Applying rules. The overloads narrow from a message description to its
  // list of rules to one rule to the connections of one peer type; each
  // returns how many connections the frame was queued on. Zero, with
  // report_delivery_error set, records a failure in the meta handler.
  unsigned int _schedule(MessageMetaHandler &meta_handler, const MultiplexerMessageDescription &desc);
  unsigned int _schedule(MessageMetaHandler &meta_handler,
                         const ::google::protobuf::RepeatedPtrField<MultiplexerMessageDescription::RoutingRule> &rules);
  unsigned int _schedule(MessageMetaHandler &meta_handler, const MultiplexerMessageDescription::RoutingRule &rule);
  unsigned int _schedule(MessageMetaHandler &meta_handler, ConnectionsList &connections,
                         const MultiplexerMessageDescription::RoutingRule &rule);
  unsigned int _schedule(MessageMetaHandler &meta_handler, ConnectionsList &connections,
                         const MultiplexerMessageDescription::RoutingRule &rule, boost::uint32_t peer_type);

  // whom: ALL and whom: ANY over one peer type's connections.
  unsigned int send_to_all(MessageMetaHandler &meta_handler, ConnectionsList &connections);
  unsigned int send_to_one(MessageMetaHandler &meta_handler, ConnectionsList &connections);

  // Recording. A record is built once and goes to the file session and to
  // every tap; nothing is built while neither exists.
  void _record(const MessageMetaHandler &meta_handler, boost::uint64_t recipient, boost::uint32_t recipient_type,
               RoutedMessage::Disposition disposition, bool error_reported) {
    if (!recorder_ && taps_.empty())
      return;
    // A message of the multiplexer's own, a DELIVERY_ERROR, is routed
    // through the connection of the peer it answers; it is still ours.
    const boost::uint32_t from_peer_type =
        meta_handler.msg.from() == instance_id_ ? peers::MULTIPLEXER : meta_handler.conn->peer_type();
    Record record;
    recording::fill_routed(record, meta_handler.msg, from_peer_type, recipient, recipient_type, disposition,
                           error_reported);
    _emit(record);
  }
  void _emit_peer(PeerEvent::Kind kind, boost::uint64_t peer_id, boost::uint32_t peer_type);
  // Stamps `record`, writes it to the file session, closing the session at
  // its cap, and streams it to every tap.
  void _emit(Record &record);

  // A peer receiving every record as RECORDING_RECORD messages.
  struct Tap {
    Connection::weak_pointer conn;
    boost::uint64_t peer_id;
    unsigned int payload_limit;
    boost::uint64_t dropped; // records its full outgoing queue lost
  };
  typedef std::vector<Tap> Taps;
  Taps::iterator _find_tap(const Connection *conn);
  void _untap(const Connection *conn);

  // What a file session was, kept after it closed for the status.
  struct Session {
    std::string label;
    std::string path;
    boost::uint64_t started_us = 0;
    boost::uint64_t max_bytes = 0;
    boost::uint64_t bytes = 0;
    boost::uint64_t records = 0;
    std::string stopped; // why it ended; empty while open or before the first
  };

  // RECORDING_CONTROL from a peer: carry it out and answer RECORDING_STATUS.
  void _handle_recording_control(MessageMetaHandler &meta_handler);
  void _fill_status(RecordingStatus &status, const Connection *requester);
  // Queues `payload` as a message of `type` on the sender's connection,
  // referencing the message being handled.
  void _reply(const MessageMetaHandler &meta_handler, boost::uint32_t type, const ::google::protobuf::Message &payload);
  static void _on_session_deadline(weak_pointer server, const boost::system::error_code &error);

  // The peers file, if configured; `leaving` is excluded, since it is
  // written before the indexes drop it.
  void _write_peers_file(Connection *leaving = NULL);
  // A peer type's name for the peers file: from the rules, or the reserved name.
  std::string _peer_name(boost::uint32_t peer_type) const;

private:
  boost::asio::ip::tcp::acceptor acceptor_;
  boost::shared_ptr<const RawMessage> welcome_message_;
  boost::asio::io_service &io_service_;
  unsigned int memory_log_every_ = 0;
  unsigned long routed_messages_ = 0;
  std::string rules_sha1_;
  std::string recording_dir_;
  bool allow_tap_ = false;
  std::unique_ptr<Recorder> recorder_;
  Session session_;
  boost::asio::deadline_timer session_timer_;
  Taps taps_;
  std::string peers_file_;
}; // class Server

}; // namespace multiplexer

#endif // MX_MULTIPLEXER_SERVER_H_
