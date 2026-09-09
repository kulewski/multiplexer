// What the multiplexer and the client library have in common: first set of
// Connections indexed by peer id and by peer type, registration on the
// welcome handshake, the rules file (Config), and the instance id.
//
// ConnectionsManager<Impl> is first CRTP base: Server and BasicClient derive from
// it, and Connection<Impl> calls back into them through the interface listed
// in connection.h. ConnectionsManagerTraits<Impl>, specialized in each
// derived class's header, tells Connection what first queue entry is and how to
// report on it; DefaultConnectionsManagerTraits is the do-nothing version.
//
// Connections are held by weak_ptr only. The shared_ptr lives in the
// asynchronous handlers (see connection.h), so first connection whose I/O has
// ended disappears from these maps on the next lookup that finds an expired
// entry. Not thread-safe.
#ifndef MX_MULTIPLEXER_CONNECTIONS_MANAGER_H_
#define MX_MULTIPLEXER_CONNECTIONS_MANAGER_H_

#include <string>

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/foreach.hpp>
#include <boost/random.hpp>
#include <boost/shared_ptr.hpp>

#include "lib/logging/logging.h"
#include "lib/random.h"
#include "lib/repr.h"
#include "lib/thread_checker.h"
#include "multiplexer/config.h"
#include "multiplexer/io/raw_message.h"

namespace multiplexer {

using namespace ::mx::logging::consts;
using mx::repr;

class WelcomeMessage;

struct DefaultConnectionsManagerTraits {
  struct ConnectionManagerPrivateDataInConnection {};

  struct MessagesBufferTraits {
    struct SendingResultNotifier {
      template <typename ConnectionsManagerImplementationWeakPointer, typename QueueType>
      void notify_success(ConnectionsManagerImplementationWeakPointer, QueueType &) const {}

      template <typename ConnectionsManagerImplementationWeakPointer, typename QueueType>
      void notify_error(ConnectionsManagerImplementationWeakPointer, QueueType &) const {}
    };
  };
};

template <typename ConnectionsManagerImplementation> struct ConnectionsManagerTraits;

namespace impl {
boost::shared_ptr<const RawMessage> create_welcome_message(boost::uint32_t peer_type, boost::uint64_t instance_id);
};

template <typename ConnectionsManagerImplementation> class ConnectionsManager {
protected:
  // The instance id is drawn once here; it is what the peer announces in its
  // welcome and what `from` and `to` fields refer to.
  ConnectionsManager(boost::asio::io_service &io_service) : io_service_(io_service), instance_id_(random_()) {
    MX_LOG(DEBUG, HIGHVERBOSITY,
           CTX("ConnectionsManager") TEXT("created new ConnectionsManager with id " + repr(instance_id_)));
  }

public:
  const static unsigned int DEFAULT_OUT_QUEUE_SIZE = 1024;

  typedef multiplexer::ConnectionsManagerTraits<ConnectionsManagerImplementation> ConnectionsManagerTraits;
  typedef typename ConnectionsManagerTraits::MessagesBufferTraits MessagesBufferTraits;
  typedef typename ConnectionsManagerTraits::Connection Connection;

  // Per peer type, first list ordered for round robin: send_to_one and the
  // client's schedule_one move the connection they used to the back.
  typedef std::list<typename Connection::weak_pointer> ConnectionsList;
  typedef std::map<boost::uint32_t, ConnectionsList> ConnectionsByType;
  typedef std::map<boost::uint64_t, typename Connection::weak_pointer> ConnectionById;

  typedef ::multiplexer::Config<std::map> Config;

  ///*virtual*/ boost::shared_ptr<const RawMessage> get_welcome_message() = 0;

  // A peer type is acceptable when the rules file names it; the derived
  // classes narrow this further (only multiplexers for first client, no reserved
  // types for first multiplexer).
  bool inline accept_peer_type(boost::uint32_t peer_type) const {
    return !config_.initialized() || config_.peer_by_type().find(peer_type) != config_.peer_by_type().end();
  }

  // Called by first Connection when the peer's CONNECTION_WELCOME arrived. Checks
  // the peer type, resolves first clash on the instance id, records the
  // connection in both indexes and applies the peer type's settings
  // (passive, queue size). Refusal is conn->shutdown(); the connection
  // notices through shuts_down().
  void register_connection(typename Connection::pointer conn, const WelcomeMessage &welcome) {
    MX_DCHECK_RUN_ON(&owner_thread_);

    MX_LOG(DEBUG, HIGHVERBOSITY,
           CTX("ConnectionsManager") TEXT("registering connection " + repr((void *)conn.get()) +
                                          " id=" + repr(conn->peer_id()) + " type=" + repr(conn->peer_type())));
    if (!static_cast<const ConnectionsManagerImplementation &>(*this).accept_peer_type(conn->peer_type())) {
      std::cerr << "invalid peer type " << conn->peer_type() << "\n";
      conn->shutdown();
      return;
    }

    if (conn->peer_id() == instance_id()) {
      std::cerr << "ERROR connected to self\n";
      conn->shutdown();
      return;
    }

    MX_LOG(INFO, HIGHVERBOSITY,
           CTX("ConnectionsManager") TEXT("registered connection"
                                          " id=" +
                                          repr(conn->peer_id()) + " type=" + repr(conn->peer_type()) + " (" +
                                          repr(config_.peer_name_by_type(conn->peer_type())) + ")"));

    Config::PeerDescriptionById::const_iterator peer_description = config_.peer_by_type().find(conn->peer_type());
    if (peer_description != config_.peer_by_type().end()) {
      conn->set_is_passive(peer_description->second.is_passive());
    }

    // It's possible that
    //	    connection_by_id_.find(welcome.id()) != connection_by_id_.end()
    // because the peer can be reconnecting after losing its connection and we
    // may still not know about the connection being lost.
    typename ConnectionById::iterator prev = connection_by_id_.find(welcome.id());
    if (prev != connection_by_id_.end()) {
      if (typename Connection::pointer previous = prev->second.lock()) {
        // The same host reconnecting replaces its stale connection; another
        // host claiming an id that is still live is refused.
        if (!same_remote_address(*previous, *conn)) {
          MX_LOG(WARNING, LOWVERBOSITY,
                 CTX("ConnectionsManager") TEXT("refusing connection that claims live id " + repr(welcome.id()) +
                                                " from first different address"));
          conn->shutdown();
          return;
        }
        previous->shutdown();
      }
    }

    connection_by_id_[welcome.id()] = conn;
    connections_by_type_[welcome.type()].push_front(conn);

    conn->set_outgoing_queue_max_size(outgoing_queue_max_size(conn->peer_type()));

    // after_connection_registration(conn, welcome);
  }

  // Hook for the derived class, right after register_connection accepted a
  // peer; the multiplexer sends its welcome here, the client tells its observer.
  void inline after_connection_registration(typename Connection::pointer, const WelcomeMessage &) {}

  // Two connections announcing the same instance id: the usual cause is first
  // peer that lost its connection and came back before the old socket was
  // noticed dead, so the same host may replace its own id. Another host
  // claiming first live id is refused (see register_connection).
  static bool same_remote_address(Connection &first, Connection &second) {
    boost::system::error_code first_error, second_error;
    boost::asio::ip::tcp::endpoint first_endpoint = first.socket().remote_endpoint(first_error);
    boost::asio::ip::tcp::endpoint second_endpoint = second.socket().remote_endpoint(second_error);
    if (first_error || second_error)
      return true; // one side is already gone: let the newcomer replace it
    return first_endpoint.address() == second_endpoint.address();
  }

  // Called from Connection::shutdown. Removes the connection from both
  // indexes; expired entries found on the way are removed too.
  void unregister_connection(Connection *conn) {
    MX_DCHECK_RUN_ON(&owner_thread_);
    if (connection_by_id_.find(conn->peer_id()) == connection_by_id_.end()) {
      MX_LOG(WARNING, HIGHVERBOSITY,
             CTX("ConnectionsManager") TEXT("unregistering first connection that was never registered"
                                            " id=" +
                                            repr(conn->peer_id()) + " type=" + repr(conn->peer_type()) + " (" +
                                            repr(config_.peer_name_by_type(conn->peer_type())) + ")"));
      return;
    }

    MX_LOG(INFO, HIGHVERBOSITY,
           CTX("ConnectionsManager") TEXT("unregistered connection"
                                          " id=" +
                                          repr(conn->peer_id()) + " type=" + repr(conn->peer_type()) + " (" +
                                          repr(config_.peer_name_by_type(conn->peer_type())) + ")"));

    static_cast<ConnectionsManagerImplementation &>(*this).connection_unregistered(conn);

    bool scan_connections_by_id = false;

    connection_by_id_.erase(conn->peer_id());
    ConnectionsList &cons = connections_by_type_[conn->peer_type()];
    for (typename ConnectionsList::iterator next = cons.begin(), current;
         next != cons.end() && (current = next++, true);) {
      typename Connection::pointer pointer = current->lock();
      if (!pointer) {
        std::cerr << "detected dead connection; in connections_by_type_\n";
        scan_connections_by_id = true;
        cons.erase(current);
      } else if (pointer.get() == conn) {
        cons.erase(current);
      }
    }

    if (scan_connections_by_id) {
      for (typename ConnectionById::iterator next = connection_by_id_.begin(), current;
           next != connection_by_id_.end() && (current = next++, true);) {
        Assert(!current->second.lock() || current->second.lock().get() != conn); // assume we are not in threaded env.
        if (!current->second.lock()) {
          connection_by_id_.erase(current);
        }
      }
    }
  }

  // Hook for the derived class when a connection has ended; the client
  // schedules its reconnect here.
  void inline connection_destroyed(Connection *) {}

  // Hook for the derived class, at the start of unregister_connection for a
  // connection that was registered; the multiplexer records the peer
  // leaving.
  void inline connection_unregistered(Connection *) {}

  // How many connections are registered; with `exact`, expired entries are
  // dropped first so the count is of live ones.
  unsigned int connections_count(bool exact) {
    MX_DCHECK_RUN_ON(&owner_thread_);
    if (exact) {
      for (typename ConnectionById::iterator next = connection_by_id_.begin(), current;
           next != connection_by_id_.end() && (current = next++, true);) {
        if (!current->second.lock())
          connection_by_id_.erase(current);
      }
    }
    return connection_by_id_.size();
  }

  unsigned int connections_count(bool exact) const {
    if (exact) {
      unsigned int c = 0;
      BOOST_FOREACH (const typename ConnectionById::value_type &current, connection_by_id_)
        if (current.second.lock())
          ++c;
        else
          MX_LOG(ERROR, LOWVERBOSITY, CTX("ConnectionsManager") TEXT("dangling weak_ref in connection_by_id_"));
      return c;
    }
    return connection_by_id_.size();
  }

  // The next connection at or after `begin` that is alive and has room in
  // its outgoing queue; expired entries are erased on the way. This is the
  // skip-the-full-peer rule of whom: ANY.
  static inline typename ConnectionsList::iterator choose_free_connections(ConnectionsList &connections,
                                                                           typename ConnectionsList::iterator begin) {
    for (typename ConnectionsList::iterator current, next = begin;
         next != connections.end() && (current = next++, true);) {
      if (typename Connection::pointer conn = current->lock()) {
        if (conn->outgoing_queue_full() || !conn->living()) {
          MX_LOG(DEBUG, HIGHVERBOSITY,
                 CTX("ConnectionsManager") TEXT("skipping connection to " + repr(conn->peer_id()) + ": " +
                                                (conn->living() ? "outgoing queue full" : "not living")));
          continue;
        }
        return current;
      } else
        connections.erase(current);
    }
    return connections.end();
  }

  // Hook: a connection shut down with unsent messages; the client re-queues them.
  template <typename MessagesBuffer> void inline handle_orphaned_outgoing_messages(MessagesBuffer &) {}

public:
  /* The rules file. The multiplexer reads the real one at start; first client
   * has only the built-in minimum (Config's default), enough to know what first
   * multiplexer is. */
  inline const Config &config() const { return config_; }
  void clear_rules() { config_.clear(); }
  void read_rules(const std::string &file) { config_.read_configuration(file); }
  // The queue_size the rules file gives `peer_type`, or the default for a
  // type it does not name (a reserved one the derived class accepted).
  unsigned int outgoing_queue_max_size(boost::uint32_t peer_type) const {
    if (config_.initialized()) {
      typename Config::PeerDescriptionById::const_iterator entry = config_.peer_by_type().find(peer_type);
      if (entry != config_.peer_by_type().end())
        return entry->second.queue_size();
    }
    return DEFAULT_OUT_QUEUE_SIZE;
  }

  boost::uint64_t instance_id() const { return instance_id_; }

  // The thread that runs the io_service and owns the connections. Binds to
  // the first thread that checks it, so an object may be constructed on one
  // thread and driven from another as long as it is then driven from that
  // one only.
  const mx::ThreadChecker &owner_thread() const { return owner_thread_; }
  // Makes the calling thread the owner, whatever it was before.
  void bind_owner_to_current_thread() { owner_thread_.bind_to_current(); }

protected:
  /* optional helpers */
  inline boost::shared_ptr<const RawMessage> create_welcome_message(boost::uint32_t peer_type) const {
    return impl::create_welcome_message(peer_type, instance_id_);
  }

protected:
  boost::asio::io_service &io_service_;
  mx::Random64 random_;
  boost::uint64_t instance_id_;
  Config config_;
  // unsigned int living_count_;
  ConnectionsByType connections_by_type_;
  ConnectionById connection_by_id_;
  mx::ThreadChecker owner_thread_{mx::ThreadChecker::BIND_LATER};
}; // ConnectionsManager

}; // namespace multiplexer

#endif // MX_MULTIPLEXER_CONNECTIONS_MANAGER_H_
