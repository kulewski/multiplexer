// Server on several event loops: the routing lock, the frames each loop's
// routing queued and writes once it let go of the lock, a live connection
// whose instance id a newcomer takes, and the steps only a connection's
// own loop may take, posted there. The design is in server.h; at one loop
// only the replacement and the unregistration pass through here, on the
// paths they had. Kept apart from server.cc, the per-message path, which
// calls in here only on several loops.
#include <asio/post.hpp>
#include <exception>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "multiplexer/server.h"

namespace multiplexer {

using mx::repr;

namespace {
// The connections this thread's routing queued a frame on and took the
// writing of, written once it lets go of the routing lock (Server::_queue).
thread_local std::vector<Server::Connection::pointer> claimed_writes;
}  // namespace

// The welcome every connection is sent is made here, before another loop
// runs, and marked as the connections' own once, so that no loop writes to
// it; the drop lines' timer ticks under the routing lock, as the lines are
// counted under it.
void Server::set_threads(unsigned int threads) {
  MX_DCHECK_RUN_ON(&owner_thread());
  AssertMsg(!loops_ && threads >= 1, "set_threads() once, before start(), with 1 thread at least");
  if (threads == 1) {
    return;
  }
  loops_.reset(new ServerLoops(io_service_, threads));
  get_welcome_message()->mark_own();
  drops_.set_guard([this](const std::function<void()>& tick) {
    RoutingScope routing(*this);
    tick();
  });
  MX_LOG(INFO, LOWVERBOSITY, CTX("multiplexer.server") TEXT(repr(threads) + " event loops"));
}

void Server::finish_loops() {
  if (loops_) {
    loops_->finish();
  }
}

void Server::post_to_loop(unsigned int loop, std::function<void()> work) {
  if (loops_) {
    asio::post(loops_->loop(loop), std::move(work));
    return;
  }
  AssertMsg(loop == 0, "loop " + repr(loop) + " of a multiplexer on one loop asked for");
  asio::post(io_service_, std::move(work));
}

void Server::_lock_routing() const {
  const std::thread::id self = std::this_thread::get_id();
  if (routing_holder_.load(std::memory_order_relaxed) != self) {
    routing_mutex_.lock();
    routing_holder_.store(self, std::memory_order_relaxed);
  }
  ++routing_depth_;
#ifndef NDEBUG
  const_cast<Server*>(this)->bind_owner_to_current_thread();
#endif
}

void Server::_unlock_routing() const {
  if (--routing_depth_) {
    return;
  }
  routing_holder_.store(std::thread::id(), std::memory_order_relaxed);
  routing_mutex_.unlock();
  _write_claimed();
}

Server::Queued Server::_queue_shared(const Connection::pointer& connection,
                                     const std::shared_ptr<const RawMessage>& raw, bool force) {
  switch (connection->enqueue_shared(raw, force)) {
    case Connection::Shared::Queued::CLAIMED:
      claimed_writes.push_back(connection);
      return Queued::QUEUED;
    case Connection::Shared::Queued::QUEUED:
      return Queued::QUEUED;
    case Connection::Shared::Queued::FULL:
      return Queued::FULL;
    case Connection::Shared::Queued::ENDED:
      break;
  }
  return Queued::GONE;
}

// A write that throws must not leave the others unwritten, their channels
// taken with nobody writing; this runs as the outermost RoutingScope ends.
void Server::_write_claimed() {
  for (std::size_t index = 0; index < claimed_writes.size(); ++index) {
    try {
      claimed_writes[index]->write_claimed();
    } catch (const std::exception& error) {
      MX_LOG(ERROR, LOWVERBOSITY,
             CTX("multiplexer.server")
                 TEXT("writing to peer " + repr(claimed_writes[index]->peer_id()) + " failed: " + error.what()));
    }
  }
  claimed_writes.clear();  // keeps its capacity for the next frame
}

void Server::unregister_connection(Connection* conn) {
  RoutingScope routing(*this);
  if (replaced_.count(conn)) {
    _leave_routing(conn);
    return;
  }
  Base::unregister_connection(conn);
}

// Out of routing now, as its shutdown takes it out at one loop: no loop
// routes to it from here on, its departure is recorded ahead of the
// newcomer's arrival, and the newcomer takes its id next
// (Base::register_connection). The rest of its leaving, its tap and its
// key filters, it does on its own loop when it ends (unregister_connection);
// what it reads until it has closed is dropped (handle_message), as one
// loop reads nothing more of it, and it leaves replaced_ once closed
// (connection_closed).
void Server::replace_connection(const Connection::pointer& previous) {
  if (!loops_) {
    previous->shutdown();  // logs its own "unregistered connection"
    return;
  }
  MX_DCHECK_RUN_ON(&owner_thread());  // register_connection()'s routing scope
  MX_LOG(INFO, HIGHVERBOSITY,
         CTX("ConnectionsManager")
             TEXT("unregistered connection id=" + repr(previous->peer_id()) + " type=" + repr(previous->peer_type()) +
                  " (" + repr(config_.peer_name_by_type(previous->peer_type())) + ")"));
  if (previous->in_type_list) {
    connections_by_type_[previous->peer_type()].erase(previous->type_list_place);
    previous->in_type_list = false;
  }
  replaced_.insert(previous.get());
  _emit_peer(PeerEvent::DISCONNECTED, previous->peer_id(), previous->peer_type());
  asio::post(previous->socket().get_executor(), [previous] {
    if (!previous->shuts_down()) {
      previous->shutdown();
    }
  });
}

void Server::_end_on_its_loop(const Connection::pointer& connection, bool drain) {
  asio::post(connection->socket().get_executor(), [connection, drain] {
    if (connection->shuts_down()) {
      return;
    }
    if (drain && connection->registered()) {
      connection->close_when_flushed(CLOSE_READ_SECONDS);
    } else {
      connection->shutdown();  // and one that never sent its welcome has nothing queued
    }
  });
}

// The queue's size at once, under the connection's own lock, as routing
// on any loop reads it; the flag on the connection's loop, if it still
// lives by then.
void Server::_follow_rules(const Connection::pointer& connection, bool passive, unsigned int queue_size) {
  connection->set_outgoing_queue_max_size(queue_size);
  asio::post(connection->socket().get_executor(), [connection, passive] {
    if (connection->living()) {
      connection->set_is_passive(passive);
    }
  });
}

}  // namespace multiplexer
