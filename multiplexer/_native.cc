// The Python extension module multiplexer._native: pybind11 bindings for
// Client (as multiplexer.mxclient.Client's base), its exceptions and
// trackers, the io_service, the logging entry points, and the constants
// from defaults.h. mxclient.py wraps this into the Python API; nothing else
// imports _native directly.
//
// Messages cross the boundary serialized: read_message returns the frame's
// bytes and Python parses them with its own protobuf classes, so the C++
// and Python protobuf runtimes never share objects. The GIL is released
// around the blocking read so that other Python threads run meanwhile.
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

// The typing helpers spell tuple and callable types out in the signatures
// pybind11 writes into docstrings, which the generated .pyi stub is made
// from. They exist from pybind11 2.12; on an older one, the distribution's
// on Debian 12 or Ubuntu 22.04 for the make build, the same names are the
// plain types and the stub says `tuple` and `Callable`.
#if PYBIND11_VERSION_HEX >= 0x020C0000
#include <pybind11/typing.h>
namespace mxtyping = pybind11::typing;
#else
namespace mxtyping {
template <typename... Types>
using Tuple = pybind11::tuple;
template <typename Signature>
using Callable = pybind11::function;
}  // namespace mxtyping
#endif
#include <pthread.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "lib/logging/logging.h"
#include "lib/memory.h"
#include "lib/seconds.h"
#include "lib/type_utils.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/client.h"
#include "multiplexer/threaded_client.h"

namespace multiplexer {

// Interpreter exit. Once finalization starts no thread but the main one
// may take the GIL: a daemon thread that tries is ended by CPython with
// pthread_exit, which force-unwinds through whatever C++ frames it is in.
// That terminates the process on libstdc++ (a destructor frame) and
// crashes the unwinder on libc++abi. So a thread coming back from a
// blocking wait must not ask for the GIL once the exit is under way; it
// parks instead (see GilRelease). Two signals say the exit is under way:
// the interpreter's own finalizing flag, and `exiting`, set by begin_exit()
// from an atexit hook that mxclient.py registers. The hook matters because
// the interpreter's flag is set only after the atexit hooks ran, and a
// thread that checked it too early then waits for the GIL that the main
// thread holds throughout finalization, and is killed when it gets it. The
// hook sets our flag first, then gives up the GIL until every callback
// that was already past its check has finished (CallbackSlot), so nothing
// is caught between the check and the GIL when finalization starts.
static std::atomic<bool> exiting(false);
// The interpreter's main thread, the one that finalizes: the only thread
// that may take the GIL back once the exit is under way. Taken from
// threading.main_thread() at import, which need not run on it (Django's
// runserver imports on its django-main-thread), recorded again by
// begin_exit(), which the atexit hooks run on the finalizing thread, and
// in a forked child, whose main thread is the one that forked.
static std::atomic<unsigned long> main_thread_ident(0);
// Threads past the finalization check and about to take the GIL, or
// holding it: a callback into Python from the io thread, or a thread coming
// back from a blocking wait. begin_exit() waits for this to reach zero
// before it returns, so that finalization never starts under one of them.
// See CallbackSlot and GilRelease.
static std::atomic<int> gil_takers(0);

static bool python_is_finalizing() {
  if (exiting.load()) {
    return true;
  }
#if PY_VERSION_HEX >= 0x030D0000
  return Py_IsFinalizing();
#else
  return _Py_IsFinalizing();
#endif
}

// The guard around every callback into Python from the io thread, and
// around the deleters of the Python objects those callbacks hold. It
// claims a slot in gil_takers before it checks for the exit, and
// begin_exit() sets the exit flag before it reads the count: whichever
// happens first, either the callback sees the flag and does nothing, or
// begin_exit() sees the slot and waits for the callback to finish. Without
// it a callback could pass the check, then take the GIL as finalization
// began and end up running Python without one, or be killed by the
// interpreter inside C++ frames.
struct CallbackSlot {
  CallbackSlot() {
    gil_takers.fetch_add(1);
    ok = !python_is_finalizing();
    if (!ok) {
      gil_takers.fetch_sub(1);
    }
  }
  ~CallbackSlot() {
    if (ok) {
      gil_takers.fetch_sub(1);
    }
  }
  bool ok;  // false: the interpreter is leaving; do nothing
};

// Sleeps forever: for a thread that must not take the GIL again. A daemon
// thread parked here does not delay the exit, and a non-daemon one was
// joined before the exit began.
[[noreturn]] static void park_forever() {
  for (;;) {
    pause();
  }
}

// Releases the GIL around a blocking wait, like pybind11::gil_scoped_release,
// except on the way back: a thread other than the main one that finds the
// exit under way parks instead of retaking the GIL. The main thread always
// retakes it; it is the one running the exit. The check and the retake
// hold a gil_takers slot, like a callback, so that begin_exit() lets a
// thread that passed the check retake the GIL and get to its next wait,
// where it parks, rather than have the interpreter kill it mid-retake.
class GilRelease {
 public:
  GilRelease() : state_(PyEval_SaveThread()) {}
  ~GilRelease() noexcept(false) {
    gil_takers.fetch_add(1);
    if (python_is_finalizing() && PyThread_get_thread_ident() != main_thread_ident.load()) {
      gil_takers.fetch_sub(1);
      park_forever();
    }
    PyEval_RestoreThread(state_);
    gil_takers.fetch_sub(1);
  }
  GilRelease(const GilRelease&) = delete;
  GilRelease& operator=(const GilRelease&) = delete;

 private:
  PyThreadState* state_;
};

// A Python callable as a client's drop observer: called with
// (message_id, DropReason) and the GIL, on whatever thread the client's
// loop runs, and destroyed only with the GIL held, as every callback here.
static BasicClient::DropObserver drop_observer_for(pybind11::object callback) {
  if (callback.is_none()) {
    return BasicClient::DropObserver();
  }
  std::shared_ptr<pybind11::object> held(new pybind11::object(callback), [](pybind11::object* object) {
    CallbackSlot slot;
    if (!slot.ok) {
      return;  // leaked on purpose: the GIL is out of reach, and the process is going
    }
    pybind11::gil_scoped_acquire acquire;
    delete object;
  });
  return [held](std::uint64_t message_id, DropReason reason) {
    CallbackSlot slot;
    if (!slot.ok) {
      return;  // the callback would need the GIL; nothing to tell any more
    }
    pybind11::gil_scoped_acquire acquire;
    try {
      (*held)(message_id, reason);
    } catch (pybind11::error_already_set& error) {
      error.restore();
      PyErr_Print();
    }
  };
}

// A Python callable as a callback the library calls, a send's or a
// flush's: with the GIL, on whatever thread the client's loop runs, an
// exception printed rather than thrown into the loop, nothing once the
// interpreter is leaving, and destroyed only with the GIL held.
template <typename... Args>
static std::function<void(Args...)> python_callback(pybind11::function callback) {
  std::shared_ptr<pybind11::function> held(new pybind11::function(callback), [](pybind11::function* function) {
    CallbackSlot slot;
    if (!slot.ok) {
      return;  // leaked on purpose: the GIL is out of reach, and the process is going
    }
    pybind11::gil_scoped_acquire acquire;
    delete function;
  });
  return [held](Args... args) {
    CallbackSlot slot;
    if (!slot.ok) {
      return;  // the callback would need the GIL; nothing to tell any more
    }
    pybind11::gil_scoped_acquire acquire;
    try {
      (*held)(args...);
    } catch (pybind11::error_already_set& error) {
      error.restore();
      PyErr_Print();
    }
  };
}

// A send's callback, the same in every client: called once with the copies
// written, 1 at the first, or 0.
static BasicClient::SendCallback send_callback_for(pybind11::function callback) {
  return python_callback<unsigned int>(callback);
}

// Client with the few signatures pybind11 needs: strings instead of
// templates, and a read that releases the GIL.
struct PythonClient : public Client {
 public:
  // The io_service is owned here, not by a Python attribute: CPython clears
  // an instance's attributes before the base type's deallocator runs, so a
  // Client that only borrowed it would shut down on freed memory.
  explicit PythonClient(std::uint32_t client_type) : Client(client_type) {}
  // What ~Client does, the loop run without the GIL as by shutdown()
  // below: a client collected while a multiplexer does not answer holds
  // no other thread for the second its connections read on. An orphan is
  // left to ~Client.
  ~PythonClient() {
    basic_client_->release_drop_observer();  // nothing calls into Python while the client dies
    if (!basic_client_->orphaned()) {
      basic_client_->bind_to_current_thread();
      basic_client_->release_follows();  // a send's callback included
      GilRelease release;
      Client::shutdown();
    }
  }
  // `observer`, a callable or None: see drop_observer_for.
  void set_drop_observer(pybind11::object observer) { Client::set_drop_observer(drop_observer_for(observer)); }

  mxtyping::Tuple<pybind11::bytes, ConnectionWrapper> read_message(float timeout) {
    static_assert(std::is_same<BasicClient::IncomingMessagesBuffer::value_type::second_type, ConnectionWrapper>::value,
                  "a reply carries the connection it came on");
    BasicClient::IncomingMessagesBuffer::value_type next;
    {
      GilRelease release;
      next = Client::read_raw_message(timeout);
    }
    return mxtyping::Tuple<pybind11::bytes, ConnectionWrapper>(
        pybind11::make_tuple((pybind11::bytes)next.first->get_message(), next.second));
  }

  bool wait_for_any_connection(float timeout) {
    basic_client_->check_not_orphaned();
    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    GilRelease release;
    return basic_client_->wait_for_any_connection(*timer);
  }
  // Connecting waits for the multiplexer's welcome, `timeout` seconds at
  // most: without the GIL, as every wait here, so that a multiplexer that
  // does not answer holds no other thread of the program.
  ConnectionWrapper connect(const std::string& host, std::uint16_t port, float timeout) {
    GilRelease release;
    return Client::connect(host, port, timeout);
  }
  bool wait_for_connection(ConnectionWrapper connection, float timeout) {
    GilRelease release;
    return Client::wait_for_connection(connection, timeout);
  }
  // read_message that returns None as soon as `watch` dies.
  // A (bytes, connection) pair, or None once `watch` is gone.
  std::optional<mxtyping::Tuple<pybind11::bytes, ConnectionWrapper>> read_message_watching(float timeout,
                                                                                           ConnectionWrapper watch) {
    basic_client_->check_not_orphaned();
    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    BasicClient::IncomingMessagesBuffer::value_type next;
    bool got;
    {
      GilRelease release;
      got = basic_client_->wait_for_incoming_message_or_loss(*timer, watch);
      if (got) {
        next = basic_client_->next_incoming_message();
      }
    }
    if (!got) {
      return std::nullopt;
    }
    return mxtyping::Tuple<pybind11::bytes, ConnectionWrapper>(
        pybind11::make_tuple((pybind11::bytes)next.first->get_message(), next.second));
  }
  // A copy on every live connection, for the library's own sends to ALL:
  // how many connections took one.
  unsigned int schedule_all(pybind11::bytes serialized, float timeout) {
    std::string message(serialized);
    return Client::schedule_all(&message, timeout);
  }
  // flush_all() runs the loop without the GIL, as the reads do, so that
  // the program's other threads go on while it waits.
  bool flush_all(float timeout) {
    basic_client_->check_not_orphaned();
    GilRelease release;
    return Client::flush_all(timeout);
  }
  // How mxclient sends, as every client does (BasicClient::send): placed
  // or held, without waiting. `callback(written)`, when given, hears how
  // the message ended, as on ThreadedClient, with the GIL, inside a later
  // call that runs the loop. False, the callback never called, when
  // nothing may take the message.
  bool send(pybind11::bytes serialized, bool all, std::optional<LanePtr> lane, float timeout,
            std::optional<mxtyping::Callable<void(unsigned int)>> callback) {
    basic_client_->check_not_orphaned();
    std::string message(serialized);
    basic_client_->poll();
    return basic_client_->send(_serialize(&message), all, lane.value_or(LanePtr()), timeout, 0, NULL, NULL,
                               callback ? send_callback_for(*callback) : BasicClient::SendCallback());
  }
  // mxclient's flushing send to one connection: Client::_send_one, the loop
  // run without the GIL; the connection that wrote it, or an exception.
  ConnectionWrapper send_one(pybind11::bytes serialized, std::optional<ConnectionWrapper> preferred,
                             std::optional<LanePtr> lane, float timeout) {
    basic_client_->check_not_orphaned();
    std::string message(serialized);
    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    GilRelease release;
    return _send_one(_serialize(&message), *timer, preferred.value_or(ConnectionWrapper()), lane.value_or(LanePtr()));
  }
  // mxclient's flushing send to ALL: waits, without the GIL, for the first
  // copy, and returns 1, or throws as the flushing send to one does.
  unsigned int send_all_and_wait(pybind11::bytes serialized, float timeout) {
    basic_client_->check_not_orphaned();
    std::string message(serialized);
    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    GilRelease release;
    bool taken = false, lost = false;
    if (_send_and_wait(_serialize(&message), true, LanePtr(), *timer, NULL, &taken, &lost)) {
      return 1;
    }
    if (!taken) {
      MXTHROW(NotConnected());
    }
    _raise_for_nothing_written(LanePtr(), lost);
  }
  // shutdown() runs the loop while what was sent before it is written,
  // `timeout` seconds at most, and while the connections close the polite
  // way, CLOSE_READ_SECONDS at most (Client::shutdown): without the GIL, so
  // that the program's other threads go on meanwhile.
  void shutdown(float timeout) {
    {
      GilRelease release;
      Client::shutdown(timeout);
    }
    basic_client_->release_drop_observer();  // after the shutdown's own drops were told
  }
};

// The exception types, as Python classes, for the translator and for the
// threaded client's callbacks.
static pybind11::object *py_not_connected, *py_operation_timed_out, *py_operation_failed;

static pybind11::object exception_for(ThreadedClient::Outcome outcome) {
  switch (outcome) {
    case ThreadedClient::TIMED_OUT:
      return (*py_operation_timed_out)();
    case ThreadedClient::FAILED:
      return (*py_operation_failed)();
    default:
      return (*py_not_connected)();
  }
}

// ThreadedClient for Python. Blocking calls release the GIL, callbacks take
// it on the io thread, and a Python callback is destroyed only with the GIL
// held, whichever thread drops the last reference.
struct PythonThreadedClient {
  // `on_message` is a Python callable, or None; it runs on the io thread with
  // the GIL, as query callbacks do, and receives (bytes, connection).
  // `on_message` is a callable or None; a std::optional of a callable would
  // not accept None here, pybind11 folds an optional of a Python object type
  // into the type itself, so it stays an object and the Python wrapper
  // types it.
  PythonThreadedClient(std::uint32_t peer_type, pybind11::object on_message)
      : client(peer_type, on_message.is_none() ? ThreadedClient::MessageSink() : sink_for(on_message)) {}
  static ThreadedClient::MessageSink sink_for(pybind11::object callback) {
    std::shared_ptr<pybind11::object> held(new pybind11::object(callback), [](pybind11::object* object) {
      CallbackSlot slot;
      if (!slot.ok) {
        return;  // leaked on purpose: the GIL is out of reach, and the process is going
      }
      pybind11::gil_scoped_acquire acquire;
      delete object;
    });
    return [held](const IncomingMessage& incoming) {
      CallbackSlot slot;
      if (!slot.ok) {
        return;  // the callback would need the GIL; nothing to deliver to any more
      }
      pybind11::gil_scoped_acquire acquire;
      try {
        (*held)((pybind11::bytes)incoming.first->get_message(), incoming.second);
      } catch (pybind11::error_already_set& error) {
        error.restore();
        PyErr_Print();
      }
    };
  }
  ~PythonThreadedClient() {
    GilRelease release;
    client.shutdown();
  }
  // `observer`, a callable or None: see drop_observer_for. It runs on the
  // io thread, which lets go of it when it ends.
  void set_drop_observer(pybind11::object observer) {
    BasicClient::DropObserver wrapped = drop_observer_for(observer);
    GilRelease release;
    client.set_drop_observer(wrapped);
  }
  // `answer` is a Python callable returning whether to answer a search
  // for a backend; runs on the io thread with the GIL. See
  // ThreadedClient::set_search_policy.
  void set_search_policy(mxtyping::Callable<pybind11::object()> answer) {  // read by its truth
    std::shared_ptr<pybind11::object> held(new pybind11::object(answer), [](pybind11::object* object) {
      CallbackSlot slot;
      if (!slot.ok) {
        return;  // leaked on purpose, see sink_for
      }
      pybind11::gil_scoped_acquire acquire;
      delete object;
    });
    ThreadedClient::SearchPolicy policy = [held]() -> bool {
      CallbackSlot slot;
      if (!slot.ok) {
        return false;
      }
      pybind11::gil_scoped_acquire acquire;
      try {
        // Python's truth: a non-empty list says yes, as `if` reads it, where
        // a cast to bool refused it; a __bool__ that raises raises here.
        return static_cast<bool>(pybind11::bool_((*held)()));
      } catch (pybind11::error_already_set& error) {
        error.restore();
        PyErr_Print();
        return false;
      }
    };
    GilRelease release;
    client.set_search_policy(policy);
  }

  bool connect(const std::string& host, std::uint16_t port, float timeout) {
    GilRelease release;
    return client.connect(host, port, timeout);
  }
  // Waits for the io thread, which may need the GIL for a callback first.
  bool disconnect(const std::string& host, std::uint16_t port) {
    GilRelease release;
    return client.disconnect(host, port);
  }
  unsigned int connections_count() {
    GilRelease release;
    return client.connections_count();
  }
  // A serialized Routing (Multiplexer.proto), which the Python wrapper
  // builds from the message class.
  void set_routing_serialized(pybind11::bytes serialized) {
    Routing routing;
    if (!routing.ParseFromString(serialized)) {
      throw std::invalid_argument("not a serialized Routing");
    }
    GilRelease release;
    client.set_routing(routing);
  }
  bool routing_acknowledged() {
    GilRelease release;
    return client.routing_acknowledged();
  }
  bool flush_all(float timeout) {
    GilRelease release;
    return client.flush_all(timeout);
  }
  // The non-flushing sends only post to the io thread, so they need no GIL
  // release and are safe from callbacks; the flushing one waits. `lane` is
  // a Lane or None; `timeout` bounds how long the message may wait for a
  // connection or for room.
  // The sends that return at once; `callback(written)`, when given, hears
  // how the message ended, on the io thread (send_callback_for).
  void send(pybind11::bytes serialized, std::optional<LanePtr> lane, float timeout,
            std::optional<mxtyping::Callable<void(unsigned int)>> callback) {
    client.send_serialized(std::string(serialized), lane.value_or(LanePtr()), timeout,
                           callback ? send_callback_for(*callback) : ThreadedClient::SendCallback());
  }
  void send_all(pybind11::bytes serialized, float timeout,
                std::optional<mxtyping::Callable<void(unsigned int)>> callback) {
    client.send_all_serialized(std::string(serialized), timeout,
                               callback ? send_callback_for(*callback) : ThreadedClient::SendCallback());
  }
  // The flushing send: (written, not_connected), the second what one that
  // wrote nothing says about why (ThreadedClient::send_serialized_and_wait).
  mxtyping::Tuple<unsigned int, bool> send_and_wait(pybind11::bytes payload, bool all, float timeout,
                                                    std::optional<LanePtr> lane_given) {
    std::string serialized(payload);
    LanePtr lane = lane_given.value_or(LanePtr());
    bool not_connected = false;
    unsigned int written;
    {
      GilRelease release;
      written = client.send_serialized_and_wait(serialized, all, timeout, lane, &not_connected);
    }
    return pybind11::make_tuple(written, not_connected);
  }
  // flush_all() with `callback(flushed)` on the io thread instead of the
  // wait; what the asyncio client awaits.
  void flush_all_and_notify(float timeout, mxtyping::Callable<void(bool)> callback) {
    client.flush_all_with_callback(timeout, python_callback<bool>(callback));
  }
  // The flushing send with `callback(written, not_connected)` on the io
  // thread instead of a wait: 1 once the message reached a socket, the
  // first copy for ALL, 0 when it was given up on or `timeout` passed
  // first, `not_connected` saying which the caller is told; what the
  // asyncio client awaits.
  void send_and_notify(pybind11::bytes payload, bool all, float timeout,
                       mxtyping::Callable<void(unsigned int, bool)> callback, std::optional<LanePtr> lane_given) {
    client.send_serialized_and_notify(std::string(payload), all, timeout, python_callback<unsigned int, bool>(callback),
                                      lane_given.value_or(LanePtr()));
  }
  // The request as a serialized MultiplexerMessage, `to` included; `lane`
  // a Lane or None.
  static MultiplexerMessage parse(const std::string& serialized) {
    MultiplexerMessage msg;
    if (!msg.ParseFromString(serialized)) {
      throw std::invalid_argument("not a serialized MultiplexerMessage");
    }
    return msg;
  }
  // A query's on_received, when given: told on the io thread, with the
  // GIL, the instance id of each backend that acknowledges the request.
  typedef std::optional<mxtyping::Callable<void(std::uint64_t)>> OnReceived;
  static ReceivedCallback received_for(const OnReceived& on_received) {
    return on_received ? python_callback<std::uint64_t>(*on_received) : ReceivedCallback();
  }
  mxtyping::Tuple<pybind11::bytes, ConnectionWrapper> query(pybind11::bytes serialized, float timeout,
                                                            std::optional<LanePtr> lane_given, OnReceived on_received) {
    LanePtr lane = lane_given.value_or(LanePtr());
    MultiplexerMessage msg = parse(std::string(serialized));
    ReceivedCallback received = received_for(on_received);
    ThreadedClient::Result result;
    {
      GilRelease release;
      result = client.query(msg, timeout, lane, received);
    }
    result.check();  // throws the C++ exception, translated below
    return mxtyping::Tuple<pybind11::bytes, ConnectionWrapper>(
        pybind11::make_tuple((pybind11::bytes)result.reply.first->get_message(), result.reply.second));
  }
  void query_with_callback(
      pybind11::bytes serialized,
      mxtyping::Callable<void(std::optional<pybind11::bytes>, std::optional<ConnectionWrapper>, pybind11::object)>
          callback,
      float timeout, std::optional<LanePtr> lane_given, OnReceived on_received) {
    LanePtr lane = lane_given.value_or(LanePtr());
    MultiplexerMessage msg = parse(std::string(serialized));
    std::shared_ptr<pybind11::function> held(new pybind11::function(callback), [](pybind11::function* function) {
      CallbackSlot slot;
      if (!slot.ok) {
        return;  // leaked on purpose, see sink_for
      }
      pybind11::gil_scoped_acquire acquire;
      delete function;
    });
    client.query(
        msg,
        [held](const ThreadedClient::Result& result) {
          CallbackSlot slot;
          if (!slot.ok) {
            return;
          }
          pybind11::gil_scoped_acquire acquire;
          try {
            if (result.outcome == ThreadedClient::REPLIED) {
              (*held)((pybind11::bytes)result.reply.first->get_message(), result.reply.second, pybind11::none());
            } else {
              (*held)(pybind11::none(), pybind11::none(), exception_for(result.outcome));
            }
          } catch (pybind11::error_already_set& error) {
            // A callback that raises: print the Python traceback and go on,
            // as a thread's uncaught exception would be printed.
            error.restore();
            PyErr_Print();
          }
        },
        timeout, lane, received_for(on_received));
  }
  void shutdown(float timeout) {
    GilRelease release;
    client.shutdown(timeout);
  }

  ThreadedClient client;
};
};  // namespace multiplexer

// template <typename T>
// void foo() {
// cerr << __PRETTY_FUNCTION__ << "\n";
//}
// asio::io_service factory function

static void test_connection_wrapper(multiplexer::ConnectionWrapper /*wrap*/) {}

PYBIND11_MODULE(_native, module) {
  module.def("should_log", &mx::logging::impl::should_log, pybind11::arg("level"), pybind11::arg("verbosity"));
  module.def("current_timestamp", &mx::logging::impl::current_timestamp);
  module.def("set_logging_file", &mx::logging::set_logging_file, pybind11::arg("path"));
  module.def(
      "set_logging_fd",
      [](unsigned int logging_fd, bool close_on_delete) { mx::logging::set_logging_fd(logging_fd, close_on_delete); },
      pybind11::arg("fd"), pybind11::arg("close_on_delete") = false);
  module.def("create_log_id", &mx::logging::create_log_id);
  module.def("process_context", &mx::logging::process_context);
  module.def("set_process_context", &mx::logging::set_process_context, pybind11::arg("context"));
  module.def("set_process_context_program_name", &mx::logging::set_process_context_program_name, pybind11::arg("name"));
  module.def("set_maximal_logging_verbosity", &mx::logging::set_maximal_logging_verbosity, pybind11::arg("level"),
             pybind11::arg("verbosity"));

  module.attr("DEBUG") = DEBUG;
  module.attr("INFO") = INFO;
  module.attr("OK") = OK;
  module.attr("WARNING") = WARNING;
  module.attr("ERROR") = ERROR;
  module.attr("CRITICAL") = CRITICAL;
  module.attr("ZEROVERBOSITY") = ZEROVERBOSITY;
  module.attr("LOWVERBOSITY") = LOWVERBOSITY;
  module.attr("MEDIUMVERBOSITY") = MEDIUMVERBOSITY;
  module.attr("HIGHVERBOSITY") = HIGHVERBOSITY;
  module.attr("CHATTERBOX") = CHATTERBOX;

  // LogEntry for the Python logger: each numeric field as a property with
  // has_/clear_, each string field likewise with a copying getter.
#define MX_EXPORT_NUM_FIELD(name)                                \
  .def("has_" #name, &mx::logging::LogEntry::has_##name)         \
      .def("clear_" #name, &mx::logging::LogEntry::clear_##name) \
      .def_property(#name, &mx::logging::LogEntry::name, &mx::logging::LogEntry::set_##name)
#define MX_EXPORT_STR_FIELD(name)                                                                                     \
  .def("has_" #name, &mx::logging::LogEntry::has_##name)                                                              \
      .def("clear_" #name, &mx::logging::LogEntry::clear_##name)                                                      \
      .def_property(#name, pybind11::cpp_function(&mx::logging::LogEntry::name, pybind11::return_value_policy::copy), \
                    pybind11::cpp_function((void(mx::logging::LogEntry::*)(const std::string&)) &                     \
                                           mx::logging::LogEntry::set_##name))
  pybind11::class_<mx::logging::LogEntry>(module, "LogEntry")
      .def(pybind11::init<>()) MX_EXPORT_NUM_FIELD(id) MX_EXPORT_NUM_FIELD(timestamp) MX_EXPORT_NUM_FIELD(level)
          MX_EXPORT_NUM_FIELD(verbosity) MX_EXPORT_NUM_FIELD(data_type) MX_EXPORT_NUM_FIELD(source_line)
              MX_EXPORT_NUM_FIELD(pid) MX_EXPORT_STR_FIELD(context) MX_EXPORT_STR_FIELD(text)
                  MX_EXPORT_STR_FIELD(workflow) MX_EXPORT_STR_FIELD(data_class)
      .def("has_data", &mx::logging::LogEntry::has_data)
      .def("clear_data", &mx::logging::LogEntry::clear_data)
      .def_property(
          "data", [](const mx::logging::LogEntry& entry) { return pybind11::bytes(entry.data()); },
          [](mx::logging::LogEntry& entry, pybind11::bytes data) { entry.set_data(std::string(data)); })
          MX_EXPORT_STR_FIELD(source_file) MX_EXPORT_STR_FIELD(compilation_datetime) MX_EXPORT_STR_FIELD(version);
#undef MX_EXPORT_NUM_FIELD
#undef MX_EXPORT_STR_FIELD

  module.def(
      "emit_log",
      [](const mx::logging::LogEntry& entry, unsigned int flags) { mx::logging::impl::emit_log(entry, flags); },
      pybind11::arg("entry"), pybind11::arg("flags") = 0, "Write an entry to stderr and to the logging stream.");

  pybind11::class_<multiplexer::Client::NotConnected>(module, "Client_NotConnected");

  static pybind11::exception<std::exception> MultiplexerClientErrorException(module, "MultiplexerClientError");
  static pybind11::exception<multiplexer::Client::NotConnected> NotConnectedException(module, "NotConnected",
                                                                                      MultiplexerClientErrorException);
  static pybind11::exception<multiplexer::Client::UsedAfterFork> UsedAfterForkException(module, "UsedAfterFork",
                                                                                        NotConnectedException);
  static pybind11::exception<multiplexer::Client::OperationTimedOut> OperationTimedOutException(
      module, "OperationTimedOut", MultiplexerClientErrorException);
  static pybind11::exception<multiplexer::Client::OperationFailed> OperationFailedException(
      module, "OperationFailed", MultiplexerClientErrorException);
  multiplexer::py_not_connected = &NotConnectedException;
  multiplexer::py_operation_timed_out = &OperationTimedOutException;
  multiplexer::py_operation_failed = &OperationFailedException;

  // Map the C++ exceptions to the Python classes registered above, so that
  // callers can catch NotConnected, OperationTimedOut and OperationFailed.
  pybind11::register_exception_translator([](std::exception_ptr p) {
    try {
      if (p) {
        std::rethrow_exception(p);
      }
    } catch (const multiplexer::Client::UsedAfterFork& e) {
      PyErr_SetString(UsedAfterForkException.ptr(), e.what());
    } catch (const multiplexer::Client::NotConnected& e) {
      PyErr_SetString(NotConnectedException.ptr(), e.what());
    } catch (const multiplexer::Client::OperationTimedOut& e) {
      PyErr_SetString(OperationTimedOutException.ptr(), e.what());
    } catch (const multiplexer::Client::OperationFailed& e) {
      PyErr_SetString(OperationFailedException.ptr(), e.what());
    }
  });

  pybind11::enum_<multiplexer::DropReason>(module, "DropReason",
                                           "Why a client gave up on a message the program sent; see the clients' "
                                           "on_drop and docs/semantics.md.")
      .value("NO_ROOM", multiplexer::DropReason::NO_ROOM,
             "It waited for room on a full connection past its timeout, or, sent with no time to wait, found none.")
      .value("NO_CONNECTION", multiplexer::DropReason::NO_CONNECTION,
             "It waited for a connection to come up past its timeout, or, sent with no time to wait, found none live.")
      .value("CONNECTION_LOST", multiplexer::DropReason::CONNECTION_LOST,
             "Its connection ended and nothing else could take it: a pinned lane's, a copy for ALL.")
      .value("SHUT_DOWN", multiplexer::DropReason::SHUT_DOWN, "The client shut down before it went.");

  pybind11::class_<multiplexer::ConnectionWrapper>(
      module, "ConnectionWrapper", "A connection to one multiplexer, held weakly: false once it is gone.")
      .def("__bool__", &multiplexer::ConnectionWrapper::operator bool)
      .def_property_readonly(
          "endpoint",
          [](const multiplexer::ConnectionWrapper& wrapper) {
            return mxtyping::Tuple<pybind11::str, pybind11::int_>(
                pybind11::make_tuple(wrapper.endpoint().address().to_string(), wrapper.endpoint().port()));
          },
          "The multiplexer's (host, port), kept after the connection is gone.");

  pybind11::class_<multiplexer::Lane, multiplexer::LanePtr>(
      module, "Lane", "One connection for a stream of messages; see multiplexer.mxclient.Client.lane().")
      .def(pybind11::init<bool>(), pybind11::arg("pinned") = false)
      .def(pybind11::init<const multiplexer::ConnectionWrapper&, bool>(), pybind11::arg("connection"),
           pybind11::arg("pinned") = false)
      .def_property_readonly("pinned", &multiplexer::Lane::pinned,
                             "Whether the lane keeps its first connection for good.")
      .def_property_readonly("connection", &multiplexer::Lane::connection,
                             "The connection held, a ConnectionWrapper; empty until the first message went through.")
      .def_property_readonly("holds_connection", &multiplexer::Lane::holds_connection,
                             "Whether a connection was ever written into the lane.")
      .def_property_readonly("connected", &multiplexer::Lane::connected, "Whether the connection held is live.")
      .def_property_readonly("closed", &multiplexer::Lane::closed,
                             "A pinned lane whose connection is gone: nothing goes through it any more.")
      .def("adopt", &multiplexer::Lane::adopt,
           "Write the connection a message went through; the synchronous client's algorithm calls it.");

  module.def("test_connection_wrapper", test_connection_wrapper);

  pybind11::class_<multiplexer::PythonClient /*, std::shared_ptr<PythonClient>*/>(
      module, "Client", "The synchronous client; see multiplexer.mxclient.Client for the Python API.")
      .def(pybind11::init<std::uint32_t>(), pybind11::arg("peer_type"))

      .def("_get_instance_id", &multiplexer::PythonClient::instance_id)

      .def("async_connect_to",
           (multiplexer::ConnectionWrapper(multiplexer::PythonClient::*)(const std::string&, std::uint16_t)) &
               multiplexer::PythonClient::async_connect,
           pybind11::arg("host"), pybind11::arg("port"))

      .def("connect_to",
           (multiplexer::ConnectionWrapper(multiplexer::PythonClient::*)(const std::string&, std::uint16_t, float)) &
               multiplexer::PythonClient::connect,
           pybind11::arg("host"), pybind11::arg("port"), pybind11::arg("timeout"))

      .def("disconnect_from",
           (bool(multiplexer::PythonClient::*)(const std::string&, std::uint16_t)) &
               multiplexer::PythonClient::disconnect,
           pybind11::arg("host"), pybind11::arg("port"),
           "Drop the multiplexer given to connect_to() with this host and port; see "
           "multiplexer.mxclient.Client.disconnect.")

      .def("wait_for_connection", &multiplexer::PythonClient::wait_for_connection, pybind11::arg("connection"),
           pybind11::arg("timeout"))
      .def("connections_count", &multiplexer::PythonClient::connections_count)
      .def(
          "set_routing_serialized",
          [](multiplexer::PythonClient& client, pybind11::bytes serialized) {
            multiplexer::Routing routing;
            if (!routing.ParseFromString(serialized)) {
              throw std::invalid_argument("not a serialized Routing");
            }
            client.set_routing(routing);
          },
          pybind11::arg("serialized"))
      .def("routing_acknowledged", &multiplexer::PythonClient::routing_acknowledged)
      .def("has_incoming_messages", &multiplexer::PythonClient::has_incoming_messages)
      .def("orphaned", &multiplexer::PythonClient::orphaned,
           "Whether the client was made before a fork this process is the child of, where its calls raise "
           "UsedAfterFork and shutdown() closes only the child's copies of its connections; as ThreadedClient's.")
      .def("refuse_arrivals", &multiplexer::PythonClient::refuse_arrivals,
           "Leaving, for the server classes: from now on a request that arrives is refused at once with "
           "DELIVERY_ERROR, so that its sender retries elsewhere; a reply, and the protocol's own messages, are "
           "dropped. What was read before stays to be received.")
      .def("refuse_unread", &multiplexer::PythonClient::refuse_unread,
           "The messages read and not received yet, refused or dropped as refuse_arrivals() does: for a server "
           "that will not handle them, at its close.")
      .def("dropped_while_closing", &multiplexer::PythonClient::dropped_while_closing,
           "The messages the client's connections read after they began closing, which they could only drop, "
           "each connection's logged as a WARNING when it ends; the protocol's own answers to what the client "
           "sent are not counted. Kept after shutdown(): what a server's close dropped, as the C++ client's.")
      .def("shutdown", &multiplexer::PythonClient::shutdown,
           pybind11::arg("timeout") = multiplexer::CLOSE_FLUSH_SECONDS)
      .def("bind_to_current_thread", &multiplexer::PythonClient::bind_to_current_thread)

      .def("read_raw_message", &multiplexer::PythonClient::read_message, pybind11::arg("timeout"))

      .def("_schedule_all", &multiplexer::PythonClient::schedule_all, pybind11::arg("serialized"),
           pybind11::arg("timeout") = multiplexer::DEFAULT_TIMEOUT)
      .def("wait_for_any_connection", &multiplexer::PythonClient::wait_for_any_connection, pybind11::arg("timeout"))
      .def("read_raw_message_watching", &multiplexer::PythonClient::read_message_watching, pybind11::arg("timeout"),
           pybind11::arg("watch"))

      .def("send", &multiplexer::PythonClient::send, pybind11::arg("serialized"), pybind11::arg("all"),
           pybind11::arg("lane"), pybind11::arg("timeout"), pybind11::arg("callback") = pybind11::none())
      .def("send_one", &multiplexer::PythonClient::send_one, pybind11::arg("serialized"), pybind11::arg("preferred"),
           pybind11::arg("lane"), pybind11::arg("timeout"))
      .def("send_all_and_wait", &multiplexer::PythonClient::send_all_and_wait, pybind11::arg("serialized"),
           pybind11::arg("timeout"))

      .def("flush_all", &multiplexer::PythonClient::flush_all, pybind11::arg("timeout"))

      .def("random", &multiplexer::PythonClient::random64)
      .def("_set_drop_observer", &multiplexer::PythonClient::set_drop_observer, pybind11::arg("observer"))
      .def("_dropped", &multiplexer::PythonClient::dropped);

  pybind11::class_<multiplexer::PythonThreadedClient>(
      module, "ThreadedClient",
      "A client with an io thread of its own; see multiplexer.threaded_client for the Python API.")
      .def(pybind11::init<std::uint32_t, pybind11::object>(), pybind11::arg("peer_type"), pybind11::arg("on_message"))
      .def("_set_drop_observer", &multiplexer::PythonThreadedClient::set_drop_observer, pybind11::arg("observer"))
      .def("_dropped", [](multiplexer::PythonThreadedClient& client) { return client.client.dropped(); })
      .def("instance_id", [](const multiplexer::PythonThreadedClient& client) { return client.client.instance_id(); })
      .def(
          "orphaned", [](const multiplexer::PythonThreadedClient& client) { return client.client.orphaned(); },
          "Whether the client was made before a fork this process is the child of: every call raises "
          "UsedAfterFork.")
      .def("random", [](multiplexer::PythonThreadedClient& client) { return client.client.random64(); })
      .def("connect", &multiplexer::PythonThreadedClient::connect, pybind11::arg("host"), pybind11::arg("port"),
           pybind11::arg("timeout"))
      .def("disconnect", &multiplexer::PythonThreadedClient::disconnect, pybind11::arg("host"), pybind11::arg("port"),
           "Drop the multiplexer given to connect() with this host and port; see "
           "multiplexer.threaded_client.ThreadedClient.disconnect.")
      .def("connections_count", &multiplexer::PythonThreadedClient::connections_count)
      .def("set_routing_serialized", &multiplexer::PythonThreadedClient::set_routing_serialized,
           pybind11::arg("serialized"))
      .def("routing_acknowledged", &multiplexer::PythonThreadedClient::routing_acknowledged)
      .def("flush_all", &multiplexer::PythonThreadedClient::flush_all, pybind11::arg("timeout"))
      .def("set_search_policy", &multiplexer::PythonThreadedClient::set_search_policy, pybind11::arg("answer"))
      .def("send", &multiplexer::PythonThreadedClient::send, pybind11::arg("serialized"),
           pybind11::arg("lane") = multiplexer::LanePtr(), pybind11::arg("timeout") = multiplexer::DEFAULT_TIMEOUT,
           pybind11::arg("callback") = pybind11::none())
      .def("send_all", &multiplexer::PythonThreadedClient::send_all, pybind11::arg("serialized"),
           pybind11::arg("timeout") = multiplexer::DEFAULT_TIMEOUT, pybind11::arg("callback") = pybind11::none())
      .def("send_and_wait", &multiplexer::PythonThreadedClient::send_and_wait, pybind11::arg("serialized"),
           pybind11::arg("all"), pybind11::arg("timeout"), pybind11::arg("lane") = multiplexer::LanePtr())
      .def("flush_all_and_notify", &multiplexer::PythonThreadedClient::flush_all_and_notify, pybind11::arg("timeout"),
           pybind11::arg("callback"))
      .def("send_and_notify", &multiplexer::PythonThreadedClient::send_and_notify, pybind11::arg("serialized"),
           pybind11::arg("all"), pybind11::arg("timeout"), pybind11::arg("callback"),
           pybind11::arg("lane") = multiplexer::LanePtr())
      .def("query", &multiplexer::PythonThreadedClient::query, pybind11::arg("serialized"), pybind11::arg("timeout"),
           pybind11::arg("lane") = multiplexer::LanePtr(), pybind11::arg("on_received") = pybind11::none())
      .def("query_with_callback", &multiplexer::PythonThreadedClient::query_with_callback, pybind11::arg("serialized"),
           pybind11::arg("callback"), pybind11::arg("timeout"), pybind11::arg("lane") = multiplexer::LanePtr(),
           pybind11::arg("on_received") = pybind11::none())
      .def("shutdown", &multiplexer::PythonThreadedClient::shutdown,
           pybind11::arg("timeout") = multiplexer::CLOSE_FLUSH_SECONDS);

  multiplexer::main_thread_ident.store(
      pybind11::module_::import("threading").attr("main_thread")().attr("ident").cast<unsigned long>());
  // A forked child has none of the parent's threads, so none of the
  // callbacks and waits they counted: begin_exit() would wait two seconds
  // for them at every child's exit. Its main thread is the one that forked.
  pthread_atfork(nullptr, nullptr, [] {
    multiplexer::gil_takers.store(0);
    multiplexer::main_thread_ident.store(PyThread_get_thread_ident());
  });
  module.def(
      "begin_exit",
      [] {
        multiplexer::main_thread_ident.store(
            PyThread_get_thread_ident());  // the atexit hooks run on the finalizing thread
        multiplexer::exiting.store(true);
        // Callbacks and waits already past their check hold or are about
        // to take the GIL: give it up and let them finish, within reason.
        pybind11::gil_scoped_release release;
        for (int waited_ms = 0; waited_ms < 2000 && multiplexer::gil_takers.load() > 0; ++waited_ms) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
      },
      "Tell the binding the interpreter is exiting: threads coming back from blocking waits park from now on, "
      "no callback into Python starts, and the ones in progress are waited for. "
      "Registered with atexit by mxclient.py.");
  module.def(
      "_gil_takers", [] { return multiplexer::gil_takers.load(); },
      "Threads past their exit check and taking or holding the GIL from C++ (callbacks, returns from blocking "
      "waits), which begin_exit() waits for: for tests.");
  module.def("heap_in_use_bytes", &mx::heap_in_use_bytes,
             "Bytes currently allocated from the C heap by this process (exact, glibc mallinfo2).");

  module.attr("DEFAULT_INCOMING_QUEUE_MAX_SIZE") = pybind11::int_(multiplexer::DEFAULT_INCOMING_QUEUE_MAX_SIZE);
  module.attr("FORCED_FRAMES_PAST_FULL_QUEUE") = pybind11::int_(multiplexer::FORCED_FRAMES_PAST_FULL_QUEUE);
  module.attr("AUTO_RECONNECT_TIME") = pybind11::int_(multiplexer::AUTO_RECONNECT_TIME);
  module.attr("DEFAULT_TIMEOUT") = pybind11::float_(multiplexer::DEFAULT_TIMEOUT);
  module.attr("ROOM_GRACE_SECONDS") = pybind11::float_(multiplexer::ROOM_GRACE_SECONDS);
  module.attr("CLOSE_FLUSH_SECONDS") = pybind11::float_(multiplexer::CLOSE_FLUSH_SECONDS);
  module.attr("MAX_MESSAGE_SIZE") = pybind11::int_(multiplexer::MAX_MESSAGE_SIZE);
  module.attr("HEARTBIT_INTERVAL") = pybind11::float_(multiplexer::HEARTBIT_INTERVAL);
  module.attr("MAX_SECONDS") = pybind11::float_(mx::MAX_SECONDS);
  module.attr("CLOSE_READ_SECONDS") = pybind11::float_(multiplexer::CLOSE_READ_SECONDS);
  module.attr("NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL") =
      pybind11::float_(multiplexer::NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL);
  module.attr("NO_HEARTBIT_SO_REALLY_DROP_INTERVAL") =
      pybind11::float_(multiplexer::NO_HEARTBIT_SO_REALLY_DROP_INTERVAL);
  module.attr("KEEPALIVE_PROBE_INTERVAL") = pybind11::float_(multiplexer::KEEPALIVE_PROBE_INTERVAL);
}
