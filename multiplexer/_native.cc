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
template <typename... Types> using Tuple = pybind11::tuple;
template <typename Signature> using Callable = pybind11::function;
} // namespace mxtyping
#endif
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "lib/logging/logging.h"
#include "lib/memory.h"
#include "lib/type_utils.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/client.h"
#include "multiplexer/multiplexer.constants.h"
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
static unsigned long main_thread_ident = 0;
// Threads past the finalization check and about to take the GIL, or
// holding it: a callback into Python from the io thread, or a thread coming
// back from a blocking wait. begin_exit() waits for this to reach zero
// before it returns, so that finalization never starts under one of them.
// See CallbackSlot and GilRelease.
static std::atomic<int> gil_takers(0);

static bool python_is_finalizing() {
  if (exiting.load())
    return true;
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
    if (!ok)
      gil_takers.fetch_sub(1);
  }
  ~CallbackSlot() {
    if (ok)
      gil_takers.fetch_sub(1);
  }
  bool ok; // false: the interpreter is leaving; do nothing
};

// Sleeps forever: for a thread that must not take the GIL again. A daemon
// thread parked here does not delay the exit, and a non-daemon one was
// joined before the exit began.
[[noreturn]] static void park_forever() {
  for (;;)
    pause();
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
    if (python_is_finalizing() && PyThread_get_thread_ident() != main_thread_ident) {
      gil_takers.fetch_sub(1);
      park_forever();
    }
    PyEval_RestoreThread(state_);
    gil_takers.fetch_sub(1);
  }
  GilRelease(const GilRelease &) = delete;
  GilRelease &operator=(const GilRelease &) = delete;

private:
  PyThreadState *state_;
};

// Client with the few signatures pybind11 needs: strings instead of
// templates, and a read that releases the GIL.
struct PythonClient : public Client {

public:
  // The io_service is owned here, not by a Python attribute: CPython clears
  // an instance's attributes before the base type's deallocator runs, so a
  // Client that only borrowed it would shut down on freed memory.
  explicit PythonClient(std::uint32_t client_type) : Client(client_type) {}

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

  ScheduledMessageTracker schedule_one(pybind11::bytes serialized) {
    std::string message(serialized);
    return Client::schedule_one(&message);
  }
  // schedule_one that also reports the connection used, for the resend logic
  // in mxclient.send_and_receive. `pinned`: the message belongs to a pinned
  // lane and must not be handed to another connection if this one dies.
  mxtyping::Tuple<ScheduledMessageTracker, ConnectionWrapper> schedule_one_used(pybind11::bytes serialized,
                                                                                bool pinned) {
    std::string message(serialized);
    ConnectionWrapper used;
    basic_client_->poll(); // as Client::schedule_one does
    shared_ptr<const RawMessage> raw = _serialize(&message);
    if (pinned)
      raw->mark_pinned();
    ScheduledMessageTracker tracker(basic_client_->schedule_one(raw, &used));
    return mxtyping::Tuple<ScheduledMessageTracker, ConnectionWrapper>(pybind11::make_tuple(tracker, used));
  }
  bool wait_for_any_connection(float timeout) {
    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    GilRelease release;
    return basic_client_->wait_for_any_connection(*timer);
  }
  // read_message that returns None as soon as `watch` dies.
  // A (bytes, connection) pair, or None once `watch` is gone.
  std::optional<mxtyping::Tuple<pybind11::bytes, ConnectionWrapper>> read_message_watching(float timeout,
                                                                                           ConnectionWrapper watch) {
    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    BasicClient::IncomingMessagesBuffer::value_type next;
    bool got;
    {
      GilRelease release;
      got = basic_client_->wait_for_incoming_message_or_loss(*timer, watch);
      if (got)
        next = basic_client_->next_incoming_message();
    }
    if (!got)
      return std::nullopt;
    return mxtyping::Tuple<pybind11::bytes, ConnectionWrapper>(
        pybind11::make_tuple((pybind11::bytes)next.first->get_message(), next.second));
  }
  ScheduledMessageTracker schedule_one(pybind11::bytes serialized, ConnectionWrapper w, float timeout) {
    std::string message(serialized);
    return Client::schedule_one(&message, w, timeout);
  }
  // schedule_one on `w` only, no reconnect: null when it is gone or full;
  // `pinned` as for schedule_one_used.
  ScheduledMessageTracker schedule_on(pybind11::bytes serialized, ConnectionWrapper w, bool pinned) {
    std::string message(serialized);
    basic_client_->poll();
    if (!w)
      return ScheduledMessageTracker(BasicScheduledMessageTracker());
    shared_ptr<const RawMessage> raw = _serialize(&message);
    if (pinned)
      raw->mark_pinned();
    try {
      return Client::schedule_one(raw, w, 0); // no reconnect: a gone connection throws
    } catch (NotConnected &) {
      return ScheduledMessageTracker(BasicScheduledMessageTracker());
    }
  }

  unsigned int schedule_all(pybind11::bytes serialized) {
    std::string message(serialized);
    return Client::schedule_all(&message);
  }
};

// The probe an addressed query locates its addressee with, as the Python
// side names it: the message type it sends.
static Probe probe_from_type(std::uint32_t type) {
  if (type == types::PING)
    return PROBE_PING;
  if (type == types::BACKEND_FOR_PACKET_SEARCH)
    return PROBE_SEARCH;
  throw std::invalid_argument("probe must be types.BACKEND_FOR_PACKET_SEARCH or types.PING");
}

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
    std::shared_ptr<pybind11::object> held(new pybind11::object(callback), [](pybind11::object *object) {
      CallbackSlot slot;
      if (!slot.ok)
        return; // leaked on purpose: the GIL is out of reach, and the process is going
      pybind11::gil_scoped_acquire acquire;
      delete object;
    });
    return [held](const IncomingMessage &incoming) {
      CallbackSlot slot;
      if (!slot.ok)
        return; // the callback would need the GIL; nothing to deliver to any more
      pybind11::gil_scoped_acquire acquire;
      try {
        (*held)((pybind11::bytes)incoming.first->get_message(), incoming.second);
      } catch (pybind11::error_already_set &error) {
        error.restore();
        PyErr_Print();
      }
    };
  }
  ~PythonThreadedClient() {
    GilRelease release;
    client.shutdown();
  }
  // `answer` is a Python callable returning whether to answer a search
  // for a backend; runs on the io thread with the GIL. See
  // ThreadedClient::set_search_policy.
  void set_search_policy(mxtyping::Callable<bool()> answer) {
    std::shared_ptr<pybind11::object> held(new pybind11::object(answer), [](pybind11::object *object) {
      CallbackSlot slot;
      if (!slot.ok)
        return; // leaked on purpose, see sink_for
      pybind11::gil_scoped_acquire acquire;
      delete object;
    });
    ThreadedClient::SearchPolicy policy = [held]() -> bool {
      CallbackSlot slot;
      if (!slot.ok)
        return false;
      pybind11::gil_scoped_acquire acquire;
      try {
        return (*held)().cast<bool>();
      } catch (pybind11::error_already_set &error) {
        error.restore();
        PyErr_Print();
        return false;
      }
    };
    GilRelease release;
    client.set_search_policy(policy);
  }

  bool connect(const std::string &host, std::uint16_t port, float timeout) {
    GilRelease release;
    return client.connect(host, port, timeout);
  }
  unsigned int connections_count() {
    GilRelease release;
    return client.connections_count();
  }
  // The non-flushing sends only post to the io thread, so they need no GIL
  // release and are safe from callbacks; the flushing one waits. `lane` is
  // a Lane or None.
  void send(pybind11::bytes serialized, std::optional<LanePtr> lane) {
    client.send_serialized(std::string(serialized), lane.value_or(LanePtr()));
  }
  void send_all(pybind11::bytes serialized) { client.send_all_serialized(std::string(serialized)); }
  unsigned int send_and_wait(pybind11::bytes payload, bool all, float timeout, std::optional<LanePtr> lane_given) {
    std::string serialized(payload);
    LanePtr lane = lane_given.value_or(LanePtr());
    GilRelease release;
    return client.send_serialized_and_wait(serialized, all, timeout, lane);
  }
  // The flushing send completed by `callback(written)` on the io thread
  // instead of a wait; the asyncio client's send. Held like a query's.
  void send_with_callback(pybind11::bytes payload, bool all, float timeout,
                          mxtyping::Callable<void(unsigned int)> callback, std::optional<LanePtr> lane_given) {
    std::string serialized(payload);
    LanePtr lane = lane_given.value_or(LanePtr());
    std::shared_ptr<pybind11::function> held(new pybind11::function(callback), [](pybind11::function *function) {
      CallbackSlot slot;
      if (!slot.ok)
        return; // leaked on purpose, see sink_for
      pybind11::gil_scoped_acquire acquire;
      delete function;
    });
    client.send_serialized_with_callback(
        serialized, all, timeout,
        [held](unsigned int written) {
          CallbackSlot slot;
          if (!slot.ok)
            return;
          pybind11::gil_scoped_acquire acquire;
          try {
            (*held)(written);
          } catch (pybind11::error_already_set &error) {
            error.restore();
            PyErr_Print();
          }
        },
        lane);
  }
  // The request as a serialized MultiplexerMessage, `to` included; `probe`
  // as a message type (see probe_from_type), `lane` a Lane or None.
  static MultiplexerMessage parse(const std::string &serialized) {
    MultiplexerMessage msg;
    if (!msg.ParseFromString(serialized))
      throw std::invalid_argument("not a serialized MultiplexerMessage");
    return msg;
  }
  mxtyping::Tuple<pybind11::bytes, ConnectionWrapper> query(pybind11::bytes serialized, float timeout,
                                                            std::uint32_t probe, std::optional<LanePtr> lane_given) {
    LanePtr lane = lane_given.value_or(LanePtr());
    MultiplexerMessage msg = parse(std::string(serialized));
    Probe how = probe_from_type(probe);
    ThreadedClient::Result result;
    {
      GilRelease release;
      result = client.query(msg, timeout, lane, how);
    }
    result.check(); // throws the C++ exception, translated below
    return mxtyping::Tuple<pybind11::bytes, ConnectionWrapper>(
        pybind11::make_tuple((pybind11::bytes)result.reply.first->get_message(), result.reply.second));
  }
  void query_with_callback(
      pybind11::bytes serialized,
      mxtyping::Callable<void(std::optional<pybind11::bytes>, std::optional<ConnectionWrapper>, pybind11::object)>
          callback,
      float timeout, std::uint32_t probe, std::optional<LanePtr> lane_given) {
    LanePtr lane = lane_given.value_or(LanePtr());
    MultiplexerMessage msg = parse(std::string(serialized));
    Probe how = probe_from_type(probe);
    std::shared_ptr<pybind11::function> held(new pybind11::function(callback), [](pybind11::function *function) {
      CallbackSlot slot;
      if (!slot.ok)
        return; // leaked on purpose, see sink_for
      pybind11::gil_scoped_acquire acquire;
      delete function;
    });
    client.query(
        msg,
        [held](const ThreadedClient::Result &result) {
          CallbackSlot slot;
          if (!slot.ok)
            return;
          pybind11::gil_scoped_acquire acquire;
          try {
            if (result.outcome == ThreadedClient::REPLIED)
              (*held)((pybind11::bytes)result.reply.first->get_message(), result.reply.second, pybind11::none());
            else
              (*held)(pybind11::none(), pybind11::none(), exception_for(result.outcome));
          } catch (pybind11::error_already_set &error) {
            // A callback that raises: print the Python traceback and go on,
            // as a thread's uncaught exception would be printed.
            error.restore();
            PyErr_Print();
          }
        },
        timeout, lane, how);
  }
  void shutdown() {
    GilRelease release;
    client.shutdown();
  }

  ThreadedClient client;
};
}; // namespace multiplexer

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
  module.def("process_context", &mx::logging::process_context, pybind11::return_value_policy::copy);
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
#define MX_EXPORT_NUM_FIELD(name)                                                                                      \
  .def("has_" #name, &mx::logging::LogEntry::has_##name)                                                               \
      .def("clear_" #name, &mx::logging::LogEntry::clear_##name)                                                       \
      .def_property(#name, &mx::logging::LogEntry::name, &mx::logging::LogEntry::set_##name)
#define MX_EXPORT_STR_FIELD(name)                                                                                      \
  .def("has_" #name, &mx::logging::LogEntry::has_##name)                                                               \
      .def("clear_" #name, &mx::logging::LogEntry::clear_##name)                                                       \
      .def_property(#name, pybind11::cpp_function(&mx::logging::LogEntry::name, pybind11::return_value_policy::copy),  \
                    pybind11::cpp_function((void(mx::logging::LogEntry::*)(const std::string &)) &                     \
                                           mx::logging::LogEntry::set_##name))
  pybind11::class_<mx::logging::LogEntry>(module, "LogEntry")
      .def(pybind11::init<>()) MX_EXPORT_NUM_FIELD(id) MX_EXPORT_NUM_FIELD(timestamp) MX_EXPORT_NUM_FIELD(level)
          MX_EXPORT_NUM_FIELD(verbosity) MX_EXPORT_NUM_FIELD(data_type) MX_EXPORT_NUM_FIELD(source_line)
              MX_EXPORT_NUM_FIELD(pid) MX_EXPORT_STR_FIELD(context) MX_EXPORT_STR_FIELD(text)
                  MX_EXPORT_STR_FIELD(workflow) MX_EXPORT_STR_FIELD(data_class)
      .def("has_data", &mx::logging::LogEntry::has_data)
      .def("clear_data", &mx::logging::LogEntry::clear_data)
      .def_property(
          "data", [](const mx::logging::LogEntry &entry) { return pybind11::bytes(entry.data()); },
          [](mx::logging::LogEntry &entry, pybind11::bytes data) { entry.set_data(std::string(data)); })
          MX_EXPORT_STR_FIELD(source_file) MX_EXPORT_STR_FIELD(compilation_datetime) MX_EXPORT_STR_FIELD(version);
#undef MX_EXPORT_NUM_FIELD
#undef MX_EXPORT_STR_FIELD

  module.def(
      "emit_log",
      [](const mx::logging::LogEntry &entry, unsigned int flags) { mx::logging::impl::emit_log(entry, flags); },
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
      if (p)
        std::rethrow_exception(p);
    } catch (const multiplexer::Client::UsedAfterFork &e) {
      PyErr_SetString(UsedAfterForkException.ptr(), e.what());
    } catch (const multiplexer::Client::NotConnected &e) {
      PyErr_SetString(NotConnectedException.ptr(), e.what());
    } catch (const multiplexer::Client::OperationTimedOut &e) {
      PyErr_SetString(OperationTimedOutException.ptr(), e.what());
    } catch (const multiplexer::Client::OperationFailed &e) {
      PyErr_SetString(OperationFailedException.ptr(), e.what());
    }
  });

  typedef multiplexer::PythonClient::ScheduledMessageTracker ScheduledMessageTracker;
  pybind11::class_<ScheduledMessageTracker>(module, "ScheduledMessageTracker")
      .def("__bool__", &ScheduledMessageTracker::operator bool)
      .def("in_queue", &ScheduledMessageTracker::in_queue)
      .def("is_sent", &ScheduledMessageTracker::is_sent)
      .def("is_lost", &ScheduledMessageTracker::is_lost);

  pybind11::class_<multiplexer::ConnectionWrapper>(
      module, "ConnectionWrapper", "A connection to one multiplexer, held weakly: false once it is gone.")
      .def("__bool__", &multiplexer::ConnectionWrapper::operator bool)
      .def_property_readonly(
          "endpoint",
          [](const multiplexer::ConnectionWrapper &wrapper) {
            return mxtyping::Tuple<pybind11::str, pybind11::int_>(
                pybind11::make_tuple(wrapper.endpoint().address().to_string(), wrapper.endpoint().port()));
          },
          "The multiplexer's (host, port), kept after the connection is gone.");

  pybind11::class_<multiplexer::Lane, multiplexer::LanePtr>(
      module, "Lane", "One connection for a stream of messages; see multiplexer.mxclient.Client.lane().")
      .def(pybind11::init<bool>(), pybind11::arg("pinned") = false)
      .def(pybind11::init<const multiplexer::ConnectionWrapper &, bool>(), pybind11::arg("connection"),
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
           (multiplexer::ConnectionWrapper(multiplexer::PythonClient::*)(const std::string &, std::uint16_t)) &
               multiplexer::PythonClient::async_connect,
           pybind11::arg("host"), pybind11::arg("port"))

      .def("connect_to",
           (multiplexer::ConnectionWrapper(multiplexer::PythonClient::*)(const std::string &, std::uint16_t, float)) &
               multiplexer::PythonClient::connect,
           pybind11::arg("host"), pybind11::arg("port"), pybind11::arg("timeout"))

      .def("wait_for_connection", &multiplexer::PythonClient::wait_for_connection, pybind11::arg("connection"),
           pybind11::arg("timeout"))
      .def("connections_count", &multiplexer::PythonClient::connections_count)
      .def("shutdown", &multiplexer::PythonClient::shutdown)
      .def("bind_to_current_thread", &multiplexer::PythonClient::bind_to_current_thread)

      .def("read_raw_message", &multiplexer::PythonClient::read_message, pybind11::arg("timeout"))

      .def("schedule_one",
           (ScheduledMessageTracker(multiplexer::PythonClient::*)(pybind11::bytes)) &
               multiplexer::PythonClient::schedule_one,
           pybind11::arg("serialized"))

      .def("schedule_one",
           (ScheduledMessageTracker(multiplexer::PythonClient::*)(pybind11::bytes, multiplexer::ConnectionWrapper,
                                                                  float)) &
               multiplexer::PythonClient::schedule_one,
           pybind11::arg("serialized"), pybind11::arg("connection"), pybind11::arg("timeout"))

      .def("schedule_all", &multiplexer::PythonClient::schedule_all, pybind11::arg("serialized"))
      .def("schedule_one_used", &multiplexer::PythonClient::schedule_one_used, pybind11::arg("serialized"),
           pybind11::arg("pinned") = false)
      .def("schedule_on", &multiplexer::PythonClient::schedule_on, pybind11::arg("serialized"),
           pybind11::arg("connection"), pybind11::arg("pinned") = false)
      .def("wait_for_any_connection", &multiplexer::PythonClient::wait_for_any_connection, pybind11::arg("timeout"))
      .def("read_raw_message_watching", &multiplexer::PythonClient::read_message_watching, pybind11::arg("timeout"),
           pybind11::arg("watch"))

      .def("flush",
           (void(multiplexer::PythonClient::*)(ScheduledMessageTracker, float) const) &
               multiplexer::PythonClient::flush,
           pybind11::arg("tracker"), pybind11::arg("timeout"))

      .def("flush_all", &multiplexer::PythonClient::flush_all, pybind11::arg("timeout"))

      .def("random", &multiplexer::PythonClient::random64);

  pybind11::class_<multiplexer::PythonThreadedClient>(
      module, "ThreadedClient",
      "A client with an io thread of its own; see multiplexer.threaded_client for the Python API.")
      .def(pybind11::init<std::uint32_t, pybind11::object>(), pybind11::arg("peer_type"), pybind11::arg("on_message"))
      .def("instance_id", [](const multiplexer::PythonThreadedClient &client) { return client.client.instance_id(); })
      .def("random", [](multiplexer::PythonThreadedClient &client) { return client.client.random64(); })
      .def("connect", &multiplexer::PythonThreadedClient::connect, pybind11::arg("host"), pybind11::arg("port"),
           pybind11::arg("timeout"))
      .def("connections_count", &multiplexer::PythonThreadedClient::connections_count)
      .def("set_search_policy", &multiplexer::PythonThreadedClient::set_search_policy, pybind11::arg("answer"))
      .def("send", &multiplexer::PythonThreadedClient::send, pybind11::arg("serialized"),
           pybind11::arg("lane") = multiplexer::LanePtr())
      .def("send_all", &multiplexer::PythonThreadedClient::send_all, pybind11::arg("serialized"))
      .def("send_and_wait", &multiplexer::PythonThreadedClient::send_and_wait, pybind11::arg("serialized"),
           pybind11::arg("all"), pybind11::arg("timeout"), pybind11::arg("lane") = multiplexer::LanePtr())
      .def("send_with_callback", &multiplexer::PythonThreadedClient::send_with_callback, pybind11::arg("serialized"),
           pybind11::arg("all"), pybind11::arg("timeout"), pybind11::arg("callback"),
           pybind11::arg("lane") = multiplexer::LanePtr())
      .def("query", &multiplexer::PythonThreadedClient::query, pybind11::arg("serialized"), pybind11::arg("timeout"),
           pybind11::arg("probe"), pybind11::arg("lane") = multiplexer::LanePtr())
      .def("query_with_callback", &multiplexer::PythonThreadedClient::query_with_callback, pybind11::arg("serialized"),
           pybind11::arg("callback"), pybind11::arg("timeout"), pybind11::arg("probe"),
           pybind11::arg("lane") = multiplexer::LanePtr())
      .def("shutdown", &multiplexer::PythonThreadedClient::shutdown);

  multiplexer::main_thread_ident = PyThread_get_thread_ident(); // the importing thread: the main one
  module.def(
      "begin_exit",
      [] {
        multiplexer::exiting.store(true);
        // Callbacks and waits already past their check hold or are about
        // to take the GIL: give it up and let them finish, within reason.
        pybind11::gil_scoped_release release;
        for (int waited_ms = 0; waited_ms < 2000 && multiplexer::gil_takers.load() > 0; ++waited_ms)
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
      },
      "Tell the binding the interpreter is exiting: threads coming back from blocking waits park from now on, "
      "no callback into Python starts, and the ones in progress are waited for. "
      "Registered with atexit by mxclient.py.");
  module.def("heap_in_use_bytes", &mx::heap_in_use_bytes,
             "Bytes currently allocated from the C heap by this process (exact, glibc mallinfo2).");

  module.attr("DEFAULT_INCOMING_QUEUE_MAX_SIZE") = pybind11::int_(multiplexer::DEFAULT_INCOMING_QUEUE_MAX_SIZE);
  module.attr("AUTO_RECONNECT_TIME") = pybind11::int_(multiplexer::AUTO_RECONNECT_TIME);
  module.attr("DEFAULT_TIMEOUT") = pybind11::float_(multiplexer::DEFAULT_TIMEOUT);
  module.attr("MAX_MESSAGE_SIZE") = pybind11::int_(multiplexer::MAX_MESSAGE_SIZE);
  module.attr("HEARTBIT_INTERVAL") = pybind11::float_(multiplexer::HEARTBIT_INTERVAL);
  module.attr("NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL") =
      pybind11::float_(multiplexer::NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL);
  module.attr("NO_HEARTBIT_SO_REALLY_DROP_INTERVAL") =
      pybind11::float_(multiplexer::NO_HEARTBIT_SO_REALLY_DROP_INTERVAL);
}
