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
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "lib/logging/logging.h"
#include "lib/memory.h"
#include "lib/type_utils.h"
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
  explicit PythonClient(boost::uint32_t client_type) : Client(client_type) {}

  pybind11::object read_message(float timeout) {
    BOOST_STATIC_ASSERT(
        (boost::is_same<BasicClient::IncomingMessagesBuffer ::value_type::second_type, ConnectionWrapper>::value));
    BasicClient::IncomingMessagesBuffer::value_type next;
    {
      GilRelease release;
      next = Client::read_raw_message(timeout);
    }
    return pybind11::make_tuple((pybind11::bytes)next.first->get_message(), next.second);
  }

  ScheduledMessageTracker schedule_one(std::string message) { return Client::schedule_one(&message); }
  // schedule_one that also reports the connection used, for the resend logic
  // in mxclient.send_and_receive.
  pybind11::tuple schedule_one_used(std::string message) {
    ConnectionWrapper used;
    basic_client_->poll(); // as Client::schedule_one does
    ScheduledMessageTracker tracker(basic_client_->schedule_one(_serialize(&message), &used));
    return pybind11::make_tuple(tracker, used);
  }
  bool wait_for_any_connection(float timeout) {
    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    GilRelease release;
    return basic_client_->wait_for_any_connection(*timer);
  }
  // read_message that returns None as soon as `watch` dies.
  pybind11::object read_message_watching(float timeout, ConnectionWrapper watch) {
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
      return pybind11::none();
    return pybind11::make_tuple((pybind11::bytes)next.first->get_message(), next.second);
  }
  ScheduledMessageTracker schedule_one(std::string message, ConnectionWrapper w, float timeout) {
    return Client::schedule_one(&message, w, timeout);
  }

  unsigned int schedule_all(std::string message) { return Client::schedule_all(&message); }
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
  PythonThreadedClient(boost::uint32_t peer_type, pybind11::object on_message)
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

  bool connect(const std::string &host, boost::uint16_t port, float timeout) {
    GilRelease release;
    return client.connect(host, port, timeout);
  }
  unsigned int connections_count() {
    GilRelease release;
    return client.connections_count();
  }
  // The non-flushing sends only post to the io thread, so they need no GIL
  // release and are safe from callbacks; the flushing one waits.
  void send(std::string serialized) { client.send_serialized(serialized); }
  void send_all(std::string serialized) { client.send_all_serialized(serialized); }
  unsigned int send_and_wait(std::string serialized, bool all, float timeout) {
    GilRelease release;
    return client.send_serialized_and_wait(serialized, all, timeout);
  }
  pybind11::tuple query(std::string payload, boost::uint32_t type, float timeout) {
    ThreadedClient::Result result;
    {
      GilRelease release;
      result = client.query(payload, type, timeout);
    }
    result.check(); // throws the C++ exception, translated below
    return pybind11::make_tuple((pybind11::bytes)result.reply.first->get_message(), result.reply.second);
  }
  void query_with_callback(std::string payload, boost::uint32_t type, pybind11::function callback, float timeout) {
    std::shared_ptr<pybind11::function> held(new pybind11::function(callback), [](pybind11::function *function) {
      CallbackSlot slot;
      if (!slot.ok)
        return; // leaked on purpose, see sink_for
      pybind11::gil_scoped_acquire acquire;
      delete function;
    });
    client.query(
        payload, type,
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
        timeout);
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
  module.def("should_log", &mx::logging::impl::should_log);
  module.def("current_timestamp", &mx::logging::impl::current_timestamp);
  module.def("emit_log", (void (*)(const mx::logging::LogEntry &, unsigned int flags)) & mx::logging::impl::emit_log);
  module.def("set_logging_file", &mx::logging::set_logging_file);
  module.def("set_logging_fd", [](unsigned int logging_fd, bool close_on_delete) {
    mx::logging::set_logging_fd(logging_fd, close_on_delete);
  });
  module.def("create_log_id", &mx::logging::create_log_id);
  module.def("process_context", &mx::logging::process_context, pybind11::return_value_policy::copy);
  module.def("set_process_context", &mx::logging::set_process_context);
  module.def("set_process_context_program_name", &mx::logging::set_process_context_program_name);
  module.def("set_maximal_logging_verbosity", &mx::logging::set_maximal_logging_verbosity);

#define export_constant_level_or_verbosity(r, d, tup)                                                                  \
  module.attr(BOOST_PP_STRINGIZE(BOOST_PP_TUPLE_ELEM(2, 0, tup))) = BOOST_PP_TUPLE_ELEM(2, 1, tup);

  BOOST_PP_SEQ_FOR_EACH(export_constant_level_or_verbosity, ~, MX_LOGGING_LEVELS_SEQ);
  BOOST_PP_SEQ_FOR_EACH(export_constant_level_or_verbosity, ~, MX_LOGGING_VERBOSITIES_SEQ);

  pybind11::class_<mx::logging::LogEntry>(module, "LogEntry")
      .def(pybind11::init<>())
#define export_a_method(class, name) .def(BOOST_PP_STRINGIZE(name), &class ::name)
#define export_field_ctl(class, name)                                                                                  \
  export_a_method(class, BOOST_PP_CAT(has_, name)) export_a_method(class, BOOST_PP_CAT(clear_, name))

#define export_num_field(r, class, name)                                                                               \
  export_field_ctl(class, name).def_property(BOOST_PP_STRINGIZE(name), &class ::name, &class ::BOOST_PP_CAT(set_, name))

#define export_str_field(r, class, name)                                                                               \
  export_field_ctl(class, name)                                                                                        \
      .def_property(BOOST_PP_STRINGIZE(name), pybind11::cpp_function(&class ::name, pybind11::return_value_policy::copy),                           \
          pybind11::cpp_function((void(class ::*)(const std::string &)) & class ::BOOST_PP_CAT(set_, name)))

          BOOST_PP_SEQ_FOR_EACH(export_num_field, mx::logging::LogEntry,
                                (id)(timestamp)(level)(verbosity)(data_type)(source_line)(pid))
              BOOST_PP_SEQ_FOR_EACH(
                  export_str_field, mx::logging::LogEntry,
                  (context)(text)(workflow)(data_class)(data)(source_file)(compilation_datetime)(version));

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

  pybind11::class_<multiplexer::ConnectionWrapper>(module, "ConnectionWrapper")
      .def("__bool__", &multiplexer::ConnectionWrapper::operator bool);

  module.def("test_connection_wrapper", test_connection_wrapper);

  pybind11::class_<multiplexer::PythonClient /*, boost::shared_ptr<PythonClient>*/>(
      module, "Client", "The synchronous client; see multiplexer.mxclient.Client for the Python API.")
      .def(pybind11::init<boost::uint32_t>())

      .def("_get_instance_id", &multiplexer::PythonClient::instance_id)

      .def("async_connect",
           (multiplexer::ConnectionWrapper(multiplexer::PythonClient::*)(const std::string &, boost::uint16_t)) &
               multiplexer::PythonClient::async_connect)

      .def("connect",
           (multiplexer::ConnectionWrapper(multiplexer::PythonClient::*)(const std::string &, boost::uint16_t, float)) &
               multiplexer::PythonClient::connect)

      .def("wait_for_connection", &multiplexer::PythonClient::wait_for_connection)
      .def("connections_count", &multiplexer::PythonClient::connections_count)
      .def("shutdown", &multiplexer::PythonClient::shutdown)
      .def("bind_to_current_thread", &multiplexer::PythonClient::bind_to_current_thread)

      .def("read_message", &multiplexer::PythonClient::read_message)

      .def("schedule_one", (ScheduledMessageTracker(multiplexer::PythonClient::*)(std::string)) &
                               multiplexer::PythonClient::schedule_one)

      .def("schedule_one",
           (ScheduledMessageTracker(multiplexer::PythonClient::*)(std::string, multiplexer::ConnectionWrapper, float)) &
               multiplexer::PythonClient::schedule_one)

      .def("schedule_all", &multiplexer::PythonClient::schedule_all)
      .def("schedule_one_used", &multiplexer::PythonClient::schedule_one_used)
      .def("wait_for_any_connection", &multiplexer::PythonClient::wait_for_any_connection)
      .def("read_message_watching", &multiplexer::PythonClient::read_message_watching)

      .def("flush", (void(multiplexer::PythonClient::*)(ScheduledMessageTracker, float) const) &
                        multiplexer::PythonClient::flush)

      .def("flush_all", &multiplexer::PythonClient::flush_all)

      .def("random", &multiplexer::PythonClient::random64);

  pybind11::class_<multiplexer::PythonThreadedClient>(
      module, "ThreadedClient",
      "A client with an io thread of its own; see multiplexer.threaded_client for the Python API.")
      .def(pybind11::init<boost::uint32_t, pybind11::object>())
      .def("instance_id", [](const multiplexer::PythonThreadedClient &client) { return client.client.instance_id(); })
      .def("random", [](multiplexer::PythonThreadedClient &client) { return client.client.random64(); })
      .def("connect", &multiplexer::PythonThreadedClient::connect)
      .def("connections_count", &multiplexer::PythonThreadedClient::connections_count)
      .def("send", &multiplexer::PythonThreadedClient::send)
      .def("send_all", &multiplexer::PythonThreadedClient::send_all)
      .def("send_and_wait", &multiplexer::PythonThreadedClient::send_and_wait)
      .def("query", &multiplexer::PythonThreadedClient::query)
      .def("query_with_callback", &multiplexer::PythonThreadedClient::query_with_callback)
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
