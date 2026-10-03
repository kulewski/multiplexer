// streamlogs: reads a binary log stream (length-prefixed LogEntry records,
// as written by --logging-file) from stdin and sends it in chunks as
// LOGS_STREAM messages through every --multiplexer given, as the peer type
// LOG_STREAMER. The Python library's logging.streaming spawns this. It
// never waits for the network: the program whose log this is blocks
// writing into the pipe as soon as nobody reads it, so a multiplexer that
// stops reading must not stop the reading here. Each chunk goes to every
// connection through a ThreadedClient's io thread, which holds a copy a
// multiplexer does not take for --timeout seconds and then drops it,
// counted. A chunk no log receiver takes, the multiplexers answer with a
// DELIVERY_ERROR, which is said in a WARNING of streamlogs' own.
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "lib/logging/logging.h"
#include "lib/protobuf/stream.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/threaded_client.h"
#include "mxcontrol/task.h"
#include "mxcontrol/tasks_holder.h"

using namespace std;
using namespace mx;
using namespace mx::logging;
using namespace multiplexer;

namespace mxcontrol {

class StreamLogs : public Task {
 public:
  virtual int run();
  virtual std::string short_description() const { return "stream logs from stdin to a Log Collector"; }
  virtual void print_help(std::ostream& out) {
    out << "Read a logging stream as produced by the mx logging library from "
           "stdin\n"
        << "and send collected records in chunks to a Log Collector.\n"
        << "\n"
        << _options();
  }

 protected:
  virtual void _initialize_options(mx::options::Options& options) {
    options.add("chunksize", &chunksize_, 32, "how many LogEntries send at a time; 0 for unlimited");
    options.add("timeout", &timeout_, DEFAULT_TIMEOUT,
                "seconds a chunk waits for a multiplexer that does not take it, after which that copy is dropped");
    _add_multiplexer_client_options(options);
  }

 private:
  int chunksize_;
  float timeout_;
};
REGISTER_MXCONTROL_SUBCOMMAND(streamlogs, mxcontrol::StreamLogs);

// The kinds of streamlogs' own lines among its client's lines about
// messages that went nowhere.
enum StreamLogsLine : unsigned int {
  NO_LOG_RECEIVER = BasicClient::OWN_LINES,  // a chunk a multiplexer had no log receiver for
  NOT_FOR_STREAMLOGS,                        // any other message: streamlogs takes none
};

// On the io thread, every message the multiplexers send streamlogs, at the
// rate of the client's lines about messages that went nowhere: the first
// of a kind at once, the rest as a count about once a second. A
// DELIVERY_ERROR answers a chunk no log receiver took; anything else is
// dropped.
static void on_message(ThreadedClient& client, const IncomingMessage& incoming) {
  const MultiplexerMessage& msg = *incoming.third;
  if (msg.type() == types::DELIVERY_ERROR) {
    if (client.drop_lines().first({NO_LOG_RECEIVER, WARNING, 0, 0},
                                  [] { return std::string("no log receiver took the log stream"); })) {
      MX_LOG(WARNING, LogSummary::VERBOSITY,
             TEXT("no log receiver took the log stream: multiplexer " + repr(msg.sender()) + " had none for chunk #" +
                  repr(msg.references())));
    }
    return;
  }
  if (client.drop_lines().first({NOT_FOR_STREAMLOGS, WARNING, msg.type(), 0}, [&] {
        return "messages of type " + repr(msg.type()) + " dropped: streamlogs takes none";
      })) {
    MX_LOG(WARNING, LogSummary::VERBOSITY,
           TEXT("message #" + repr(msg.id()) + " of type " + repr(msg.type()) + " dropped: streamlogs takes none"));
  }
}

// Hands `logs` to the io thread, a copy for every connection, and returns
// at once.
static inline void send_logs(ThreadedClient& client, const LogEntriesMessage& logs, float timeout) {
  MultiplexerMessage mxmsg = client.new_message(types::LOGS_STREAM, logs.SerializeAsString());
  mxmsg.set_logging_method(LoggingMethod::CONSOLE);
  MX_LOG(DEBUG, HIGHVERBOSITY,
         TEXT("sending LOGS_STREAM with " + repr(logs.log_size()) + " LogEntries (encoded on " +
              repr(mxmsg.message().size()) + " b)"));
  client.send_all_serialized(mxmsg.SerializeAsString(), mxmsg.id(), mxmsg.type(), timeout);
}

int StreamLogs::run() {
  using namespace mx::protobuf;

  FileMessageInputStream fmis(0);
  multiplexer::LogEntriesMessage logs;

  ThreadedClient client(peers::LOG_STREAMER,
                        [&client](const IncomingMessage& incoming) { on_message(client, incoming); });
  for (const std::pair<std::string, std::uint16_t>& address : _multiplexer_addresses()) {
    client.connect(address.first, address.second, 0);  // no wait: the io thread connects, and reconnects, on its own
  }

  while (fmis.read(*logs.add_log())) {
    // if chunksize_ == 0 (unlimited) this will be always false
    if (logs.log_size() == chunksize_) {
      send_logs(client, logs, timeout_);
      logs.clear_log();
    }
  }
  logs.mutable_log()->RemoveLast();

  if (logs.log_size()) {
    send_logs(client, logs, timeout_);
  }

  // What is still on its way gets the same time to go out.
  client.shutdown(timeout_);
  if (const std::uint64_t dropped = client.dropped()) {
    MX_LOG(WARNING, LOWVERBOSITY,
           TEXT(repr(dropped) + " copies of log chunks were dropped: no multiplexer took them within " +
                repr(timeout_) + " s"));
  }

  if (fmis.failed()) {
    // What came before is sent; that the stream broke off must not pass
    // for its end.
    MX_LOG(ERROR, LOWVERBOSITY,
           TEXT("the log stream on stdin broke off: an entry cut short or garbled, or a read error"));
    return 1;
  }
  return 0;
}

};  // namespace mxcontrol
