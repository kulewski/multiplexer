// receivelogs: the smallest complete backend. Connects as
// LOG_RECEIVER_EXAMPLE, prints every message it receives in protobuf text
// format, and answers with a PING. With the example rules it receives what
// streamlogs sends; give any peer type a rule and it is a tap on that type.
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "lib/logging/logging.h"
#include "lib/protobuf/stream.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/client.h"
#include "mxcontrol/task.h"
#include "mxcontrol/tasks_holder.h"

using namespace std;
using namespace mx;
using namespace mx::logging;
using namespace multiplexer;

using mx::util::kwargs::Kwargs;

namespace mxcontrol {

class ReceiveLogs : public Task {
public:
  virtual int run();
  virtual std::string short_description() const { return "receive logs and print them on stdout"; }
  virtual void print_help(std::ostream &out) { out << "Start a Multiplexer backend that does something with logs.\n"; }

protected:
  virtual void _initialize_options_description(po::options_description &generic) {
    _add_multiplexer_client_options(generic);
  }

private:
};

REGISTER_MXCONTROL_SUBCOMMAND(receivelogs, mxcontrol::ReceiveLogs);

struct LogReceiverServer : public multiplexer::backend::BaseMultiplexerServer {

  LogReceiverServer(multiplexer::Client *conn, multiplexer::backend::PeerType type)
      : BaseMultiplexerServer(conn, type) {}

protected:
  virtual void handle_message(MultiplexerMessage &mxmsg) {
    std::string tf;
    google::protobuf::TextFormat::PrintToString(mxmsg, &tf);
    std::cout << tf << "\n";

    send_message(Kwargs().set("message", std::string()).set("type", types::PING).set("to", mxmsg.from()));
  }
};

int ReceiveLogs::run() {

  LogReceiverServer(&_multiplexer_client(peers::LOG_RECEIVER_EXAMPLE), peers::LOG_RECEIVER_EXAMPLE).serve_forever();

  return 0;
}

}; // namespace mxcontrol
