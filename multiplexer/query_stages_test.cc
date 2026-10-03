// A query's stages when a connection is lost, in both C++ clients, against
// multiplexers of the test's own (StandIn), which script every answer. A
// request goes out at most twice, the request and the direct request, and a
// query never goes back a stage; a lost connection never proves an attempt
// dead, since it may have been routed first and its reply may come back
// another way, so a query fails before its time only when every attempt drew
// a delivery error. Each case failed on the clients before its fix: the
// search that finds nobody waits for the request a backend may have, where
// it failed at once; a lost direct request is waited for, where it went out
// again; an addressed query whose second request draws a delivery error
// waits for the first, where it failed; an addressed query whose request
// went with its connection takes the reply to the request sent again, where
// the C++ SyncClient sent it again at once; a request whose send ran out of
// time is still an attempt, its reply, written later, answering, where the
// synchronous clients threw it away; a request the client gave up on went
// nowhere and is struck off the late wait, which then ends at once; a pinned
// query waiting for a late reply ends NotConnected when the lane's
// connection goes, where it timed out; the request sent again is a copy of
// the request, its workflow kept, where the synchronous client built it anew
// from the type and payload; the synchronous client's search copies wait for
// room no longer than their stage, where they had 10 s; a search that waited
// for a connection and lost it at once waits on for the next, where the
// synchronous client placed it on nothing and waited out the stage; and a
// delivery error for the request, which a backend leaving sends back another
// way, strikes the request off and leaves the search, and the direct
// request's stage, on, where the synchronous clients took it for the
// multiplexer's "nobody" or the direct request's own and timed out. And in
// ThreadedClient: a searched connection that goes down is no answer, where
// it was one and the query failed or started over; a query waiting for a
// connection sends its own stage's message, where it started over; a request
// still unwritten when its connection died is followed where the outbox
// handed it, where it went out again; one held for the next connection
// counts as lost, the query moving on to the search, where it started over;
// and a delivery error for the request strikes it off, so that the query
// fails at once when nothing else can answer, where the late wait dropped it
// and ran out; and a query given an empty callback runs to its deadline,
// where it never ended, and returns after shutdown(), where it threw. And an
// addressed query whose request still waits for room at its one deadline, a
// connection live, ends OperationTimedOut, not NotConnected, which nothing
// pinned. Counted, not timed: the stand-ins record every message the client
// sent, every turn is a script's but a first stage's end in the cases only a
// stage that runs out of time reaches, a pinned query's late wait, a request
// left unwritten and an addressed query's deadline, and a bound on a wait
// only detects a failure.
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "multiplexer/client.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

using multiplexer::Client;
using multiplexer::IncomingMessage;
using multiplexer::MultiplexerMessage;
using multiplexer::RawMessage;
using multiplexer::ThreadedClient;
using multiplexer::testing::FILL_FRAMES;
using multiplexer::testing::fill_size;
namespace types = multiplexer::types;
namespace peers = multiplexer::peers;

namespace {

// Reads exactly `size` bytes into `at`: 0, -1 at the end of the stream, or the errno.
int read_exactly(int fd, void* at, std::size_t size) {
  char* into = static_cast<char*>(at);
  while (size) {
    const ssize_t got = ::recv(fd, into, size, 0);
    if (got == 0) {
      return -1;
    }
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      return errno;
    }
    into += got;
    size -= static_cast<std::size_t>(got);
  }
  return 0;
}

// Reads one frame: 0, -1 at the end of the stream, or the errno.
int read_frame(int fd, RawMessage* frame) {
  if (int error = read_exactly(fd, frame->get_header_buffer().data(), frame->get_header_buffer().size())) {
    return error;
  }
  if (!frame->unpack_header()) {
    return EPROTO;
  }
  if (int error = read_exactly(fd, frame->get_body_buffer().data(), frame->get_body_buffer().size())) {
    return error;
  }
  return frame->verify() ? 0 : EPROTO;
}

// Writes one frame whole: 0 or the errno.
int write_frame(int fd, const RawMessage& frame) {
  for (const asio::const_buffer& part : frame.get_message_buffer()) {
    const char* at = static_cast<const char*>(part.data());
    std::size_t size = part.size();
    while (size) {
      const ssize_t sent = ::send(fd, at, size, MSG_NOSIGNAL);
      if (sent < 0) {
        if (errno == EINTR) {
          continue;
        }
        return errno;
      }
      at += sent;
      size -= static_cast<std::size_t>(sent);
    }
  }
  return 0;
}

// A multiplexer of the test's own on 127.0.0.1, for one client: a thread
// accepts it, exchanges welcomes and then reads every frame it sends,
// keeping its messages, the protocol's own frames skipped, for the test's
// script to take in order (next) and to count afterwards (count); one
// made `stalled` reads nothing after the welcome until release(), so that
// what the client sends fills the socket's buffers and waits in the
// client. What the script writes goes to the client; hang_up() ends the
// connection.
class StandIn {
 public:
  explicit StandIn(bool stalled = false) : stalled_(stalled) {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof address;
    if (::bind(listener_, reinterpret_cast<sockaddr*>(&address), length) != 0 || ::listen(listener_, 1) != 0 ||
        ::getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
      ::close(listener_);
      throw std::runtime_error("no socket to listen on");
    }
    port = ntohs(address.sin_port);
    id = 0x5100 + port;
    ids_ = std::uint64_t(port) << 32;  // ids of its own: the client drops a repeated id as a copy
    reader_ = std::thread([this] { _read(); });
  }
  ~StandIn() {
    hang_up();
    ::shutdown(listener_, SHUT_RDWR);  // an accept still waiting returns
    reader_.join();
    if (fd_ >= 0) {
      ::close(fd_);
    }
    ::close(listener_);
  }

  // The client's next message into `msg`, within 30 s: false when none came.
  bool next(MultiplexerMessage* msg) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!changed_.wait_for(lock, std::chrono::seconds(30), [this] { return !unread_.empty() || ended_; }) ||
        unread_.empty()) {
      return false;
    }
    *msg = unread_.front();
    unread_.pop_front();
    return true;
  }

  // `msg` to the client: false when it could not be written.
  bool write(const MultiplexerMessage& msg) {
    std::unique_lock<std::mutex> lock(mutex_);
    changed_.wait_for(lock, std::chrono::seconds(30), [this] { return welcomed_ || ended_; });
    if (!welcomed_ || hung_up_) {
      return false;
    }
    const int fd = fd_;
    lock.unlock();
    return write_frame(fd, *std::unique_ptr<RawMessage>(RawMessage::FromMessage(msg))) == 0;
  }

  // A message of `type` from `sender` to the client, answering `references`.
  MultiplexerMessage answer(std::uint32_t type, std::uint64_t sender, std::uint64_t references) {
    MultiplexerMessage msg;
    msg.set_id(++ids_);
    msg.set_sender(sender);
    msg.set_to(client_.load());
    msg.set_type(type);
    msg.set_references(references);
    msg.set_message("late");
    return msg;
  }

  // Ends the client's connection, once.
  void hang_up() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (hung_up_) {
      return;
    }
    hung_up_ = true;
    if (fd_ >= 0) {
      ::shutdown(fd_, SHUT_RDWR);  // the reader returns; the client sees its end
    }
    changed_.notify_all();
  }

  // A stalled stand-in reads from now on, a multiplexer that caught up.
  void release() {
    std::lock_guard<std::mutex> lock(mutex_);
    released_ = true;
    changed_.notify_all();
  }

  // Waits until the client's connection ended, everything it sent read,
  // 30 s at most: whether it did.
  bool wait_ended() {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, std::chrono::seconds(30), [this] { return ended_; });
  }

  // How many messages of `type` the client sent so far, addressed `to`
  // when it is given.
  int count(std::uint32_t type, std::uint64_t to = ~0ULL) {
    std::lock_guard<std::mutex> lock(mutex_);
    int counted = 0;
    for (const MultiplexerMessage& msg : seen_) {
      counted += msg.type() == type && (to == ~0ULL || msg.to() == to);
    }
    return counted;
  }

  unsigned short port = 0;
  std::uint64_t id = 0;  // this multiplexer's instance id

 private:
  // The reader thread: the client's connection, the welcomes, then every
  // frame until the connection ends.
  void _read() {
    const int fd = ::accept(listener_, nullptr, nullptr);
    RawMessage welcome;
    MultiplexerMessage theirs;
    if (fd < 0 || read_frame(fd, &welcome) || !theirs.ParseFromString(welcome.get_message()) ||
        write_frame(fd, *multiplexer::impl::create_welcome_message(peers::MULTIPLEXER, id))) {
      if (fd >= 0) {
        ::close(fd);
      }
      _end();
      return;
    }
    client_ = theirs.sender();
    {
      std::unique_lock<std::mutex> lock(mutex_);
      fd_ = fd;
      welcomed_ = true;
      if (hung_up_) {
        ::shutdown(fd_, SHUT_RDWR);
      }
      changed_.notify_all();
      if (stalled_) {
        changed_.wait(lock, [this] { return hung_up_ || released_; });
      }
    }
    for (;;) {
      RawMessage frame;
      MultiplexerMessage msg;
      if (read_frame(fd, &frame) || !msg.ParseFromString(frame.get_message())) {
        break;
      }
      if (msg.type() == types::HEARTBIT || msg.type() == types::PEER_CONTROL) {
        continue;
      }
      std::lock_guard<std::mutex> lock(mutex_);
      seen_.push_back(msg);
      unread_.push_back(msg);
      changed_.notify_all();
    }
    // The client ended its side, or hang_up() this one: this side closes
    // too, as a multiplexer's does, so that the client's shutdown, which
    // waits for it, returns at once.
    ::shutdown(fd, SHUT_RDWR);
    _end();
  }

  // The connection is over: whoever waits for a message stops waiting.
  void _end() {
    std::lock_guard<std::mutex> lock(mutex_);
    ended_ = true;
    changed_.notify_all();
  }

  const bool stalled_;
  int listener_ = -1;
  int fd_ = -1;
  std::atomic<std::uint64_t> client_{0};
  std::atomic<std::uint64_t> ids_{0};
  std::mutex mutex_;
  std::condition_variable changed_;
  std::deque<MultiplexerMessage> unread_;
  std::vector<MultiplexerMessage> seen_;
  bool welcomed_ = false;
  bool hung_up_ = false;
  bool ended_ = false;
  bool released_ = false;
  std::thread reader_;
};

// What two scripts tell each other: an attempt's id, once one knows it,
// and that a multiplexer went.
struct Board {
  std::promise<std::uint64_t> request;  // the first request's id
  std::promise<std::uint64_t> direct;   // the direct request's id
  std::promise<std::uint64_t> gone;     // a script's multiplexer went, with the search unanswered
  std::atomic<bool> claimed{false};     // a script took the search's one role
};

// Tells the other script `id`, once: a client that sends again makes a
// second script see what only one should, and the first word stands.
void publish(std::promise<std::uint64_t>& promised, std::uint64_t id) {
  try {
    promised.set_value(id);
  } catch (const std::future_error&) {
  }
}

// Waits for an id `promised` by the other script, 30 s at most: 0 when none came.
std::uint64_t awaited(std::promise<std::uint64_t>& promised, std::shared_future<std::uint64_t>& future) {
  if (!future.valid()) {
    future = promised.get_future().share();
  }
  return future.wait_for(std::chrono::seconds(30)) == std::future_status::ready ? future.get() : 0;
}

const std::uint64_t BACKEND = 0xbac;   // the instance a search finds, or an addressed query asks
const std::uint64_t LEAVING = 0x1ea7;  // the backend that had the request, refusing it as it leaves

// Which client asks.
enum class Kind { SYNC, THREADED };

// A query through a client of `kind` connected to every stand-in, in
// order: the reply, or what the synchronous client throws. `to` makes it
// addressed; `lane` is the query's; `workflow` the request's.
IncomingMessage ask(Kind kind, const std::vector<StandIn*>& stand_ins, float timeout, std::uint64_t to = 0,
                    multiplexer::LanePtr lane = multiplexer::LanePtr(), const std::string& workflow = std::string()) {
  if (kind == Kind::SYNC) {
    Client client(peers::WEBSITE);
    for (StandIn* stand_in : stand_ins) {
      client.connect("127.0.0.1", stand_in->port, 5);
    }
    MultiplexerMessage request;
    request.set_id(client.random64());
    request.set_sender(client.instance_id());
    request.set_type(types::PYTHON_TEST_REQUEST);
    request.set_message("question");
    request.set_to(to);
    request.set_workflow(workflow);
    return client.query(request, timeout, lane);
  }
  ThreadedClient client(peers::WEBSITE);
  for (StandIn* stand_in : stand_ins) {
    client.connect("127.0.0.1", stand_in->port, 5);
  }
  MultiplexerMessage request = client.new_message(types::PYTHON_TEST_REQUEST, "question");
  request.set_to(to);
  request.set_workflow(workflow);
  return client.query(request, timeout, lane).check();
}

// A client of `kind` driven step by step while scripts play its
// multiplexers on other threads: connected one stand-in at a time, events
// queued through a lane without waiting, then asked; dropped() from any
// thread.
class Asker {
 public:
  explicit Asker(Kind kind) {
    if (kind == Kind::SYNC) {
      sync_.reset(new Client(peers::WEBSITE));
    } else {
      threaded_.reset(new ThreadedClient(peers::WEBSITE));
    }
  }

  // Whether the client connected to `stand_in`.
  bool connect(const StandIn& stand_in) {
    if (sync_) {
      return static_cast<bool>(sync_->connect("127.0.0.1", stand_in.port, 5));
    }
    return threaded_->connect("127.0.0.1", stand_in.port, 5);
  }

  // `count` events of `size` bytes queued through `lane`, waiting for nothing.
  void queue_events(int count, std::size_t size, const multiplexer::LanePtr& lane) {
    const std::string payload(size, 'x');
    for (int index = 0; index < count; ++index) {
      if (threaded_) {
        threaded_->send(threaded_->new_message(types::TEST_EVENT, payload), lane);
        continue;
      }
      MultiplexerMessage event;
      event.set_id(sync_->random64());
      event.set_sender(sync_->instance_id());
      event.set_type(types::TEST_EVENT);
      event.set_message(payload);
      sync_->queue(event, multiplexer::DEFAULT_TIMEOUT, lane);
    }
  }

  // A query through `lane`, typed, or addressed with `to`: the reply, or
  // what the query throws.
  IncomingMessage ask(float timeout, const multiplexer::LanePtr& lane, std::uint64_t to = 0) {
    if (threaded_) {
      MultiplexerMessage request = threaded_->new_message(types::PYTHON_TEST_REQUEST, "question");
      request.set_to(to);
      return threaded_->query(request, timeout, lane).check();
    }
    MultiplexerMessage request;
    request.set_id(sync_->random64());
    request.set_sender(sync_->instance_id());
    request.set_type(types::PYTHON_TEST_REQUEST);
    request.set_message("question");
    request.set_to(to);
    return sync_->query(request, timeout, lane);
  }

  // How many messages the client gave up on so far; from any thread.
  std::uint64_t dropped() { return sync_ ? sync_->dropped() : threaded_->dropped(); }

 private:
  std::unique_ptr<Client> sync_;
  std::unique_ptr<ThreadedClient> threaded_;
};

// A multiplexer of the test's own that the client connects to three
// times, one connection after another, on one thread: the first takes the
// request and ends; the second, the client's reconnect, is welcomed and
// ended at once, the welcome and the end in one segment (TCP_CORK), so
// that the client counts it up and has its end at hand before it places
// anything on it; the third, the next reconnect, says nobody has a backend
// of the type to the search and then answers the request. outcome() is ""
// when it went so.
class ThreeConnections {
 public:
  ThreeConnections() {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof address;
    if (::bind(listener_, reinterpret_cast<sockaddr*>(&address), length) != 0 || ::listen(listener_, 4) != 0 ||
        ::getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
      ::close(listener_);
      throw std::runtime_error("no socket to listen on");
    }
    port = ntohs(address.sin_port);
    id_ = 0x5100 + port;
    outcome_ = std::async(std::launch::async, [this] { return _run(); });
  }
  ~ThreeConnections() {
    ::shutdown(listener_, SHUT_RDWR);  // an accept still waiting returns
    if (outcome_.valid()) {
      outcome_.wait();
    }
    ::close(listener_);
  }

  // How the three connections went: "" as planned; waits for the third to end.
  std::string outcome() { return outcome_.get(); }

  unsigned short port = 0;

 private:
  // The connections in turn.
  std::string _run() {
    std::uint64_t client = 0, request = 0;
    MultiplexerMessage msg;
    int fd = _welcome(&client);
    if (fd < 0) {
      return "the first connection never came";
    }
    while (_next(fd, &msg) && msg.type() != types::PYTHON_TEST_REQUEST) {
    }
    request = msg.type() == types::PYTHON_TEST_REQUEST ? msg.id() : 0;
    ::shutdown(fd, SHUT_RDWR);  // the request's way: it goes
    ::close(fd);
    if (!request) {
      return "the request did not come";
    }
    fd = _welcome(&client, /*then_end=*/true);
    if (fd < 0) {
      return "the reconnect never came";
    }
    ::close(fd);
    fd = _welcome(&client);
    if (fd < 0) {
      return "the second reconnect never came";
    }
    while (_next(fd, &msg) && msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
    }
    if (msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
      ::close(fd);
      return "no search came";
    }
    _write(fd, types::DELIVERY_ERROR, id_, client, msg.id());  // nobody here
    _write(fd, types::PYTHON_TEST_RESPONSE, BACKEND, client, request);
    while (_next(fd, &msg)) {  // until the client ends
    }
    ::close(fd);
    return std::string();
  }

  // The next connection, its welcomes exchanged: its descriptor, -1 when
  // none came; `client` the client's instance id. With `then_end` this
  // side's welcome and its end go out in one segment.
  int _welcome(std::uint64_t* client, bool then_end = false) {
    const int fd = ::accept(listener_, nullptr, nullptr);
    if (fd < 0) {
      return -1;
    }
    RawMessage welcome;
    MultiplexerMessage theirs;
    int corked = then_end ? 1 : 0;
    ::setsockopt(fd, IPPROTO_TCP, TCP_CORK, &corked, sizeof corked);
    if (read_frame(fd, &welcome) || !theirs.ParseFromString(welcome.get_message()) ||
        write_frame(fd, *multiplexer::impl::create_welcome_message(peers::MULTIPLEXER, id_))) {
      ::close(fd);
      return -1;
    }
    if (then_end) {
      ::shutdown(fd, SHUT_RDWR);  // the end behind the welcome, the corked segment sent with it
    }
    *client = theirs.sender();
    return fd;
  }

  // The client's next message on `fd`, the protocol's own skipped: false
  // at the connection's end, within 30 s of the last frame.
  bool _next(int fd, MultiplexerMessage* msg) {
    timeval wait = {30, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &wait, sizeof wait);
    for (;;) {
      RawMessage frame;
      if (read_frame(fd, &frame) || !msg->ParseFromString(frame.get_message())) {
        msg->Clear();
        return false;
      }
      if (msg->type() != types::HEARTBIT && msg->type() != types::PEER_CONTROL) {
        return true;
      }
    }
  }

  // A message of `type` from `sender` to `client`, answering `references`.
  void _write(int fd, std::uint32_t type, std::uint64_t sender, std::uint64_t client, std::uint64_t references) {
    MultiplexerMessage msg;
    msg.set_id(++ids_);
    msg.set_sender(sender);
    msg.set_to(client);
    msg.set_type(type);
    msg.set_references(references);
    msg.set_message("late");
    write_frame(fd, *std::unique_ptr<RawMessage>(RawMessage::FromMessage(msg)));
  }

  int listener_ = -1;
  std::uint64_t id_ = 0;
  std::uint64_t ids_ = std::uint64_t(1) << 40;
  std::future<std::string> outcome_;
};

class QueryStages : public ::testing::TestWithParam<Kind> {};

// The connection that carried the request goes: the query searches at once
// through the other multiplexer, which says nobody has a backend of the
// type; the request may have been routed before the connection went, so
// the query waits for its reply, which comes. It failed at once.
TEST_P(QueryStages, ASearchThatFindsNobodyWaitsForTheRequestABackendMayHave) {
  StandIn first, second;
  Board board;
  std::shared_future<std::uint64_t> request_id;
  auto script = [&board, &request_id](StandIn* stand_in) {
    MultiplexerMessage msg;
    if (!stand_in->next(&msg)) {
      return std::string("nothing came");
    }
    if (msg.type() == types::PYTHON_TEST_REQUEST) {  // the request's way: it goes
      publish(board.request, msg.id());
      stand_in->hang_up();
      return std::string();
    }
    if (msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
      return "type " + std::to_string(msg.type()) + " came, not the search";
    }
    stand_in->write(stand_in->answer(types::DELIVERY_ERROR, stand_in->id, msg.id()));
    const std::uint64_t request = awaited(board.request, request_id);
    stand_in->write(stand_in->answer(types::PYTHON_TEST_RESPONSE, BACKEND, request));
    return std::string();
  };
  std::future<std::string> one = std::async(std::launch::async, script, &first);
  std::future<std::string> two = std::async(std::launch::async, script, &second);
  std::string payload;
  try {
    payload = ask(GetParam(), {&first, &second}, 30).third->message();
  } catch (const std::exception& error) {
    ADD_FAILURE() << "the query raised " << error.what();
  }
  first.hang_up();
  second.hang_up();
  EXPECT_EQ("", one.get());
  EXPECT_EQ("", two.get());
  EXPECT_EQ("late", payload);
  EXPECT_EQ(1, first.count(types::PYTHON_TEST_REQUEST) + second.count(types::PYTHON_TEST_REQUEST))
      << "the request went out again";
}

// Nobody takes the request; the search finds a backend behind one
// multiplexer, which takes the direct request and then goes, while the
// other says it has none. The direct request may have been routed first:
// the query waits for its reply, which comes through the other, and
// nothing goes out again. It went out a third time.
TEST_P(QueryStages, ALostDirectRequestIsWaitedForNotSentAgain) {
  StandIn first, second;
  Board board;
  std::shared_future<std::uint64_t> direct_id;
  auto script = [&board, &direct_id](StandIn* stand_in) {
    MultiplexerMessage msg;
    if (!stand_in->next(&msg)) {
      return std::string("nothing came");
    }
    if (msg.type() == types::PYTHON_TEST_REQUEST) {  // nobody takes the request
      stand_in->write(stand_in->answer(types::DELIVERY_ERROR, stand_in->id, msg.id()));
      if (!stand_in->next(&msg)) {
        return std::string("no search came");
      }
    }
    if (msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
      return "type " + std::to_string(msg.type()) + " came, not the search";
    }
    if (!board.claimed.exchange(true)) {  // the backend is behind this one: it takes the direct request, then goes
      stand_in->write(stand_in->answer(types::PING, BACKEND, msg.id()));
      if (!stand_in->next(&msg) || msg.type() != types::PYTHON_TEST_REQUEST || msg.to() != BACKEND) {
        return std::string("no direct request came");
      }
      publish(board.direct, msg.id());
      stand_in->hang_up();
      return std::string();
    }
    stand_in->write(stand_in->answer(types::DELIVERY_ERROR, stand_in->id, msg.id()));
    const std::uint64_t direct = awaited(board.direct, direct_id);
    stand_in->write(stand_in->answer(types::PYTHON_TEST_RESPONSE, BACKEND, direct));
    return std::string();
  };
  std::future<std::string> one = std::async(std::launch::async, script, &first);
  std::future<std::string> two = std::async(std::launch::async, script, &second);
  std::string payload;
  try {
    payload = ask(GetParam(), {&first, &second}, 30).third->message();
  } catch (const std::exception& error) {
    ADD_FAILURE() << "the query raised " << error.what();
  }
  first.hang_up();
  second.hang_up();
  EXPECT_EQ("", one.get());
  EXPECT_EQ("", two.get());
  EXPECT_EQ("late", payload);
  EXPECT_EQ(2, first.count(types::PYTHON_TEST_REQUEST) + second.count(types::PYTHON_TEST_REQUEST))
      << "the request and the direct request, no more";
}

// An addressed query's request goes with its connection; the PING finds
// the addressee behind the other multiplexer, which then answers the
// request again with a delivery error, the addressee gone from it. The
// first request may still be answered: the query waits, and its reply
// comes. It failed at once, or sent the request again where it located.
TEST_P(QueryStages, AnAddressedQueryWaitsForTheRequestTheAddresseeMayHave) {
  StandIn first, second;
  Board board;
  std::shared_future<std::uint64_t> request_id;
  auto script = [&board, &request_id](StandIn* stand_in) {
    MultiplexerMessage msg;
    if (!stand_in->next(&msg)) {
      return std::string("nothing came");
    }
    if (msg.type() == types::PYTHON_TEST_REQUEST && msg.to() == BACKEND) {  // the request's way: it goes
      publish(board.request, msg.id());
      stand_in->hang_up();
      return std::string();
    }
    if (msg.type() != types::PING || msg.to() != BACKEND) {
      return "type " + std::to_string(msg.type()) + " came, not the PING that locates";
    }
    stand_in->write(stand_in->answer(types::PING, BACKEND, msg.id()));
    if (!stand_in->next(&msg) || msg.type() != types::PYTHON_TEST_REQUEST) {
      return std::string("the request did not come again");
    }
    stand_in->write(stand_in->answer(types::DELIVERY_ERROR, stand_in->id, msg.id()));
    const std::uint64_t request = awaited(board.request, request_id);
    stand_in->write(stand_in->answer(types::PYTHON_TEST_RESPONSE, BACKEND, request));
    return std::string();
  };
  std::future<std::string> one = std::async(std::launch::async, script, &first);
  std::future<std::string> two = std::async(std::launch::async, script, &second);
  std::string payload;
  try {
    payload = ask(GetParam(), {&first, &second}, 30, BACKEND).third->message();
  } catch (const std::exception& error) {
    ADD_FAILURE() << "the query raised " << error.what();
  }
  first.hang_up();
  second.hang_up();
  EXPECT_EQ("", one.get());
  EXPECT_EQ("", two.get());
  EXPECT_EQ("late", payload);
}

// An addressed query's request goes with its connection; the PING finds
// the addressee behind the other multiplexer, which answers the request
// sent again: that reply is the query's answer, whatever became of the
// first. The C++ SyncClient sent the request again at once through the
// other multiplexer, never locating the addressee.
TEST_P(QueryStages, AnAddressedQueryTakesTheReplyToItsSecondRequestAfterALoss) {
  StandIn first, second;
  auto script = [](StandIn* stand_in) {
    MultiplexerMessage msg;
    if (!stand_in->next(&msg)) {
      return std::string("nothing came");
    }
    if (msg.type() == types::PYTHON_TEST_REQUEST && msg.to() == BACKEND) {  // the request's way: it goes
      stand_in->hang_up();
      return std::string();
    }
    if (msg.type() != types::PING || msg.to() != BACKEND) {
      return "type " + std::to_string(msg.type()) + " came, not the PING that locates";
    }
    stand_in->write(stand_in->answer(types::PING, BACKEND, msg.id()));
    if (!stand_in->next(&msg) || msg.type() != types::PYTHON_TEST_REQUEST) {
      return std::string("the request did not come again");
    }
    stand_in->write(stand_in->answer(types::PYTHON_TEST_RESPONSE, BACKEND, msg.id()));
    return std::string();
  };
  std::future<std::string> one = std::async(std::launch::async, script, &first);
  std::future<std::string> two = std::async(std::launch::async, script, &second);
  std::string payload;
  try {
    payload = ask(GetParam(), {&first, &second}, 30, BACKEND).third->message();
  } catch (const std::exception& error) {
    ADD_FAILURE() << "the query raised " << error.what();
  }
  first.hang_up();
  second.hang_up();
  EXPECT_EQ("", one.get());
  EXPECT_EQ("", two.get());
  EXPECT_EQ("late", payload);
}

// Nobody takes the request; one multiplexer goes with the search
// unanswered, and the other says nobody has a backend of the type. The
// search that went may have been routed first: the query waits on, and the
// backend's PING to it comes back another way, through the other, so the
// direct request goes there and is answered; the request went out twice,
// no more. ThreadedClient took the lost connection for an answer: the
// search was over, and the query failed at once, nobody found, or, when
// the delivery error came first, started over and sent the request again.
TEST_P(QueryStages, ASearchedConnectionThatGoesIsNoAnswer) {
  StandIn first, second;
  Board board;
  std::shared_future<std::uint64_t> gone;
  auto script = [&board, &gone](StandIn* stand_in) {
    MultiplexerMessage msg;
    if (!stand_in->next(&msg)) {
      return std::string("nothing came");
    }
    if (msg.type() == types::PYTHON_TEST_REQUEST) {  // nobody takes the request
      stand_in->write(stand_in->answer(types::DELIVERY_ERROR, stand_in->id, msg.id()));
      if (!stand_in->next(&msg)) {
        return std::string("no search came");
      }
    }
    if (msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
      return "type " + std::to_string(msg.type()) + " came, not the search";
    }
    if (!board.claimed.exchange(true)) {
      stand_in->hang_up();  // goes with the search unanswered
      publish(board.gone, 1);
      return std::string();
    }
    const std::uint64_t search = msg.id();
    stand_in->write(stand_in->answer(types::DELIVERY_ERROR, stand_in->id, search));
    if (!awaited(board.gone, gone)) {
      return std::string("the other multiplexer never went");
    }
    stand_in->write(stand_in->answer(types::PING, BACKEND, search));  // the gone one's, come back this way
    if (!stand_in->next(&msg) || msg.type() != types::PYTHON_TEST_REQUEST || msg.to() != BACKEND) {
      return std::string("no direct request came");
    }
    stand_in->write(stand_in->answer(types::PYTHON_TEST_RESPONSE, BACKEND, msg.id()));
    return std::string();
  };
  std::future<std::string> one = std::async(std::launch::async, script, &first);
  std::future<std::string> two = std::async(std::launch::async, script, &second);
  std::string payload;
  try {
    payload = ask(GetParam(), {&first, &second}, 30).third->message();
  } catch (const std::exception& error) {
    ADD_FAILURE() << "the query raised " << error.what();
  }
  first.hang_up();
  second.hang_up();
  EXPECT_EQ("", one.get());
  EXPECT_EQ("", two.get());
  EXPECT_EQ("late", payload);
  EXPECT_EQ(2, first.count(types::PYTHON_TEST_REQUEST) + second.count(types::PYTHON_TEST_REQUEST))
      << "the request and the direct request, no more";
}

// Through a pinned lane a late reply can come only the lane's way. The
// request draws no answer within its stage, so a backend may have it; the
// search through the lane finds nobody, and the query waits for that late
// reply; then the lane's connection goes: NotConnected, at once, where the
// wait ran on to the end of its stage, OperationTimedOut. The one case a
// timer drives: a pinned query reaches the late wait only once its
// request's stage has run out, a loss through a pinned lane ending the
// query at once. The stand-in acts once the search comes, which says the
// timer fired, so what is left to time is two frames on one socket, read
// within the search's 2 s.
TEST_P(QueryStages, APinnedQueryWaitingLateEndsWhenItsConnectionGoes) {
  StandIn only;
  auto script = [](StandIn* stand_in) {
    MultiplexerMessage msg;
    if (!stand_in->next(&msg) || msg.type() != types::PYTHON_TEST_REQUEST) {
      return std::string("the request did not come");
    }
    if (!stand_in->next(&msg) || msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
      return std::string("no search came");
    }
    stand_in->write(stand_in->answer(types::DELIVERY_ERROR, stand_in->id, msg.id()));  // nobody
    stand_in->hang_up();
    return std::string();
  };
  std::future<std::string> run = std::async(std::launch::async, script, &only);
  EXPECT_THROW(ask(GetParam(), {&only}, 2, 0, std::make_shared<multiplexer::Lane>(true)), Client::NotConnected);
  only.hang_up();
  EXPECT_EQ("", run.get());
  EXPECT_EQ(1, only.count(types::PYTHON_TEST_REQUEST));
}

// The request waits unwritten, behind what a lane sent before it, on a
// connection whose multiplexer reads nothing, past its stage's time; the
// search goes out, and the other multiplexer says nobody has a backend of
// the type; then the first catches up, reads the request, written at
// last, and answers it: that reply is the query's answer, the request
// having gone out once. The synchronous clients recorded the request only
// once its send was done and threw that reply away, OperationTimedOut. A
// timer drives the first stage, as only a stage that runs out of time
// leaves a request unwritten; the rest are the scripts' turns, within the
// search's 3 s.
TEST_P(QueryStages, ARequestWhoseSendRanOutOfTimeIsStillAnAttempt) {
  StandIn first(/*stalled=*/true), second;
  Asker asker(GetParam());
  ASSERT_TRUE(asker.connect(first));
  multiplexer::LanePtr lane = std::make_shared<multiplexer::Lane>();
  asker.queue_events(FILL_FRAMES, fill_size(), lane);  // past the sockets of a multiplexer that reads nothing
  ASSERT_TRUE(asker.connect(second));
  std::future<std::string> nobody = std::async(std::launch::async, [&first, &second] {
    MultiplexerMessage msg;
    if (!second.next(&msg) || msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
      return std::string("no search came");
    }
    second.write(second.answer(types::DELIVERY_ERROR, second.id, msg.id()));
    first.release();  // the first multiplexer catches up
    return std::string();
  });
  std::future<std::string> answers = std::async(std::launch::async, [&first] {
    MultiplexerMessage msg;
    while (first.next(&msg)) {
      if (msg.type() == types::PYTHON_TEST_REQUEST) {
        first.write(first.answer(types::PYTHON_TEST_RESPONSE, BACKEND, msg.id()));
        return std::string();
      }
    }
    return std::string("the request never came");
  });
  std::string payload;
  try {
    payload = asker.ask(3, lane).third->message();
  } catch (const std::exception& error) {
    ADD_FAILURE() << "the query raised " << error.what();
  }
  first.hang_up();
  second.hang_up();
  EXPECT_EQ("", nobody.get());
  EXPECT_EQ("", answers.get());
  EXPECT_EQ("late", payload);
  EXPECT_EQ(1, first.count(types::PYTHON_TEST_REQUEST) + second.count(types::PYTHON_TEST_REQUEST))
      << "the request went out again";
}

// The request waits for room behind a full queue, on a connection whose
// multiplexer reads nothing, past its stage's time, and the client gives
// it up; the search goes out behind it, and once the first multiplexer
// catches up, both say nobody has a backend of the type. The request went
// nowhere, so nothing can answer: OperationFailed at once, not a wait for
// a late reply that cannot come. The first stage runs out on its timer,
// as in the case above; the first multiplexer catches up once the client
// counts the request given up.
TEST_P(QueryStages, ARequestItsSendGaveUpIsStruckOffTheLateWait) {
  StandIn first(/*stalled=*/true), second;
  Asker asker(GetParam());
  ASSERT_TRUE(asker.connect(first));
  multiplexer::LanePtr lane = std::make_shared<multiplexer::Lane>();
  asker.queue_events(FILL_FRAMES, fill_size(), lane);  // past the sockets of a multiplexer that reads nothing
  // The connection's queue full, at 1024 messages: what follows waits for room.
  asker.queue_events(1024 + 64, 16, lane);
  ASSERT_TRUE(asker.connect(second));
  std::future<std::string> nobody = std::async(std::launch::async, [&first, &second, &asker] {
    MultiplexerMessage msg;
    if (!second.next(&msg) || msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
      return std::string("no search came");
    }
    second.write(second.answer(types::DELIVERY_ERROR, second.id, msg.id()));
    // A bound on the wait only detects a failure.
    for (int checks = 0; checks < 3000 && asker.dropped() == 0; ++checks) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    first.release();  // the first multiplexer catches up
    return asker.dropped() ? std::string() : std::string("the request was never given up");
  });
  std::future<std::string> answers = std::async(std::launch::async, [&first] {
    MultiplexerMessage msg;
    while (first.next(&msg)) {
      if (msg.type() == types::PYTHON_TEST_REQUEST) {
        return std::string("the request was written after all");
      }
      if (msg.type() == types::BACKEND_FOR_PACKET_SEARCH) {
        first.write(first.answer(types::DELIVERY_ERROR, first.id, msg.id()));
        return std::string();
      }
    }
    return std::string("the search never came");
  });
  EXPECT_THROW(asker.ask(3, lane), Client::OperationFailed);
  first.hang_up();
  second.hang_up();
  EXPECT_EQ("", nobody.get());
  EXPECT_EQ("", answers.get());
}

// An addressed query whose request waits for room behind a full queue,
// on a connection whose multiplexer reads nothing, past the query's one
// deadline, a connection live all along: OperationTimedOut, the query's
// time up, not NotConnected, which would say no connection was there.
// Nothing pinned it. The deadline is what is tested, so a timer ends the
// query.
TEST_P(QueryStages, AnAddressedQueryWaitingForRoomAtItsDeadlineTimesOut) {
  StandIn only(/*stalled=*/true);
  Asker asker(GetParam());
  ASSERT_TRUE(asker.connect(only));
  multiplexer::LanePtr lane = std::make_shared<multiplexer::Lane>();
  asker.queue_events(FILL_FRAMES, fill_size(), lane);  // past the sockets of a multiplexer that reads nothing
  // The connection's queue full, at 1024 messages: what follows waits for room.
  asker.queue_events(1024 + 64, 16, lane);
  EXPECT_THROW(asker.ask(1, lane, BACKEND), Client::OperationTimedOut);
  only.hang_up();
}

// The request a query sends again, to the backend its search found, is a
// copy of the request, its workflow and every other field kept, with a
// fresh id, where the synchronous client built it anew from the type and
// payload alone and the backend lost the caller's workflow.
TEST_P(QueryStages, TheRequestSentAgainKeepsItsWorkflow) {
  StandIn only;
  auto script = [](StandIn* stand_in) {
    MultiplexerMessage msg;
    if (!stand_in->next(&msg) || msg.type() != types::PYTHON_TEST_REQUEST) {
      return std::string("the request did not come");
    }
    stand_in->write(stand_in->answer(types::DELIVERY_ERROR, stand_in->id, msg.id()));  // nobody takes it
    if (!stand_in->next(&msg) || msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
      return std::string("no search came");
    }
    stand_in->write(stand_in->answer(types::PING, BACKEND, msg.id()));
    if (!stand_in->next(&msg) || msg.type() != types::PYTHON_TEST_REQUEST || msg.to() != BACKEND) {
      return std::string("no direct request came");
    }
    if (msg.workflow() != "trace") {
      return "the request sent again had the workflow '" + msg.workflow() + "'";
    }
    stand_in->write(stand_in->answer(types::PYTHON_TEST_RESPONSE, BACKEND, msg.id()));
    return std::string();
  };
  std::future<std::string> run = std::async(std::launch::async, script, &only);
  std::string payload;
  try {
    payload = ask(GetParam(), {&only}, 30, 0, multiplexer::LanePtr(), "trace").third->message();
  } catch (const std::exception& error) {
    ADD_FAILURE() << "the query raised " << error.what();
  }
  only.hang_up();
  EXPECT_EQ("", run.get());
  EXPECT_EQ("late", payload);
}

// The connection that carried the request goes, and the search goes
// through the other multiplexer; the backend that had the request refuses
// it as it leaves, its delivery error coming back that way, and then a
// backend there answers the search. The refusal strikes the request off
// and leaves the search on: the PING finds the backend, and the direct
// request's reply is the answer. The synchronous clients took the refusal
// for that multiplexer's "nobody", ended the search on it and waited for a
// reply to the request alone, timing out.
TEST_P(QueryStages, ARefusalOfTheRequestDuringTheSearchLeavesTheSearchOn) {
  StandIn first, second;
  Board board;
  std::shared_future<std::uint64_t> request_id;
  auto script = [&board, &request_id](StandIn* stand_in) {
    MultiplexerMessage msg;
    if (!stand_in->next(&msg)) {
      return std::string("nothing came");
    }
    if (msg.type() == types::PYTHON_TEST_REQUEST) {  // the request's way: it goes
      publish(board.request, msg.id());
      stand_in->hang_up();
      return std::string();
    }
    if (msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
      return "type " + std::to_string(msg.type()) + " came, not the search";
    }
    const std::uint64_t request = awaited(board.request, request_id);
    if (!request) {
      return std::string("the request's id never came");
    }
    stand_in->write(stand_in->answer(types::DELIVERY_ERROR, LEAVING, request));
    stand_in->write(stand_in->answer(types::PING, BACKEND, msg.id()));
    if (!stand_in->next(&msg) || msg.type() != types::PYTHON_TEST_REQUEST || msg.to() != BACKEND) {
      return std::string("no direct request came");
    }
    stand_in->write(stand_in->answer(types::PYTHON_TEST_RESPONSE, BACKEND, msg.id()));
    return std::string();
  };
  std::future<std::string> one = std::async(std::launch::async, script, &first);
  std::future<std::string> two = std::async(std::launch::async, script, &second);
  std::string payload;
  try {
    payload = ask(GetParam(), {&first, &second}, 30).third->message();
  } catch (const std::exception& error) {
    ADD_FAILURE() << "the query raised " << error.what();
  }
  first.hang_up();
  second.hang_up();
  EXPECT_EQ("", one.get());
  EXPECT_EQ("", two.get());
  EXPECT_EQ("late", payload);
  EXPECT_EQ(2, first.count(types::PYTHON_TEST_REQUEST) + second.count(types::PYTHON_TEST_REQUEST))
      << "the request and the direct request, no more";
}

// The connection that carried the request goes; the search finds a
// backend behind the other multiplexer, which takes the direct request;
// then the backend that had the first request refuses it as it leaves,
// its delivery error coming back the same way, before the direct
// request's reply. The refusal strikes the request off and leaves the
// direct request's stage on: its reply is the answer. The synchronous
// clients took the refusal for the direct request's own and waited for a
// reply to the request alone, dropping the direct request's and timing
// out.
TEST_P(QueryStages, ARefusalOfTheRequestDuringTheDirectRequestLeavesItsStageOn) {
  StandIn first, second;
  Board board;
  std::shared_future<std::uint64_t> request_id;
  auto script = [&board, &request_id](StandIn* stand_in) {
    MultiplexerMessage msg;
    if (!stand_in->next(&msg)) {
      return std::string("nothing came");
    }
    if (msg.type() == types::PYTHON_TEST_REQUEST) {  // the request's way: it goes
      publish(board.request, msg.id());
      stand_in->hang_up();
      return std::string();
    }
    if (msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
      return "type " + std::to_string(msg.type()) + " came, not the search";
    }
    stand_in->write(stand_in->answer(types::PING, BACKEND, msg.id()));
    if (!stand_in->next(&msg) || msg.type() != types::PYTHON_TEST_REQUEST || msg.to() != BACKEND) {
      return std::string("no direct request came");
    }
    const std::uint64_t request = awaited(board.request, request_id);
    if (!request) {
      return std::string("the request's id never came");
    }
    stand_in->write(stand_in->answer(types::DELIVERY_ERROR, LEAVING, request));
    stand_in->write(stand_in->answer(types::PYTHON_TEST_RESPONSE, BACKEND, msg.id()));
    return std::string();
  };
  std::future<std::string> one = std::async(std::launch::async, script, &first);
  std::future<std::string> two = std::async(std::launch::async, script, &second);
  std::string payload;
  try {
    payload = ask(GetParam(), {&first, &second}, 30).third->message();
  } catch (const std::exception& error) {
    ADD_FAILURE() << "the query raised " << error.what();
  }
  first.hang_up();
  second.hang_up();
  EXPECT_EQ("", one.get());
  EXPECT_EQ("", two.get());
  EXPECT_EQ("late", payload);
  EXPECT_EQ(2, first.count(types::PYTHON_TEST_REQUEST) + second.count(types::PYTHON_TEST_REQUEST))
      << "the request and the direct request, no more";
}

// The connection that carried the request goes, and the search goes
// through the other two multiplexers; the backend that had the request
// refuses it as it leaves, its delivery error coming back through one of
// them, and both say nobody has a backend of the type. The refusal
// strikes the request off, and the search ends once both have said so:
// with nothing left that can answer, OperationFailed at once. The
// synchronous clients counted the refusal as one multiplexer's "nobody"
// and waited for a reply to the request it refused, timing out.
TEST_P(QueryStages, ARequestRefusedDuringTheSearchLeavesNothingToWaitFor) {
  StandIn first, second, third;
  Board board;
  std::shared_future<std::uint64_t> request_id;
  auto script = [&board, &request_id](StandIn* stand_in) {
    MultiplexerMessage msg;
    if (!stand_in->next(&msg)) {
      return std::string("nothing came");
    }
    if (msg.type() == types::PYTHON_TEST_REQUEST) {  // the request's way: it goes
      publish(board.request, msg.id());
      stand_in->hang_up();
      return std::string();
    }
    if (msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
      return "type " + std::to_string(msg.type()) + " came, not the search";
    }
    if (!board.claimed.exchange(true)) {  // the refusal comes back this way
      const std::uint64_t request = awaited(board.request, request_id);
      if (!request) {
        return std::string("the request's id never came");
      }
      stand_in->write(stand_in->answer(types::DELIVERY_ERROR, LEAVING, request));
    }
    stand_in->write(stand_in->answer(types::DELIVERY_ERROR, stand_in->id, msg.id()));  // nobody here
    return std::string();
  };
  std::future<std::string> one = std::async(std::launch::async, script, &first);
  std::future<std::string> two = std::async(std::launch::async, script, &second);
  std::future<std::string> three = std::async(std::launch::async, script, &third);
  EXPECT_THROW(ask(GetParam(), {&first, &second, &third}, 30), Client::OperationFailed);
  first.hang_up();
  second.hang_up();
  third.hang_up();
  EXPECT_EQ("", one.get());
  EXPECT_EQ("", two.get());
  EXPECT_EQ("", three.get());
  EXPECT_EQ(1, first.count(types::PYTHON_TEST_REQUEST) + second.count(types::PYTHON_TEST_REQUEST) +
                   third.count(types::PYTHON_TEST_REQUEST))
      << "the request alone";
}

// The request's connection goes, and the search waits for a connection;
// the reconnect is counted up and lost before the search is placed on it.
// The search waits on and goes out on the next one, which says nobody has
// a backend of the type, and the request, which a backend may have, is
// answered there. The synchronous client placed the search on nothing
// after that one wait and waited out the stage, OperationTimedOut.
// ThreadedClient waits for a connection after any placement that found
// none, as the synchronous client does now. Counted, not timed: the
// stand-in acts on what the client sends, the two waits for a reconnect
// the client's own.
TEST(QueryStagesSync, ASearchWaitsOnWhenTheConnectionItWaitedForGoesAtOnce) {
  ThreeConnections mx;
  MultiplexerMessage request;
  request.set_type(types::PYTHON_TEST_REQUEST);
  request.set_message("question");
  std::string payload;
  try {
    Client client(peers::WEBSITE);
    ASSERT_TRUE(client.wait_for_connection(client.connect("127.0.0.1", mx.port, 5), 5));
    payload = client.query(request, 30).third->message();
  } catch (const std::exception& error) {
    ADD_FAILURE() << "the query raised " << error.what();
  }
  EXPECT_EQ("", mx.outcome());
  EXPECT_EQ("late", payload);
}

// The search's copies wait for room no longer than the stage has left,
// and a moment more, as the request's send lets the request wait: a copy
// still waiting then is given up, where it had the default 10 s and could
// go out long after the query had ended, its answer read by nobody. The
// first stage runs out on its timer, the request waiting for room behind
// a full queue on a multiplexer that reads nothing; the search's copy for
// that multiplexer waits there too, the other says nobody has a backend of
// the type, and the query ends OperationTimedOut. The client then counts
// both given up within moments, the request at its stage's end and the
// copy at the search's; a bound of 5 s tells that from the 10 s.
TEST(QueryStagesSync, ASearchCopyWaitsForRoomNoLongerThanItsStage) {
  StandIn first(/*stalled=*/true), second;
  Client client(peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", first.port, 5));
  multiplexer::LanePtr lane = std::make_shared<multiplexer::Lane>();
  // The sockets full, then the connection's queue, at 1024: what follows
  // waits for room. A minute each, so that none of them is given up here.
  const std::string fill(fill_size(), 'f');
  const int events = FILL_FRAMES + 1024 + 64;
  for (int index = 0; index < events; ++index) {
    MultiplexerMessage event;
    event.set_id(client.random64());
    event.set_sender(client.instance_id());
    event.set_type(types::TEST_EVENT);
    event.set_message(index < FILL_FRAMES ? fill : std::string(16, 'x'));
    client.queue(event, 60, lane);
  }
  ASSERT_TRUE(client.connect("127.0.0.1", second.port, 5));
  std::future<std::string> nobody = std::async(std::launch::async, [&second] {
    MultiplexerMessage msg;
    if (!second.next(&msg) || msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
      return std::string("no search came");
    }
    second.write(second.answer(types::DELIVERY_ERROR, second.id, msg.id()));
    return std::string();
  });
  MultiplexerMessage request;
  request.set_type(types::PYTHON_TEST_REQUEST);
  request.set_message("question");
  EXPECT_THROW(client.query(request, 2, lane), Client::OperationTimedOut);
  for (int checks = 0; checks < 500 && client.dropped() < 2; ++checks) {
    client.flush_all(0.01f);  // the loop runs, the copy's deadline among what it handles
  }
  EXPECT_EQ(2u, client.dropped()) << "the request and the search's copy, both given up";
  first.hang_up();
  second.hang_up();
  EXPECT_EQ("", nobody.get());
}

INSTANTIATE_TEST_SUITE_P(BothClients, QueryStages, ::testing::Values(Kind::SYNC, Kind::THREADED),
                         [](const ::testing::TestParamInfo<Kind>& info) {
                           return std::string(info.param == Kind::SYNC ? "SyncClient" : "ThreadedClient");
                         });

// A query given an empty callback, from a caller who wants the request
// delivered and not the reply, runs as any other: its first stage runs out
// and the search goes out, where the empty callback read as a query over
// from the start and its deadline never fired. After shutdown() such a
// query returns, where it threw std::bad_function_call. The deadline is
// what is tested, so a timer ends the first stage; the stand-in only waits
// for the search, a bound on that wait detecting a failure.
TEST(QueryStagesThreaded, AQueryWithAnEmptyCallbackRunsToItsDeadline) {
  StandIn only;
  std::future<std::string> run = std::async(std::launch::async, [&only] {
    MultiplexerMessage msg;
    if (!only.next(&msg) || msg.type() != types::PYTHON_TEST_REQUEST) {
      return std::string("the request did not come");
    }
    if (!only.next(&msg) || msg.type() != types::BACKEND_FOR_PACKET_SEARCH) {
      return std::string("no search came: the first stage never ran out");
    }
    return std::string();
  });
  ThreadedClient client(peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", only.port, 5));
  client.query(client.new_message(types::PYTHON_TEST_REQUEST, "question"), ThreadedClient::Callback(), 0.2f);
  EXPECT_EQ("", run.get());
  client.shutdown();
  EXPECT_NO_THROW(client.query(client.new_message(types::PYTHON_TEST_REQUEST, "after"), ThreadedClient::Callback(), 1));
  only.hang_up();
}

// A query whose only connection goes under the request waits for one to
// come up, and then sends the search, its own stage's message: the
// request went out once. It started over and sent the request again.
TEST(QueryStagesThreaded, AQueryWaitingForAConnectionSendsItsOwnStage) {
  StandIn first, second;
  std::promise<ThreadedClient::Result> done;  // outlives the client, whose shutdown may end the query
  ThreadedClient client(peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", first.port, 5));
  client.query(
      client.new_message(types::PYTHON_TEST_REQUEST, "question"),
      [&done](const ThreadedClient::Result& result) { done.set_value(result); }, 30);
  MultiplexerMessage request;
  ASSERT_TRUE(first.next(&request));
  ASSERT_EQ(types::PYTHON_TEST_REQUEST, request.type());
  first.hang_up();
  // The query waits for a connection once it saw its own go: a bound on
  // the wait only detects a failure.
  for (int checks = 0; checks < 3000 && client.waiting_queries() != 1; ++checks) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(1u, client.waiting_queries()) << "the query never waited for a connection";
  ASSERT_TRUE(client.connect("127.0.0.1", second.port, 5));
  MultiplexerMessage search;
  ASSERT_TRUE(second.next(&search));
  EXPECT_EQ(types::BACKEND_FOR_PACKET_SEARCH, search.type()) << "the request went out again";
  second.write(second.answer(types::DELIVERY_ERROR, second.id, search.id()));
  second.write(second.answer(types::PYTHON_TEST_RESPONSE, BACKEND, request.id()));
  std::future<ThreadedClient::Result> answered = done.get_future();
  ASSERT_EQ(std::future_status::ready, answered.wait_for(std::chrono::seconds(30)));
  ThreadedClient::Result result = answered.get();
  ASSERT_EQ(ThreadedClient::REPLIED, result.outcome);
  EXPECT_EQ("late", result.reply.third->message());
  EXPECT_EQ(0, second.count(types::PYTHON_TEST_REQUEST));
}

// The request waits unwritten behind what a lane sent before it, on a
// connection whose multiplexer reads nothing, and that connection goes:
// the outbox hands the request to the other connection, as every client's
// unwritten message, and the query follows it there, sending nothing
// more, so the request reaches the other multiplexer once and its reply
// ends the query. It went out again, beside the one handed over.
TEST(QueryStagesThreaded, AnUnwrittenRequestHandedOverIsFollowedNotSentAgain) {
  StandIn first(/*stalled=*/true), second;
  std::promise<ThreadedClient::Result> done;  // outlives the client, whose shutdown may end the query
  ThreadedClient client(peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", first.port, 5));
  multiplexer::LanePtr lane(new multiplexer::Lane());
  const std::string fill(fill_size(), 'f');
  for (int index = FILL_FRAMES; index > 0; --index) {  // past the sockets of a multiplexer that reads nothing
    client.send(client.new_message(types::TEST_EVENT, fill), lane);
  }
  MultiplexerMessage question = client.new_message(types::PYTHON_TEST_REQUEST, "question");
  client.query(question, [&done](const ThreadedClient::Result& result) { done.set_value(result); }, 60, lane);
  client.watched_ids();  // a call through the io thread: the query is placed, behind the events
  ASSERT_TRUE(client.connect("127.0.0.1", second.port, 5));
  first.hang_up();
  MultiplexerMessage msg;
  while (second.next(&msg) && msg.type() != types::PYTHON_TEST_REQUEST) {
  }
  ASSERT_EQ(types::PYTHON_TEST_REQUEST, msg.type()) << "the request never came";
  second.write(second.answer(types::PYTHON_TEST_RESPONSE, BACKEND, msg.id()));
  std::future<ThreadedClient::Result> answered = done.get_future();
  ASSERT_EQ(std::future_status::ready, answered.wait_for(std::chrono::seconds(30)));
  ASSERT_EQ(ThreadedClient::REPLIED, answered.get().outcome);
  client.shutdown(30);  // what was handed over is written first, a request sent again with it
  ASSERT_TRUE(second.wait_ended()) << "the client never closed";
  EXPECT_EQ(1, second.count(types::PYTHON_TEST_REQUEST)) << "the request went out again";
  EXPECT_EQ(0, second.count(types::BACKEND_FOR_PACKET_SEARCH)) << "the query moved on to the search";
}

// A query whose request waits unwritten behind what a lane sent before
// it, on `client`'s only connection, to `stalled`, a multiplexer that
// reads nothing; then that connection goes, and the outbox holds the
// request for the next connection. The query's result goes to `done`; its
// stages have 60 s each. Whether the query went on to wait for a
// connection with its search, as one whose request's connection was lost
// does, within 30 s: one that sat in its first stage would leave it at
// that stage's end, 60 s on.
bool hold_the_request(ThreadedClient& client, StandIn& stalled, std::promise<ThreadedClient::Result>& done) {
  multiplexer::LanePtr lane(new multiplexer::Lane());
  const std::string fill(fill_size(), 'f');
  for (int index = FILL_FRAMES; index > 0; --index) {  // past the sockets of a multiplexer that reads nothing
    client.send(client.new_message(types::TEST_EVENT, fill), lane);
  }
  client.query(
      client.new_message(types::PYTHON_TEST_REQUEST, "question"),
      [&done](const ThreadedClient::Result& result) { done.set_value(result); }, 60, lane);
  client.watched_ids();  // a call through the io thread: the query is placed, behind the events
  stalled.hang_up();
  for (int checks = 0; checks < 3000 && client.waiting_queries() != 1; ++checks) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return client.waiting_queries() == 1;
}

// The request is held for the next connection when its own goes, the
// only one: on no live connection, it counts as lost, and the query moves
// on to the search at once, which waits for a connection too. Once one
// comes up the request and the search go out there, the request once, and
// its reply ends the query. The query started over, and the request went
// out again beside the one held.
TEST(QueryStagesThreaded, ARequestHeldWhenItsConnectionWentMovesOnToTheSearch) {
  StandIn first(/*stalled=*/true), second;
  std::promise<ThreadedClient::Result> done;  // outlives the client, whose shutdown may end the query
  ThreadedClient client(peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", first.port, 5));
  ASSERT_TRUE(hold_the_request(client, first, done)) << "the query sat in its first stage";
  ASSERT_TRUE(client.connect("127.0.0.1", second.port, 5));
  std::uint64_t request = 0;
  bool searched = false;
  MultiplexerMessage msg;
  while ((!request || !searched) && second.next(&msg)) {
    if (msg.type() == types::PYTHON_TEST_REQUEST) {
      request = msg.id();
    } else if (msg.type() == types::BACKEND_FOR_PACKET_SEARCH) {
      searched = true;
      second.write(second.answer(types::DELIVERY_ERROR, second.id, msg.id()));  // nobody: the request may be answered
    }
  }
  ASSERT_NE(0u, request) << "the request never came";
  ASSERT_TRUE(searched) << "the search never came";
  second.write(second.answer(types::PYTHON_TEST_RESPONSE, BACKEND, request));
  std::future<ThreadedClient::Result> answered = done.get_future();
  ASSERT_EQ(std::future_status::ready, answered.wait_for(std::chrono::seconds(30)));
  ThreadedClient::Result result = answered.get();
  ASSERT_EQ(ThreadedClient::REPLIED, result.outcome);
  EXPECT_EQ("late", result.reply.third->message());
  client.shutdown(30);
  ASSERT_TRUE(second.wait_ended()) << "the client never closed";
  EXPECT_EQ(1, second.count(types::PYTHON_TEST_REQUEST)) << "the request went out again";
}

// The request held for the next connection, the query has moved on to the
// search, a backend perhaps having the request; the next multiplexer has
// no backend of the type and says so for the request and for the search,
// in either order. The request struck off, nothing can answer:
// OperationFailed at once, where a delivery error for the request was
// dropped in the search and in the late wait, which ran to the end of its
// stage, OperationTimedOut.
TEST(QueryStagesThreaded, AHeldRequestThatDrawsADeliveryErrorIsStruckOff) {
  StandIn first(/*stalled=*/true), second;
  std::promise<ThreadedClient::Result> done;  // outlives the client, whose shutdown may end the query
  ThreadedClient client(peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", first.port, 5));
  ASSERT_TRUE(hold_the_request(client, first, done)) << "the query sat in its first stage";
  ASSERT_TRUE(client.connect("127.0.0.1", second.port, 5));
  int refused = 0;
  MultiplexerMessage msg;
  while (refused < 2 && second.next(&msg)) {
    if (msg.type() == types::PYTHON_TEST_REQUEST || msg.type() == types::BACKEND_FOR_PACKET_SEARCH) {
      second.write(second.answer(types::DELIVERY_ERROR, second.id, msg.id()));
      ++refused;
    }
  }
  ASSERT_EQ(2, refused) << "the request and the search did not both come";
  std::future<ThreadedClient::Result> answered = done.get_future();
  ASSERT_EQ(std::future_status::ready, answered.wait_for(std::chrono::seconds(120)));  // past the stage's 60 s
  EXPECT_EQ(ThreadedClient::FAILED, answered.get().outcome) << "the late wait ran on with nothing that could answer";
}

}  // namespace
