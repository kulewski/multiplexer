// EveryAddress: the lookups and the connections; see every_address.h.
#include "mxcontrol/every_address.h"

#include <signal.h>

#include <algorithm>
#include <asio/ip/tcp.hpp>
#include <iostream>
#include <map>

namespace mxcontrol {

typedef asio::ip::tcp::endpoint Endpoint;

namespace {

// Every signal blocked on the calling thread while it lives, so that a
// thread started meanwhile, asio's resolver thread, inherits the mask and
// never takes one: the subcommands' handlers set flags that the main
// thread alone reads. asio blocks them for its scheduler's own thread, not
// for this one.
class SignalsBlocked {
 public:
  SignalsBlocked() {
    sigset_t all;
    sigfillset(&all);
    pthread_sigmask(SIG_BLOCK, &all, &previous_);
  }
  ~SignalsBlocked() { pthread_sigmask(SIG_SETMASK, &previous_, nullptr); }

 private:
  sigset_t previous_;
};

}  // namespace

// What the lookups and their answers share, held weakly by a lookup under
// way so that its answer, arriving after the subcommand let go of the
// client, touches nothing.
struct EveryAddress::State {
  // One -M address and the state of its lookups.
  struct Address {
    std::string text;                // as given
    std::string host;                // empty when `text` is not host:port
    std::string port;                // a number or a service name, as getaddrinfo takes it
    bool literal = false;            // an address, not a name: it resolves to itself for good
    bool looking_up = false;         // a refresh's lookup is under way
    bool failing = false;            // its last lookup failed, which was said
    std::vector<Endpoint> resolved;  // what its last lookup that answered gave
  };

  State(multiplexer::Client& client, asio::io_service& io_service) : client(client), resolver(io_service) {}

  // The client's connection to `endpoint`, made the first time: the client
  // keeps it and reconnects it until it is dropped, and a second connect to
  // the same address would replace a connection that works.
  multiplexer::ConnectionWrapper connection(const Endpoint& endpoint) {
    std::map<Endpoint, multiplexer::ConnectionWrapper>::iterator entry = given.find(endpoint);
    if (entry == given.end()) {
      entry = given.emplace(endpoint, client.async_connect(endpoint)).first;
    }
    return entry->second;
  }

  // A refresh's lookup of addresses[index] ended: a connection to every
  // address that is new, then the addresses gone dropped; nothing at all
  // when the name did not resolve.
  void found(std::size_t index, const asio::error_code& error, const std::vector<Endpoint>& endpoints) {
    Address& address = addresses[index];
    address.looking_up = false;
    if (error || endpoints.empty()) {
      if (!address.failing) {
        std::cerr << "cannot resolve " << address.text << ": " << (error ? error.message() : "no address")
                  << "; its connections stay\n";
        address.failing = true;
      }
      return;
    }
    address.failing = false;
    address.resolved = endpoints;
    for (const Endpoint& endpoint : endpoints) {
      if (!given.count(endpoint)) {
        std::cerr << address.text << ": new address " << endpoint << ", connecting\n";
        connection(endpoint);
      }
    }
    drop_gone();
  }

  // Whether some -M address resolves to `endpoint`, by its last answer.
  bool resolved(const Endpoint& endpoint) const {
    for (const Address& address : addresses) {
      if (std::find(address.resolved.begin(), address.resolved.end(), endpoint) != address.resolved.end()) {
        return true;
      }
    }
    return false;
  }

  // Every address given that no -M address resolves to now, whose
  // connection, the last heard from, is down: dropped, so that the client
  // never connects there again, unless a name resolves to it again.
  void drop_gone() {
    for (std::map<Endpoint, multiplexer::ConnectionWrapper>::iterator next = given.begin(), entry;
         next != given.end() && (entry = next++, true);) {
      if (entry->second || resolved(entry->first)) {
        continue;
      }
      std::cerr << "dropping " << entry->first << ": no -M address resolves to it now, and its connection is down\n";
      client.disconnect(entry->first);
      given.erase(entry);
    }
  }

  multiplexer::Client& client;
  asio::ip::tcp::resolver resolver;
  std::vector<Address> addresses;
  // Every address the client has, with its connection: the first, or the
  // last a message came through (EveryAddress::heard).
  std::map<Endpoint, multiplexer::ConnectionWrapper> given;
};

EveryAddress::EveryAddress(multiplexer::Client& client, asio::io_service& io_service,
                           const std::vector<std::string>& addresses)
    : state_(std::make_shared<State>(client, io_service)) {
  for (const std::string& text : addresses) {
    State::Address address;
    address.text = text;
    const std::string::size_type colon = text.rfind(':');
    if (colon != std::string::npos) {
      address.host = colon ? text.substr(0, colon) : "127.0.0.1";
      address.port = text.substr(colon + 1);
      asio::error_code not_an_address;
      asio::ip::make_address(address.host, not_an_address);
      address.literal = !not_an_address;
    }
    state_->addresses.push_back(address);
  }
}

// Drops the state and with it the resolver, which cancels what has not
// started; a lookup already in getaddrinfo ends on asio's resolver thread
// whatever happens here, and its handler then finds the state gone.
EveryAddress::~EveryAddress() {}

unsigned int EveryAddress::connect(float timeout) {
  unsigned int reached = 0;
  for (State::Address& address : state_->addresses) {
    if (address.host.empty()) {
      std::cerr << "invalid multiplexer address " << address.text << " (host:port expected)\n";
      continue;
    }
    asio::error_code error;
    const asio::ip::tcp::resolver::results_type results =
        state_->resolver.resolve(address.host, address.port, asio::ip::resolver_base::address_configured, error);
    if (error) {
      std::cerr << "cannot resolve " << address.text << ": " << error.message() << "\n";
      address.failing = true;  // said: a refresh says it again only once it has resolved since
      continue;
    }
    unsigned int connected = 0;
    for (const asio::ip::tcp::resolver::results_type::value_type& entry : results) {
      address.resolved.push_back(entry.endpoint());
      // Connected means welcomed: a socket that opened but never finished
      // the handshake, refused or black-holed, is no multiplexer reached.
      const multiplexer::ConnectionWrapper connection = state_->connection(entry.endpoint());
      if (state_->client.wait_for_connection(connection, timeout)) {
        ++connected;
      } else {
        std::cerr << "cannot connect to " << entry.endpoint() << (connection ? " (no handshake in time)" : "") << "\n";
      }
    }
    if (connected) {
      ++reached;
    }
  }
  return reached;
}

void EveryAddress::refresh() {
  for (std::size_t index = 0; index < state_->addresses.size(); ++index) {
    State::Address& address = state_->addresses[index];
    if (address.host.empty() || address.literal || address.looking_up) {
      continue;
    }
    address.looking_up = true;
    std::weak_ptr<State> weak_state = state_;
    const SignalsBlocked blocked;  // asio starts its resolver thread in the first lookup
    state_->resolver.async_resolve(
        address.host, address.port, asio::ip::resolver_base::address_configured,
        [weak_state, index](const asio::error_code& error, const asio::ip::tcp::resolver::results_type& results) {
          std::shared_ptr<State> state = weak_state.lock();
          if (!state) {
            return;  // the subcommand is done with its client
          }
          std::vector<Endpoint> endpoints;
          for (const asio::ip::tcp::resolver::results_type::value_type& entry : results) {
            endpoints.push_back(entry.endpoint());
          }
          state->found(index, error, endpoints);
        });
  }
}

void EveryAddress::heard(const multiplexer::ConnectionWrapper& connection) {
  std::map<Endpoint, multiplexer::ConnectionWrapper>::iterator entry = state_->given.find(connection.endpoint());
  if (entry != state_->given.end()) {
    entry->second = connection;
  }
}

bool EveryAddress::resolved(const Endpoint& address) const { return state_->resolved(address); }

}  // namespace mxcontrol
