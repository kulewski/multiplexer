// mxcontrol_fake_dns --hosts FILE <mxcontrol arguments>: mxcontrol's
// recording command, built from the code mxcontrol runs, with the names
// under .test (RFC 6761's names for testing) looked up in FILE, so that a
// scenario moves a name to another address with no root and no change to
// the host. FILE has /etc/hosts' lines, an address and the names it has, `#`
// starting a comment, and is read again at every lookup, as the C library
// reads /etc/hosts; a .test name has every address listed for it, in the
// file's order, and none when none is, or there is no file. Every other
// name, and every address, goes to the system resolver.
//
// The lookup is replaced at link time: this binary defines getaddrinfo(),
// which asio's resolver calls, and reaches the C library's own through
// dlsym(RTLD_NEXT), as multiplexer/fork_test.cc does with
// pthread_mutex_lock. Each address listed is resolved by the C library as
// the number it is, so the list returned is the C library's own, which its
// freeaddrinfo() frees. Lookups run on asio's resolver thread as well as
// the main one; FILE's name is set before the first lookup, on the main
// thread, before the other exists.
#include <dlfcn.h>
#include <netdb.h>

#include <atomic>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "lib/program.h" /* main() */
#include "mxcontrol/driver.h"

namespace {

const char* hosts_file = nullptr;  // --hosts

using LookupFunction = int (*)(const char*, const char*, const struct addrinfo*, struct addrinfo**);
std::atomic<LookupFunction> real_lookup(nullptr);

// The C library's getaddrinfo.
LookupFunction system_lookup() {
  LookupFunction real = real_lookup.load(std::memory_order_acquire);
  if (!real) {
    real = reinterpret_cast<LookupFunction>(dlsym(RTLD_NEXT, "getaddrinfo"));
    real_lookup.store(real, std::memory_order_release);
  }
  return real;
}

// Whether `name` is one FILE answers for: under .test.
bool under_test_domain(const std::string& name) {
  static const std::string suffix = ".test";
  return name.size() > suffix.size() && name.compare(name.size() - suffix.size(), suffix.size(), suffix) == 0;
}

// The addresses FILE lists for `name`, in its order.
std::vector<std::string> listed_addresses(const std::string& name) {
  std::vector<std::string> addresses;
  std::ifstream file(hosts_file);
  std::string line;
  while (std::getline(file, line)) {
    std::istringstream words(line.substr(0, line.find('#')));
    std::string address;
    std::string word;
    words >> address;
    while (words >> word) {
      if (word == name) {
        addresses.push_back(address);
        break;
      }
    }
  }
  return addresses;
}

}  // namespace

// What asio's resolver calls: a .test name's addresses from FILE, each with
// the service and hints asked for, anything else from the C library.
extern "C" int getaddrinfo(const char* node, const char* service, const struct addrinfo* hints,
                           struct addrinfo** result) {
  if (!node || !hosts_file || !under_test_domain(node)) {
    return system_lookup()(node, service, hints, result);
  }
  struct addrinfo numeric = {};
  if (hints) {
    numeric = *hints;
  }
  numeric.ai_flags |= AI_NUMERICHOST;
  *result = nullptr;
  struct addrinfo** tail = result;
  for (const std::string& address : listed_addresses(node)) {
    struct addrinfo* found = nullptr;
    if (system_lookup()(address.c_str(), service, &numeric, &found) == 0) {
      *tail = found;  // appended: the C library's freeaddrinfo frees a list entry by entry
      while (*tail) {
        tail = &(*tail)->ai_next;
      }
    }
  }
  return *result ? 0 : EAI_NONAME;
}

// Takes --hosts FILE off the front and runs the rest as mxcontrol would.
int MxMain(int argc, char** argv) {
  if (argc < 3 || std::strcmp(argv[1], "--hosts") != 0) {
    std::cerr << "usage: " << argv[0] << " --hosts FILE <mxcontrol arguments>\n";
    return 2;
  }
  hosts_file = argv[2];
  argv[2] = argv[0];
  return mxcontrol::run_tasks(argc - 2, argv + 2);
}
