// See common.h.
#include "tests/roles/cc/common.h"

#include <cstdio>
#include <iostream>
#include <mutex>

#include <google/protobuf/text_format.h>

#include "lib/memory.h"

namespace mxtestroles {

Event event(const std::string &name) {
  Event result;
  result.set_event(name);
  return result;
}

// Roles report from several threads (workers, and the threaded client's io
// thread), so lines are written under a lock.
static std::mutex emit_mutex;

void emit(const Event &event) {
  google::protobuf::TextFormat::Printer printer;
  printer.SetSingleLineMode(true);
  std::string line;
  printer.PrintToString(event, &line);
  std::lock_guard<std::mutex> lock(emit_mutex);
  std::cout << line << "\n" << std::flush;
}

void set_payload(Event &event, const std::string &data) {
  event.set_size(data.size());
  if (data.size() <= 256)
    event.set_payload(data);
}

Event memory_event(long after) {
  Event memory = event("memory");
  memory.set_after(after);
  memory.set_heap_bytes(mx::heap_in_use_bytes());
  return memory;
}

double ms_since(const std::chrono::steady_clock::time_point &start) {
  return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
}

volatile std::sig_atomic_t stop_requested = 0;
static void on_signal(int) { stop_requested = 1; }

void install_signal_handlers() {
  std::signal(SIGTERM, on_signal);
  std::signal(SIGINT, on_signal);
}

void CommonOptions::add(po::options_description &options) {
  options.add_options()("mx", po::value(&mx)->composing(), "host:port of a multiplexer, repeatable")(
      "type", po::value(&type)->required(), "peer type id")("name", po::value(&name)->default_value(""),
                                                            "label used in events");
}

std::unique_ptr<Client> CommonOptions::connect() const {
  std::unique_ptr<Client> client(new Client(type));
  for (size_t index = 0; index < mx.size(); ++index) {
    std::string::size_type colon = mx[index].rfind(':');
    std::string host = mx[index].substr(0, colon);
    boost::uint16_t port = boost::lexical_cast<boost::uint16_t>(mx[index].substr(colon + 1));
    client->connect(host, port);
  }
  return client;
}

Event CommonOptions::connected_event(Client &client) const {
  Event connected = event("connected");
  connected.set_instance_id(client.instance_id());
  connected.set_connections(client.connections_count());
  connected.set_name(name);
  return connected;
}

std::map<boost::uint32_t, boost::uint32_t> kv_ints(const std::vector<std::string> &items) {
  std::map<boost::uint32_t, boost::uint32_t> out;
  for (size_t index = 0; index < items.size(); ++index) {
    std::string::size_type eq = items[index].find('=');
    out[boost::lexical_cast<boost::uint32_t>(items[index].substr(0, eq))] =
        boost::lexical_cast<boost::uint32_t>(items[index].substr(eq + 1));
  }
  return out;
}

std::vector<std::pair<boost::uint32_t, std::string>> typed_payloads(const std::vector<std::string> &items) {
  std::vector<std::pair<boost::uint32_t, std::string>> out;
  for (size_t index = 0; index < items.size(); ++index) {
    std::string::size_type colon = items[index].find(':');
    out.push_back(std::make_pair(boost::lexical_cast<boost::uint32_t>(items[index].substr(0, colon)),
                                 items[index].substr(colon + 1)));
  }
  return out;
}

std::string replace_all(std::string text, const std::string &from, const std::string &to) {
  for (std::string::size_type pos = text.find(from); pos != std::string::npos; pos = text.find(from, pos + to.size()))
    text.replace(pos, from.size(), to);
  return text;
}

std::string upper(std::string text) {
  for (size_t index = 0; index < text.size(); ++index)
    text[index] = std::toupper(static_cast<unsigned char>(text[index]));
  return text;
}

} // namespace mxtestroles
