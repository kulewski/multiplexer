// See common.h.
#include "tests/roles/cc/common.h"

#include <google/protobuf/text_format.h>

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <mutex>

#include "lib/memory.h"
#include "lib/repr.h"

namespace mxtestroles {

Event event(const std::string& name) {
  Event result;
  result.set_event(name);
  return result;
}

// Roles report from several threads (workers, and the threaded client's io
// thread), so lines are written under a lock.
static std::mutex emit_mutex;

void emit(const Event& event) {
  google::protobuf::TextFormat::Printer printer;
  printer.SetSingleLineMode(true);
  std::string line;
  printer.PrintToString(event, &line);
  std::lock_guard<std::mutex> lock(emit_mutex);
  std::cout << line << "\n" << std::flush;
}

namespace {
// Whether `data` is UTF-8 as the text format reads a string field back:
// what Python's parser of the event line accepts.
bool valid_utf8(const std::string& data) {
  std::size_t index = 0;
  while (index < data.size()) {
    const unsigned char lead = static_cast<unsigned char>(data[index]);
    std::size_t length = 0;
    std::uint32_t point = 0;
    if (lead < 0x80) {
      length = 1;
      point = lead;
    } else if ((lead & 0xE0) == 0xC0) {
      length = 2;
      point = lead & 0x1F;
    } else if ((lead & 0xF0) == 0xE0) {
      length = 3;
      point = lead & 0x0F;
    } else if ((lead & 0xF8) == 0xF0) {
      length = 4;
      point = lead & 0x07;
    } else {
      return false;
    }
    if (index + length > data.size()) {
      return false;
    }
    for (std::size_t next = 1; next < length; ++next) {
      const unsigned char byte = static_cast<unsigned char>(data[index + next]);
      if ((byte & 0xC0) != 0x80) {
        return false;
      }
      point = (point << 6) | (byte & 0x3F);
    }
    // Not the shortest form, a surrogate, or past the last code point.
    static const std::uint32_t least[] = {0, 0, 0x80, 0x800, 0x10000};
    if (point < least[length] || (point >= 0xD800 && point <= 0xDFFF) || point > 0x10FFFF) {
      return false;
    }
    index += length;
  }
  return true;
}
}  // namespace

// The payload itself when it is short and UTF-8, as the Python roles give
// it; only its size otherwise, since a line with bytes that are not UTF-8
// would not parse back and the event would be lost.
void set_payload(Event& event, const std::string& data) {
  event.set_size(data.size());
  if (data.size() <= 256 && valid_utf8(data)) {
    event.set_payload(data);
  }
}

Event memory_event(long after) {
  Event memory = event("memory");
  memory.set_after(after);
  memory.set_heap_bytes(mx::heap_in_use_bytes());
  return memory;
}

double ms_since(const std::chrono::steady_clock::time_point& start) {
  return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
}

volatile std::sig_atomic_t stop_requested = 0;
static void on_signal(int) { stop_requested = 1; }

void install_signal_handlers() {
  std::signal(SIGTERM, on_signal);
  std::signal(SIGINT, on_signal);
}

void CommonOptions::add(mx::options::Options& options) {
  options.add("mx", &mx, "host:port of a multiplexer, repeatable");
  options.add("type", &type, "peer type id").required();
  options.add("name", &name, "", "label used in events");
}

multiplexer::backend::MultiplexerAddresses CommonOptions::addresses() const {
  multiplexer::backend::MultiplexerAddresses addresses;
  for (size_t index = 0; index < mx.size(); ++index) {
    std::string::size_type colon = mx[index].rfind(':');
    addresses.push_back(
        std::make_pair(mx[index].substr(0, colon), mx::from_string<std::uint16_t>(mx[index].substr(colon + 1))));
  }
  return addresses;
}

std::unique_ptr<Client> CommonOptions::connect() const {
  std::unique_ptr<Client> client(new Client(type));
  for (const multiplexer::backend::MultiplexerAddress& address : addresses()) {
    client->connect(address.first, address.second);
  }
  return client;
}

Event CommonOptions::connected_event(Client& client) const {
  Event connected = event("connected");
  connected.set_instance_id(client.instance_id());
  connected.set_connections(client.connections_count());
  connected.set_name(name);
  return connected;
}

std::map<std::uint32_t, std::uint32_t> kv_ints(const std::vector<std::string>& items) {
  std::map<std::uint32_t, std::uint32_t> out;
  for (size_t index = 0; index < items.size(); ++index) {
    std::string::size_type eq = items[index].find('=');
    out[mx::from_string<std::uint32_t>(items[index].substr(0, eq))] =
        mx::from_string<std::uint32_t>(items[index].substr(eq + 1));
  }
  return out;
}

std::vector<std::pair<std::uint32_t, std::string>> typed_payloads(const std::vector<std::string>& items) {
  std::vector<std::pair<std::uint32_t, std::string>> out;
  for (size_t index = 0; index < items.size(); ++index) {
    std::string::size_type colon = items[index].find(':');
    out.push_back(
        std::make_pair(mx::from_string<std::uint32_t>(items[index].substr(0, colon)), items[index].substr(colon + 1)));
  }
  return out;
}

std::string replace_all(std::string text, const std::string& from, const std::string& to) {
  for (std::string::size_type pos = text.find(from); pos != std::string::npos; pos = text.find(from, pos + to.size())) {
    text.replace(pos, from.size(), to);
  }
  return text;
}

std::string upper(std::string text) {
  for (size_t index = 0; index < text.size(); ++index) {
    text[index] = std::toupper(static_cast<unsigned char>(text[index]));
  }
  return text;
}

}  // namespace mxtestroles
