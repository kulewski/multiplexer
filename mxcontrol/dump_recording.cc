// dump_recording: prints a multiplexer's --record file, one line per
// record (Recording.proto). With --rules the peer and message types are
// shown by name; without, by number. --type and --peer filter.
#include <cstdio>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "lib/protobuf/stream.h"
#include "multiplexer/Recording.pb.h" /* generated */
#include "multiplexer/config.h"
#include "mxcontrol/task.h"
#include "mxcontrol/tasks_holder.h"

namespace mxcontrol {

class DumpRecording : public Task {
public:
  virtual std::string short_description() const { return "print a recording made with run_multiplexer --record"; }
  virtual std::string short_synopsis(const std::string &commandname) { return "<" + commandname + "-options> FILE..."; }
  virtual int run();

protected:
  virtual void _initialize_options_description(po::options_description &options) {
    options.add_options()("file", po::value(&files_)->composing(),
                          "a recording; several are merged by time, each record marked with its multiplexer")(
        "rules", po::value(&rules_file_), "the rules file the multiplexer ran with, for peer and message type names")(
        "type", po::value(&type_)->default_value(0), "only routed messages of this type (number)")(
        "peer", po::value(&peer_)->default_value(0), "only records involving this instance id");
  }
  virtual void _initialize_positional_options_description(po::positional_options_description &positional) {
    positional.add("file", -1);
  }

private:
  std::vector<std::string> files_;
  std::string rules_file_;
  boost::uint32_t type_;
  boost::uint64_t peer_;
};

REGISTER_MXCONTROL_SUBCOMMAND(dump_recording, DumpRecording);

namespace {

typedef multiplexer::Config<std::multimap> Rules;

std::string peer_name(const Rules *rules, boost::uint32_t peer_type) {
  if (!peer_type)
    return "-";
  if (peer_type == multiplexer::RECORDING_CONTROLLER)
    return "RECORDING_CONTROLLER";
  if (rules) {
    Rules::PeerDescriptionById::const_iterator entry = rules->peer_by_type().find(peer_type);
    if (entry != rules->peer_by_type().end())
      return entry->second.name();
  }
  return std::to_string(peer_type);
}

std::string type_name(const Rules *rules, boost::uint32_t type) {
  if (rules) {
    Rules::MessageDescriptionById::const_iterator entry = rules->message_description_by_id().find(type);
    if (entry != rules->message_description_by_id().end())
      return entry->second.name();
  }
  return std::to_string(type);
}

bool involves(const multiplexer::Record &record, boost::uint64_t peer) {
  if (record.has_peer())
    return record.peer().peer_id() == peer;
  if (record.has_routed())
    return record.routed().from() == peer || record.routed().recipient() == peer || record.routed().to() == peer;
  return false;
}

std::string when(boost::uint64_t timestamp_us) {
  char text[32];
  std::snprintf(text, sizeof(text), "%llu.%06llu", (unsigned long long)(timestamp_us / 1000000),
                (unsigned long long)(timestamp_us % 1000000));
  return text;
}

} // namespace

// One recording being read: its stream, the record at its front, and the
// multiplexer that wrote it, from its header.
struct Stream {
  explicit Stream(int fd) : input(fd, true), multiplexer_id(0), pending(false) {}
  bool next() {
    record.Clear();
    pending = input.read(record);
    if (pending && record.has_header())
      multiplexer_id = record.header().multiplexer_id();
    return pending;
  }
  mx::protobuf::FileMessageInputStream input;
  multiplexer::Record record;
  boost::uint64_t multiplexer_id;
  bool pending;
};

int DumpRecording::run() {
  if (files_.empty()) {
    std::cerr << "dump_recording: no file given\n";
    return 1;
  }
  std::unique_ptr<Rules> rules;
  if (!rules_file_.empty())
    rules.reset(new Rules(rules_file_));
  std::vector<std::unique_ptr<Stream>> streams;
  for (std::vector<std::string>::const_iterator file = files_.begin(); file != files_.end(); ++file) {
    int fd = ::open(file->c_str(), O_RDONLY);
    if (fd < 0) {
      std::cerr << "dump_recording: cannot open " << *file << "\n";
      return 1;
    }
    streams.emplace_back(new Stream(fd));
    streams.back()->next();
  }
  const bool several = streams.size() > 1;
  for (;;) {
    // The earliest pending record across the files; a tap's stream may
    // carry its own multiplexer id, a file's comes from its header.
    Stream *earliest = NULL;
    for (std::vector<std::unique_ptr<Stream>>::iterator stream = streams.begin(); stream != streams.end(); ++stream)
      if ((*stream)->pending && (!earliest || (*stream)->record.timestamp_us() < earliest->record.timestamp_us()))
        earliest = stream->get();
    if (!earliest)
      break;
    const multiplexer::Record &record = earliest->record;
    const bool wanted =
        !(type_ && !(record.has_routed() && record.routed().type() == type_)) && !(peer_ && !involves(record, peer_));
    if (wanted) {
      std::cout << when(record.timestamp_us()) << " ";
      const boost::uint64_t multiplexer_id =
          record.multiplexer_id() ? record.multiplexer_id() : earliest->multiplexer_id;
      if ((several || record.multiplexer_id()) && multiplexer_id)
        std::cout << "mx=" << multiplexer_id << " ";
      if (record.has_header()) {
        const multiplexer::RecordingHeader &header = record.header();
        std::cout << "header multiplexer=" << header.multiplexer_id() << " rules=" << header.rules_sha1().substr(0, 12)
                  << " payload_limit=" << header.payload_limit();
        if (header.has_label())
          std::cout << " label=" << header.label();
        std::cout << "\n";
      } else if (record.has_peer()) {
        const multiplexer::PeerEvent &peer = record.peer();
        std::cout << "peer " << multiplexer::PeerEvent::Kind_Name(peer.kind()) << " id=" << peer.peer_id()
                  << " type=" << peer_name(rules.get(), peer.peer_type()) << "\n";
      } else if (record.has_routed()) {
        const multiplexer::RoutedMessage &routed = record.routed();
        std::cout << "routed " << multiplexer::RoutedMessage::Disposition_Name(routed.disposition())
                  << " type=" << type_name(rules.get(), routed.type()) << " id=" << routed.id()
                  << " from=" << routed.from() << " (" << peer_name(rules.get(), routed.from_peer_type()) << ")";
        if (routed.recipient() || routed.recipient_peer_type())
          std::cout << " -> " << routed.recipient() << " (" << peer_name(rules.get(), routed.recipient_peer_type())
                    << ")";
        if (routed.to())
          std::cout << " to=" << routed.to();
        if (routed.references())
          std::cout << " references=" << routed.references();
        if (routed.error_reported())
          std::cout << " error_reported";
        std::cout << " payload_bytes=" << routed.payload().size() << (routed.truncated() ? " truncated" : "") << "\n";
      }
    }
    earliest->next();
  }
  return 0;
}

} // namespace mxcontrol
