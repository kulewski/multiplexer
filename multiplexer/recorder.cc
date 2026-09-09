// See recorder.h.
#include "multiplexer/recorder.h"

#include <chrono>
#include <ctime>

#include "lib/logging/logging.h"
#include "lib/repr.h"

namespace multiplexer {
namespace recording {

boost::uint64_t now_us() {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

void fill_peer(Record &record, PeerEvent::Kind kind, boost::uint64_t peer_id, boost::uint32_t peer_type) {
  PeerEvent *event = record.mutable_peer();
  event->set_kind(kind);
  event->set_peer_id(peer_id);
  event->set_peer_type(peer_type);
}

void fill_routed(Record &record, const MultiplexerMessage &msg, boost::uint32_t from_peer_type,
                 boost::uint64_t recipient, boost::uint32_t recipient_type, RoutedMessage::Disposition disposition,
                 bool error_reported) {
  RoutedMessage *routed = record.mutable_routed();
  routed->set_id(msg.id());
  routed->set_from(msg.from());
  if (msg.to())
    routed->set_to(msg.to());
  if (msg.references())
    routed->set_references(msg.references());
  routed->set_type(msg.type());
  if (!msg.workflow().empty())
    routed->set_workflow(msg.workflow());
  routed->set_from_peer_type(from_peer_type);
  if (recipient)
    routed->set_recipient(recipient);
  if (recipient_type)
    routed->set_recipient_peer_type(recipient_type);
  routed->set_disposition(disposition);
  routed->set_error_reported(error_reported);
  routed->set_payload(msg.message());
}

Record truncate(const Record &record, unsigned int payload_limit) {
  Record copy(record);
  if (payload_limit && copy.has_routed() && copy.routed().payload().size() > payload_limit) {
    copy.mutable_routed()->mutable_payload()->resize(payload_limit);
    copy.mutable_routed()->set_truncated(true);
  }
  return copy;
}

bool valid_label(const std::string &label) {
  if (label.empty() || label.size() > 64)
    return false;
  for (std::string::const_iterator c = label.begin(); c != label.end(); ++c) {
    const bool ok =
        (*c >= 'a' && *c <= 'z') || (*c >= 'A' && *c <= 'Z') || (*c >= '0' && *c <= '9') || *c == '-' || *c == '_';
    if (!ok)
      return false;
  }
  return true;
}

std::string session_path(const std::string &dir, const std::string &label, boost::uint64_t multiplexer_id,
                         boost::uint64_t started_us) {
  std::time_t seconds = static_cast<std::time_t>(started_us / 1000000);
  struct tm utc;
  gmtime_r(&seconds, &utc);
  char when[32];
  std::strftime(when, sizeof(when), "%Y%m%dT%H%M%S", &utc);
  char micros[16];
  std::snprintf(micros, sizeof(micros), ".%06uZ", static_cast<unsigned int>(started_us % 1000000));
  std::string directory = dir;
  if (!directory.empty() && directory[directory.size() - 1] != '/')
    directory += '/';
  return directory + label + "." + when + micros + "." + mx::repr(multiplexer_id) + ".rec";
}

} // namespace recording

Recorder::Recorder(const std::string &path, unsigned int payload_limit)
    : path_(path), out_(path.c_str(), std::ios::out | std::ios::app | std::ios::binary), stream_(&out_, false),
      payload_limit_(payload_limit), failed_(!out_.good()), bytes_(0), records_(0) {
  if (failed_)
    MX_LOG(ERROR, LOWVERBOSITY, CTX("multiplexer.recorder") TEXT("cannot open " + path + "; not recording"));
}

void Recorder::header(boost::uint64_t multiplexer_id, const std::string &rules_sha1, const std::string &label) {
  Record record;
  RecordingHeader *header = record.mutable_header();
  header->set_multiplexer_id(multiplexer_id);
  header->set_rules_sha1(rules_sha1);
  header->set_started_us(recording::now_us());
  header->set_payload_limit(payload_limit_);
  if (!label.empty())
    header->set_label(label);
  record.set_timestamp_us(recording::now_us());
  _write(record);
}

void Recorder::write(const Record &record) {
  if (failed_)
    return;
  if (payload_limit_ && record.has_routed() && record.routed().payload().size() > payload_limit_)
    _write(recording::truncate(record, payload_limit_));
  else
    _write(record);
}

// One record, or the first failure: log it once and stop writing.
void Recorder::_write(const Record &record) {
  if (!stream_.write(record) || !out_.good()) {
    failed_ = true;
    MX_LOG(ERROR, LOWVERBOSITY, CTX("multiplexer.recorder") TEXT("write failed; recording stopped"));
    return;
  }
  bytes_ = static_cast<boost::uint64_t>(out_.tellp());
  records_ += 1;
}

} // namespace multiplexer
