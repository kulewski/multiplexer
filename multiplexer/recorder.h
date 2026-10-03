// Recording: the Record messages (Recording.proto) a multiplexer produces,
// and the Recorder that writes them to a file through a buffered stream.
// It runs on the io thread, with blocking calls: the open, every write, the
// flush when the buffer fills or the server asks, and the close, so a slow
// or stalled file system holds up routing while it lasts; record to local
// storage. A write error switches the file off and is logged once; the
// multiplexer keeps serving. The server (server.h) owns one Recorder per
// file session, flushes it once a second while it is open, and streams the
// same records to the peers that tapped in.
#ifndef MX_MULTIPLEXER_RECORDER_H_
#define MX_MULTIPLEXER_RECORDER_H_

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>

#include "lib/protobuf/stream.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/Recording.pb.h"   /* generated */

namespace multiplexer {
namespace recording {

// Microseconds since the epoch, the clock every record is stamped with.
std::uint64_t now_us();

// A peer registered (CONNECTED) or left (DISCONNECTED).
void fill_peer(Record& record, PeerEvent::Kind kind, std::uint64_t peer_id, std::uint32_t peer_type);
// A peer's routing changed (PEER_CONTROL): a ROUTING event with the flags now in effect.
void fill_peer_routing(Record& record, std::uint64_t peer_id, std::uint32_t peer_type, const Routing& routing);
// Another rules file was put in use: what a reader needs to know that the
// numbers changed under it.
void fill_rules(Record& record, const std::string& fingerprint, const std::string& path, std::uint32_t message_types,
                std::uint32_t peer_types);

// One delivery attempt of `msg`: to `recipient` of `recipient_type` (either
// may be 0 when routing found nobody), with the outcome. The first
// `payload_limit` bytes of the payload are copied into the record, all of
// it for 0, `truncated` set when that cut it: the most any sink keeps, so
// that what no sink keeps is never copied. cut_payload() cuts it further
// for a sink with a smaller limit.
void fill_routed(Record& record, const MultiplexerMessage& msg, std::uint32_t from_peer_type, std::uint64_t recipient,
                 std::uint32_t recipient_type, RoutedMessage::Disposition disposition, bool error_reported,
                 unsigned int payload_limit = 0);

// Cuts `record`'s payload to its first `length` bytes in place, setting
// `truncated`, when it is a routed message longer than that: a string cut
// keeps its storage, so nothing is copied, and a sink whose limit is
// smaller than the one before it is served from the same record.
void cut_payload(Record& record, std::size_t length);

// Whether `label` may name a session: letters, digits, '-' and '_', one to
// 64 characters, so that it is safe as part of a file name.
bool valid_label(const std::string& label);

// The file of a session: "<dir>/<label>.<UTC time>.<multiplexer id>.rec",
// unique across multiplexers sharing a directory and across sessions.
std::string session_path(const std::string& dir, const std::string& label, std::uint64_t multiplexer_id,
                         std::uint64_t started_us);

}  // namespace recording

// One recording file. Counts what it wrote, for the status.
class Recorder {
  Recorder(const Recorder&) = delete;
  Recorder& operator=(const Recorder&) = delete;

 public:
  // Opens `path` for appending, first cutting what follows the last whole
  // record of a file that exists, a record a multiplexer that died left
  // half written, which would make every record after it unreadable; a
  // file that exists is so read once. `payload_limit` bytes of each
  // payload are kept, all of it when 0. ok() says whether the file could be
  // read and opened.
  Recorder(const std::string& path, unsigned int payload_limit);
  bool ok() const { return !failed_; }

  const std::string& path() const { return path_; }
  unsigned int payload_limit() const { return payload_limit_; }
  // What this recorder wrote, with what the buffer still holds, which
  // reaches the file at the next flush: the bytes, as protobuf's output
  // stream counted them, and the records. Of an appended file, only this
  // session's.
  std::uint64_t bytes() const { return stream_.bytes(); }
  std::uint64_t records() const { return records_; }

  // The first record: who wrote the file, with which rules, under which label.
  void header(std::uint64_t multiplexer_id, const std::string& rules_fingerprint, const std::string& label);
  // Any other record, already stamped with the time, its payload cut to
  // the limit.
  void write(const Record& record);
  // What the buffer holds goes to the file now; a buffer that holds
  // nothing costs no system call. A failure switches the file off, as a
  // write's does.
  void flush();

 private:
  void _write(const Record& record);
  void _fail();

  const std::string path_;
  const std::int64_t cut_;  // bytes of a torn last record cut before the open; -1 when that failed
  std::ofstream out_;
  mx::protobuf::OstreamMessageOutputStream stream_;
  const unsigned int payload_limit_;
  bool failed_;
  std::uint64_t records_;
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_RECORDER_H_
