// Recording: the Record messages (Recording.proto) a multiplexer produces,
// and the Recorder that writes them to a file, through a buffered stream on
// the io thread, so routing never waits on the disk. A write error switches
// the file off and is logged once; the multiplexer keeps serving. The
// server (server.h) owns one Recorder per file session and streams the
// same records to the peers that tapped in.
#ifndef MX_MULTIPLEXER_RECORDER_H_
#define MX_MULTIPLEXER_RECORDER_H_

#include <fstream>
#include <memory>
#include <string>

#include <boost/cstdint.hpp>
#include <boost/noncopyable.hpp>

#include "lib/protobuf/stream.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/Recording.pb.h"   /* generated */

namespace multiplexer {
namespace recording {

// Microseconds since the epoch, the clock every record is stamped with.
boost::uint64_t now_us();

// A peer registered (CONNECTED) or left (DISCONNECTED).
void fill_peer(Record &record, PeerEvent::Kind kind, boost::uint64_t peer_id, boost::uint32_t peer_type);

// One delivery attempt of `msg`: to `recipient` of `recipient_type` (either
// may be 0 when routing found nobody), with the outcome. The whole payload
// is kept; truncate() cuts it for a sink with a limit.
void fill_routed(Record &record, const MultiplexerMessage &msg, boost::uint32_t from_peer_type,
                 boost::uint64_t recipient, boost::uint32_t recipient_type, RoutedMessage::Disposition disposition,
                 bool error_reported);

// `record` with its payload cut to `payload_limit` bytes (`truncated` set)
// when it is a routed message longer than that; 0 means no limit.
Record truncate(const Record &record, unsigned int payload_limit);

// Whether `label` may name a session: letters, digits, '-' and '_', one to
// 64 characters, so that it is safe as part of a file name.
bool valid_label(const std::string &label);

// The file of a session: "<dir>/<label>.<UTC time>.<multiplexer id>.rec",
// unique across multiplexers sharing a directory and across sessions.
std::string session_path(const std::string &dir, const std::string &label, boost::uint64_t multiplexer_id,
                         boost::uint64_t started_us);

} // namespace recording

// One recording file. Counts what it wrote, for the status.
class Recorder : boost::noncopyable {
public:
  // Opens `path` for appending. `payload_limit` bytes of each payload are
  // kept, all of it when 0. ok() says whether the file could be opened.
  Recorder(const std::string &path, unsigned int payload_limit);
  bool ok() const { return !failed_; }

  const std::string &path() const { return path_; }
  unsigned int payload_limit() const { return payload_limit_; }
  boost::uint64_t bytes() const { return bytes_; }
  boost::uint64_t records() const { return records_; }

  // The first record: who wrote the file, with which rules, under which label.
  void header(boost::uint64_t multiplexer_id, const std::string &rules_sha1, const std::string &label);
  // Any other record, already stamped with the time, its payload cut to
  // the limit.
  void write(const Record &record);

private:
  void _write(const Record &record);

  const std::string path_;
  std::ofstream out_;
  mx::protobuf::OstreamMessageOutputStream stream_;
  const unsigned int payload_limit_;
  bool failed_;
  boost::uint64_t bytes_;
  boost::uint64_t records_;
};

} // namespace multiplexer

#endif // MX_MULTIPLEXER_RECORDER_H_
