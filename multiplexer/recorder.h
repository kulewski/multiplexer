// Recorder: writes the --record file of a multiplexer, one Record per
// event (Recording.proto), through a buffered stream on the io thread, so
// routing never waits on the disk. A write error switches recording off
// and is logged once; the multiplexer keeps serving.
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

class Recorder : boost::noncopyable {
public:
  // Opens `path` for appending. `payload_limit` bytes of each payload are
  // kept, all of it when 0. ok() says whether the file could be opened.
  Recorder(const std::string &path, unsigned int payload_limit);
  bool ok() const { return !failed_; }

  // The first record: who wrote the file and with which rules.
  void header(boost::uint64_t multiplexer_id, const std::string &rules_sha1);
  // A peer registered (CONNECTED) or left (DISCONNECTED).
  void peer(PeerEvent::Kind kind, boost::uint64_t peer_id, boost::uint32_t peer_type);
  // One delivery attempt of `msg`: to `recipient` of `recipient_type` (either
  // may be 0 when routing found nobody), with the outcome.
  void routed(const MultiplexerMessage &msg, boost::uint32_t from_peer_type, boost::uint64_t recipient,
              boost::uint32_t recipient_type, RoutedMessage::Disposition disposition, bool error_reported);

private:
  void _write(Record &record);

  std::ofstream out_;
  mx::protobuf::OstreamMessageOutputStream stream_;
  const unsigned int payload_limit_;
  bool failed_;
};

} // namespace multiplexer

#endif // MX_MULTIPLEXER_RECORDER_H_
