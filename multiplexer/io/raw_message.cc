// RawMessage: header encoding, size check and CRC. See the header for the
// object's life cycle.
#include <boost/crc.hpp>
#include <boost/static_assert.hpp>

#include "lib/assertion.h"
#include "lib/encoding/decode_from_range.h"
#include "lib/encoding/encode_to_range.h"
#include "lib/encoding/little_endian.h"

#include "multiplexer/io/raw_message.h"

using namespace multiplexer;

// Builds the scatter list Asio writes from: the 8-byte header and the body,
// both already in place, so writing a frame copies nothing.
void RawMessage::switch_to_writing() {
  Assert(usability_ == PRE_WRITING || usability_ == READING);
  Assert(writing_buffers_.empty());
  usability_ = READING; // fool get_header_buffer() and get_body_buffer()
  writing_buffers_.push_back(get_header_buffer());
  writing_buffers_.push_back(get_body_buffer());
  usability_ = WRITING;
}

bool RawMessage::unpack_header() {
  Assert(usability_ == READING);

  mx::encoders::DecodeFromRange<std::string::const_iterator, mx::encodings::LittleEndian> decoder(header_.begin(),
                                                                                                  header_.end());
  decoder(length_);
  decoder(crc32_);
  // A valid MultiplexerMessage is never empty (`type` is required). Leave the
  // object consistent on failure so callers can check the result safely.
  if (length_ == 0 || length_ > MAX_MESSAGE_SIZE) {
    length_ = 0;
    return false;
  }

  // allocate memory for reading
  Assert(contents_.empty());
  contents_.resize(length_);

  // indicate success
  return true;
}

bool RawMessage::verify() {
  Assert(usability_ == READING);
  AssertMsg(!contents_.empty(), "You can't verify partial message.");
  const bool ok = Crc32(contents_) == crc32_;
  if (!ok) {
    usability_ = NONE;
  } else {
    usability_ = PRE_WRITING;
    switch_to_writing();
  }
  return ok;
}

// Standard CRC-32 (the zlib one), so peers in any language can compute it.
boost::uint32_t RawMessage::Crc32(const std::string &message) {
  boost::crc_32_type crc;
  if (!message.empty())
    crc.process_bytes(message.data(), message.size());
  return crc.checksum();
}

void RawMessage::initialize_header() {
  mx::encoders::EncodeToRange<std::string::iterator, mx::encodings::LittleEndian> encoder(header_.begin(),
                                                                                          header_.end());
  encoder(length_);
  encoder(crc32_);
}
