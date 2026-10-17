// RawMessage: header encoding, size check and checksum. See the header for the
// object's life cycle.

#include "multiplexer/io/raw_message.h"

#include "lib/assertion.h"
#include "lib/encoding/decode_from_range.h"
#include "lib/encoding/encode_to_range.h"
#include "lib/encoding/little_endian.h"
#include "lib/xxh3.h"

using namespace multiplexer;

// Builds the scatter list Asio writes from: the 8-byte header and the body,
// both already in place, so writing a frame copies nothing.
void RawMessage::switch_to_writing() const {
  Assert(usability_ == PRE_WRITING || usability_ == READING);
  Assert(writing_buffers_.empty());
  writing_buffers_.push_back(asio::buffer(header_.data(), header_.size()));
  writing_buffers_.push_back(asio::buffer(contents_.data(), contents_.size()));
  usability_ = WRITING;
}

void RawMessage::seal() const {
  if (usability_ != UNSEALED) {
    return;
  }
  checksum_ = Checksum(contents_);
  initialize_header();
  usability_ = PRE_WRITING;
  switch_to_writing();
}

bool RawMessage::unpack_header() {
  Assert(usability_ == READING);

  mx::encoders::DecodeFromRange<std::string::const_iterator, mx::encodings::LittleEndian> decoder(header_.begin(),
                                                                                                  header_.end());
  decoder(length_);
  decoder(checksum_);
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

bool RawMessage::unpack_header(const char* header) {
  Assert(usability_ == READING);
  header_.assign(header, HEADER_LENGTH);
  return unpack_header();
}

std::uint32_t RawMessage::body_length(const char* header) {
  mx::encoders::DecodeFromRange<const char*, mx::encodings::LittleEndian> decoder(header, header + HEADER_LENGTH);
  std::uint32_t length = 0;
  decoder(length);
  return length > MAX_MESSAGE_SIZE ? 0 : length;
}

bool RawMessage::verify() {
  Assert(usability_ == READING);
  AssertMsg(!contents_.empty(), "You can't verify partial message.");
  const bool ok = Checksum(contents_) == checksum_;
  if (!ok) {
    usability_ = NONE;
  } else {
    usability_ = PRE_WRITING;
    switch_to_writing();
  }
  return ok;
}

// The low 32 bits of the body's XXH3-64, by the widest vector instructions
// this processor has (lib/xxh3.cc).
std::uint32_t RawMessage::Checksum(const std::string& message) {
  return static_cast<std::uint32_t>(mx::xxh3_64(message.data(), message.size()));
}

void RawMessage::initialize_header() const {
  mx::encoders::EncodeToRange<std::string::iterator, mx::encodings::LittleEndian> encoder(header_.begin(),
                                                                                          header_.end());
  encoder(length_);
  encoder(checksum_);
}
