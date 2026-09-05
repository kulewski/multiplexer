// One frame on the wire: a header of two little-endian uint32 (body length,
// CRC-32 of the body) followed by the body, a serialized MultiplexerMessage.
// docs/wire_format.md is the reference.
//
// The same object is used for reading (buffers for Asio to fill, then
// unpack_header() and verify()) and for writing (a fixed header plus body,
// exposed as a buffer list). A frame received by the multiplexer is
// forwarded as the same RawMessage, so the body bytes are never copied or
// re-serialized on the routing path.
#ifndef MX_MULTIPLEXER_IO_RAW_MESSAGE_H_
#define MX_MULTIPLEXER_IO_RAW_MESSAGE_H_

#include <boost/asio/buffer.hpp>
#include <google/protobuf/message.h>
#include <list>
#include <string>

#include "multiplexer/defaults.h"

namespace multiplexer {

/**
 * RawMessage is a structured representation of wire format [length, crc, body].
 * It has well defined life cycles:
 *	    create empty -> read -> freeze -> use for writing
 *	    create frozen with message ->  use for writing
 * so that we don't have to serialize back the message after it's deserialized
 * -- save on Protocol Buffers serialization -- prior to forwarding it to other
 *peers.
 */
class RawMessage {
public:
  enum Usability { READING, WRITING, NONE, PRE_WRITING };
  static const boost::uint32_t HEADER_LENGTH = 8; // for length_ and crc32_

  // construct message suitable for reading input with ASIO
  inline RawMessage() : usability_(READING), length_(0), crc32_(0), header_(HEADER_LENGTH, 0) {
    BOOST_STATIC_ASSERT((HEADER_LENGTH == sizeof(length_) + sizeof(crc32_)));
  }

  // construct message suitable for writing output with ASIO
  inline explicit RawMessage(const std::string &message)
      : usability_(PRE_WRITING), length_(message.size()), crc32_(Crc32(message)), header_(HEADER_LENGTH, 0),
        contents_(message) {
    Assert(contents_.size() <= MAX_MESSAGE_SIZE);
    initialize_header();
    switch_to_writing();
  }

  // like above but destroys `contents'
  inline explicit RawMessage(std::string *message)
      : usability_(PRE_WRITING), length_(message->size()), crc32_(Crc32(*message)), header_(HEADER_LENGTH, 0),
        contents_() {
    contents_.swap(*message);
    Assert(contents_.size() <= MAX_MESSAGE_SIZE);
    initialize_header();
    switch_to_writing();
  }

  static RawMessage *FromMessage(const ::google::protobuf::Message &mxmsg) {
    std::string serialized;
    mxmsg.SerializeToString(&serialized);
    return new RawMessage(&serialized);
  }

  /* accessors */
  inline Usability usability() const { return usability_; }
  inline const std::string &get_message() const { return contents_; }

  /* ASIO reading buffers (for reading RawMessage from channel) */
  // returns buffer for reading-in RawMessage header
  inline boost::asio::mutable_buffer get_header_buffer() {
    Assert(usability_ == READING);
    return boost::asio::buffer((void *)(header_.size() ? &header_[0] : NULL), header_.size());
  }
  inline size_t get_header_length() const { return header_.size(); }

  // returns buffer for reading-in RawMessage body
  inline boost::asio::mutable_buffer get_body_buffer() {
    Assert(usability_ == READING);
    return boost::asio::buffer((void *)(contents_.size() ? &contents_[0] : NULL), contents_.size());
  }

  // returns RawMessage body length
  inline size_t get_body_length() const {
    Assert((contents_.empty() && !length_) || length_ == contents_.size());
    return length_;
  }

  /* ASIO writing buffers (for writing RawMessage to channel) */
  // returns buffer for writing-out whole RawMessage (header + body)
  inline const std::list<boost::asio::const_buffer> &get_message_buffer() const {
    Assert(usability_ == WRITING);
    Assert(writing_buffers_.size());
    return writing_buffers_;
  }

  /* header manipulations */
  // Decodes the header read into get_header_buffer() and sizes the body
  // buffer. False for a length of 0 or above MAX_MESSAGE_SIZE: the caller
  // must drop the connection, since the stream cannot be resynchronized.
  bool unpack_header();
  // Checks the CRC of the body read into get_body_buffer() and, on success,
  // switches the object to WRITING so it can be forwarded as is.
  bool verify();

private:
  static boost::uint32_t Crc32(const std::string &message);
  void initialize_header();
  void switch_to_writing();

private:
  Usability usability_;
  boost::uint32_t length_, crc32_;
  std::string header_;
  std::string contents_;
  std::list<boost::asio::const_buffer> writing_buffers_; // buffers that can be used in write operations
};

}; // namespace multiplexer

#endif // MX_MULTIPLEXER_IO_RAW_MESSAGE_H_
