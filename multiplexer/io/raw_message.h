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

#include <google/protobuf/message.h>

#include <asio/buffer.hpp>
#include <cstdint>
#include <list>
#include <stdexcept>
#include <string>

#include "lib/assertion.h"
#include "multiplexer/defaults.h"

namespace multiplexer {

// A body over MAX_MESSAGE_SIZE never leaves: the receiving side would close
// the connection. Raised where a frame is built, on the sender's thread, as
// std::length_error, which Python sees as ValueError.
inline void check_message_size(std::size_t size) {
  if (size > MAX_MESSAGE_SIZE) {
    throw std::length_error("a message of " + std::to_string(size) + " bytes, over the limit of " +
                            std::to_string(MAX_MESSAGE_SIZE) + " (MAX_MESSAGE_SIZE)");
  }
}

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
  static const std::uint32_t HEADER_LENGTH = 8;  // for length_ and crc32_

  // construct message suitable for reading input with ASIO
  inline RawMessage() : usability_(READING), length_(0), crc32_(0), header_(HEADER_LENGTH, 0) {
    static_assert(HEADER_LENGTH == sizeof(length_) + sizeof(crc32_), "the header is the length and the CRC");
  }

  // construct message suitable for writing output with ASIO
  inline explicit RawMessage(const std::string& message)
      : usability_(PRE_WRITING),
        length_(message.size()),
        crc32_(Crc32(message)),
        header_(HEADER_LENGTH, 0),
        contents_(message) {
    check_message_size(contents_.size());
    initialize_header();
    switch_to_writing();
  }

  // like above but destroys `contents'
  inline explicit RawMessage(std::string* message)
      : usability_(PRE_WRITING),
        length_(message->size()),
        crc32_(Crc32(*message)),
        header_(HEADER_LENGTH, 0),
        contents_() {
    contents_.swap(*message);
    check_message_size(contents_.size());
    initialize_header();
    switch_to_writing();
  }

  static RawMessage* FromMessage(const ::google::protobuf::Message& mxmsg) {
    std::string serialized;
    mxmsg.SerializeToString(&serialized);
    return new RawMessage(&serialized);
  }

  /* accessors */
  inline Usability usability() const { return usability_; }
  inline const std::string& get_message() const { return contents_; }

  // A message pinned to its connection: when that connection dies with the
  // message still unsent, the client reports it lost instead of handing it
  // to another connection (BasicClient::handle_orphaned_outgoing_messages).
  // Set by the client for a pinned lane; mutable because a queued message
  // is shared as const.
  inline void mark_pinned() const { pinned_ = true; }
  inline bool pinned() const { return pinned_; }
  // A message sent to ALL, its copies sharing this frame: a copy whose
  // connection dies goes to no other connection, which has a copy of its
  // own, and with no connection live the message waits for one once,
  // whole (BasicClient::handle_orphaned_outgoing_messages).
  inline void mark_for_all() const { for_all_ = true; }
  inline bool for_all() const { return for_all_; }
  // A frame the connection's side of the protocol makes for that
  // connection alone, its welcome, a heartbeat or the client's PEER_CONTROL:
  // when the connection dies with it unsent, it goes with the connection,
  // neither handed to another, where a second welcome makes the multiplexer
  // close that one, nor reported lost, since nobody sent it.
  inline void mark_own() const { own_ = true; }
  inline bool own() const { return own_; }
  // The message's place in the order its client sent it, which flush_all()
  // goes by (BasicClient::next_number): kept here so that a message a dead
  // connection hands over keeps its place, its queue entry carrying none.
  // The first send's: a frame sent again keeps the earlier place, which a
  // flush waits for rather than miss. 0 until it is placed.
  inline void mark_number(std::uint64_t number) const {
    if (!number_) {
      number_ = number;
    }
  }
  inline std::uint64_t number() const { return number_; }

  /* ASIO reading buffers (for reading RawMessage from channel) */
  // returns buffer for reading-in RawMessage header
  inline asio::mutable_buffer get_header_buffer() {
    Assert(usability_ == READING);
    return asio::buffer((void*)(header_.size() ? &header_[0] : NULL), header_.size());
  }
  inline size_t get_header_length() const { return header_.size(); }

  // returns buffer for reading-in RawMessage body
  inline asio::mutable_buffer get_body_buffer() {
    Assert(usability_ == READING);
    return asio::buffer((void*)(contents_.size() ? &contents_[0] : NULL), contents_.size());
  }

  // returns RawMessage body length
  inline size_t get_body_length() const {
    Assert((contents_.empty() && !length_) || length_ == contents_.size());
    return length_;
  }

  /* ASIO writing buffers (for writing RawMessage to channel) */
  // returns buffer for writing-out whole RawMessage (header + body)
  inline const std::list<asio::const_buffer>& get_message_buffer() const {
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
  static std::uint32_t Crc32(const std::string& message);
  void initialize_header();
  void switch_to_writing();

 private:
  Usability usability_;
  std::uint32_t length_, crc32_;
  std::string header_;
  std::string contents_;
  mutable bool pinned_ = false;
  mutable bool for_all_ = false;
  mutable bool own_ = false;
  mutable std::uint64_t number_ = 0;
  std::list<asio::const_buffer> writing_buffers_;  // buffers that can be used in write operations
};

};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_IO_RAW_MESSAGE_H_
