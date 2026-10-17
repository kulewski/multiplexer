// One frame on the wire: a header of two little-endian uint32 (body length,
// the low 32 bits of the body's XXH3-64, lib/xxh3.h) followed by the body,
// a serialized MultiplexerMessage. docs/wire_format.md is the reference.
//
// The same object is used for reading (buffers for Asio to fill, or a
// header and a body copied from a connection's read buffer, then
// unpack_header() and verify()) and for writing (a fixed header plus body,
// exposed as a buffer list). A frame received by the multiplexer is
// forwarded as the same RawMessage, so the bytes forwarded are those that
// came, never re-serialized; parsing the body to route it copies the
// payload once.
//
// A client that batches makes the frame of a message that may go in a
// batch without its checksum (Unsealed): in a batch only its body is read,
// and its own checksum would be computed for nothing, a pass over every
// byte. One that goes alone after all is sealed, its checksum and header
// made in place, on the thread that sends it, before any connection has it
// (seal(), in BasicClient::_send and outbox.cc's writable()).
#ifndef MX_MULTIPLEXER_IO_RAW_MESSAGE_H_
#define MX_MULTIPLEXER_IO_RAW_MESSAGE_H_

#include <array>
#include <asio/buffer.hpp>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "lib/assertion.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
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

// A key over MAX_KEY_SIZE never leaves either: the multiplexer would drop
// the message. Raised where a frame is built and, for a threaded query,
// whose frames the io thread builds, at the call.
inline void check_key_size(std::size_t size) {
  if (size > MAX_KEY_SIZE) {
    throw std::length_error("a key of " + std::to_string(size) + " bytes, over the limit of " +
                            std::to_string(MAX_KEY_SIZE) + " (MAX_KEY_SIZE)");
  }
}

/**
 * RawMessage is a structured representation of wire format [length, checksum, body].
 * It has well defined life cycles:
 *	    create empty -> read -> freeze -> use for writing
 *	    create frozen with message ->  use for writing
 * so that we don't have to serialize back the message after it's deserialized
 * -- save on Protocol Buffers serialization -- prior to forwarding it to other
 *peers.
 */
class RawMessage {
 public:
  enum Usability { READING, WRITING, NONE, PRE_WRITING, UNSEALED };
  static const std::uint32_t HEADER_LENGTH = 8;  // for length_ and checksum_

  // construct message suitable for reading input with ASIO
  inline RawMessage() : usability_(READING), length_(0), checksum_(0), header_(HEADER_LENGTH, 0) {
    static_assert(HEADER_LENGTH == sizeof(length_) + sizeof(checksum_), "the header is the length and the checksum");
  }

  // construct message suitable for writing output with ASIO: `message`, a
  // serialized MultiplexerMessage, with the id and the type it carries
  inline explicit RawMessage(const std::string& message, std::uint64_t id, std::uint32_t type)
      : usability_(PRE_WRITING),
        length_(message.size()),
        checksum_(Checksum(message)),
        header_(HEADER_LENGTH, 0),
        contents_(message),
        id_(id),
        type_(type) {
    check_message_size(contents_.size());
    initialize_header();
    switch_to_writing();
  }

  // like above but destroys `contents'
  inline explicit RawMessage(std::string* message, std::uint64_t id, std::uint32_t type)
      : usability_(PRE_WRITING),
        length_(message->size()),
        checksum_(Checksum(*message)),
        header_(HEADER_LENGTH, 0),
        contents_(),
        id_(id),
        type_(type) {
    contents_.swap(*message);
    check_message_size(contents_.size());
    initialize_header();
    switch_to_writing();
  }

  // `message`, a serialized MultiplexerMessage, swapped out, with the id
  // and the type it carries, its checksum and header not made yet: the frame of
  // a message that may go in a batch (see the file comment), sealed by
  // seal() if it goes alone. Its size is checked here, as any frame's.
  struct Unsealed {};
  inline RawMessage(Unsealed, std::string* message, std::uint64_t id, std::uint32_t type)
      : usability_(UNSEALED),
        length_(message->size()),
        checksum_(0),
        header_(HEADER_LENGTH, 0),
        contents_(),
        id_(id),
        type_(type) {
    contents_.swap(*message);
    check_message_size(contents_.size());
  }

  // `mxmsg`, a MultiplexerMessage or a client's BatchEnvelope, serialized
  // into a frame, with its id and type, unsealed when `sealed` is false
  // (Unsealed). One protobuf will not serialize, over 2 GiB, raises
  // std::length_error as one over MAX_MESSAGE_SIZE does, its size measured:
  // protobuf leaves the string empty, which went out as a frame with no
  // body, and the multiplexer closed the connection. A key over
  // MAX_KEY_SIZE raises std::length_error too: the multiplexer would drop
  // the message.
  template <typename Message>
  static RawMessage* FromMessage(const Message& mxmsg, bool sealed = true) {
    check_key_size(mxmsg.key().size());
    std::string serialized;
    if (!mxmsg.SerializeToString(&serialized)) {
      check_message_size(mxmsg.ByteSizeLong());
      throw std::length_error("a message protobuf could not serialize, of " + std::to_string(mxmsg.ByteSizeLong()) +
                              " bytes");
    }
    if (!sealed) {
      return new RawMessage(Unsealed(), &serialized, mxmsg.id(), mxmsg.type());
    }
    return new RawMessage(&serialized, mxmsg.id(), mxmsg.type());
  }

  // An unsealed frame's checksum and header made, in place, and the frame
  // switched to writing; nothing for any other. Called on the thread that
  // sends the frame, before any connection has it: the client's own
  // thread, its io thread for ThreadedClient, to which the caller's thread
  // handed the frame through a post, so nothing else reads it meanwhile.
  // Const, as a queued frame is shared as const; what it fills is mutable.
  void seal() const;

  // A message taken out of a batch that arrived, serialized again for a
  // reader of raw frames (BasicClient::with_raw_frame), or a DELIVERY_ERROR
  // made for a message inside one, its serialized form swapped out of
  // `message`, with its id and type: for the program to read as it reads a
  // frame's (get_message()), unframed, which a client sending it on frames
  // then (outbox.cc's writable()).
  static std::shared_ptr<const RawMessage> FromBatch(std::string* message, std::uint64_t id, std::uint32_t type) {
    return std::make_shared<RawMessage>(Unframed(), message, id, type);  // one allocation
  }

  // The body of a frame that goes in a batch, which only its body is read
  // of (BasicClient::_send_batch): moved out when `raw` is the frame's only
  // holder, as a frame made for a send is by then, so that a message's
  // bytes are not copied on their way into the batch, else copied.
  // GiveBodyBack() returns it to a frame left empty so, for a message cut
  // from the batch, or one whose batch could not be made, to go alone or
  // in the next. A frame is never made const (writable()'s copies, made
  // const, never join a batch), so the body may be moved out of one held
  // as const.
  static std::string TakeBody(const std::shared_ptr<const RawMessage>& raw) {
    if (raw.use_count() == 1) {
      return std::move(const_cast<RawMessage&>(*raw).contents_);
    }
    return raw->contents_;
  }
  // The body of a frame its last holder let go of, moved out just before
  // the frame is deleted, for its maker to reuse (BasicClient's batch
  // frames, which a pool of buffers serves).
  static std::string ReleaseBody(const RawMessage* frame) {
    return std::move(const_cast<RawMessage*>(frame)->contents_);
  }
  static void GiveBodyBack(const std::shared_ptr<const RawMessage>& raw, std::string* body) {
    std::string& contents = const_cast<RawMessage&>(*raw).contents_;
    if (contents.empty()) {
      contents.swap(*body);
    }
  }

  // What a client's delivery (BasicClient::set_delivery) needs of the
  // message in a frame: whether it may go by one, a plain event
  // (plain_event()), and its key, which its batch goes by, or none, which
  // routes otherwise than an empty one. A client with a delivery set marks
  // the frames it makes of whole messages (frame_stamped), which no other
  // client pays for; any other frame knows none, UNKNOWN, until a client
  // with a delivery set for its type reads them from the bytes. Kept apart (BatchMarks),
  // made for a marked frame only, so that every other frame, every one a
  // multiplexer routes, carries a null pointer for them. Mutable because a
  // queued frame is shared as const.
  enum class Batchable : unsigned char { UNKNOWN, NO, YES };
  inline Batchable batchable() const { return batch_marks_ ? batch_marks_->batchable : Batchable::UNKNOWN; }
  inline bool batch_keyed() const { return batch_marks_ && batch_marks_->keyed; }
  inline const std::string& batch_key() const {
    static const std::string none;
    return batch_marks_ ? batch_marks_->key : none;
  }
  inline void mark_batchable(Batchable batchable, bool keyed, std::string key) const {
    BatchMarks& marks = _batch_marks();
    marks.batchable = batchable;
    marks.keyed = keyed;
    marks.key = std::move(key);
  }
  // Whether `mxmsg` may go in a batch: an event the program sends that
  // nobody waits for, with an id, a sender, a type, a payload and at most a
  // key, a workflow and a timestamp, nothing that routes it otherwise or
  // asks the multiplexer for a report, and no batch itself.
  static bool plain_event(const MultiplexerMessage& mxmsg) {
    return !mxmsg.to() && !mxmsg.references() && !mxmsg.report_delivery_error() &&
           !mxmsg.include_original_packet_in_report() && mxmsg.override_rrules_size() == 0 &&
           mxmsg.logging_method() == LoggingMethod::BOTH && mxmsg.compression() == Compression::NO_COMPRESSION &&
           !mxmsg.has_batch();
  }
  // A batch's frame: the ids of the messages inside, in order, by which a
  // drop of the frame is told, one each (BasicClient::report_drop). Null
  // for any other frame.
  inline void mark_batch(std::shared_ptr<const std::vector<std::uint64_t>> ids) const {
    _batch_marks().ids = std::move(ids);
  }
  inline const std::shared_ptr<const std::vector<std::uint64_t>>& batch_ids() const {
    static const std::shared_ptr<const std::vector<std::uint64_t>> none;
    return batch_marks_ ? batch_marks_->ids : none;
  }
  // A confirmed batch's frame (BasicClient::Delivery::confirm): its drop is
  // the confirmed sends' to tell, for the messages not resolved yet.
  inline void mark_confirmed() const { _batch_marks().confirmed = true; }
  inline bool confirmed() const { return batch_marks_ && batch_marks_->confirmed; }

  /* accessors */
  inline Usability usability() const { return usability_; }
  inline const std::string& get_message() const { return contents_; }
  // The id and the type of the message in the frame, as its maker read
  // them from the message: what a drop is reported by
  // (BasicClient::report_drop), so that nothing parses the frame for them.
  // 0 in a frame read from a connection, which nothing reports dropped.
  inline std::uint64_t id() const { return id_; }
  inline std::uint32_t type() const { return type_; }

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
  // How long the message may wait for room or for a connection: the
  // deadline its first send gave it, kept here, as its place is, so that a
  // message a dead connection hands over keeps it, its queue entry
  // carrying none. None until it is placed.
  inline void mark_deadline(std::chrono::steady_clock::time_point deadline) const {
    if (!deadline_marked_) {
      deadline_ = deadline;
      deadline_marked_ = true;
    }
  }
  inline std::chrono::steady_clock::time_point deadline() const { return deadline_; }

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
  inline const std::array<asio::const_buffer, 2>& get_message_buffer() const {
    Assert(usability_ == WRITING);
    return writing_buffers_;
  }

  /* header manipulations */
  // Decodes the header read into get_header_buffer() and sizes the body
  // buffer. False for a length of 0 or above MAX_MESSAGE_SIZE: the caller
  // must drop the connection, since the stream cannot be resynchronized.
  bool unpack_header();
  // The same for the header that is the HEADER_LENGTH bytes at `header`,
  // in a connection's read buffer.
  bool unpack_header(const char* header);
  // The body length the header at `header` gives, before a frame is made
  // for it: 0 for one unpack_header() refuses.
  static std::uint32_t body_length(const char* header);
  // Checks the body read into get_body_buffer() against the header's
  // checksum and, on success, switches the object to WRITING so it can be
  // forwarded as is.
  bool verify();

  // FromBatch's: the body alone, unframed; public for make_shared, made
  // through FromBatch().
  struct Unframed {};
  RawMessage(Unframed, std::string* message, std::uint64_t id, std::uint32_t type)
      : usability_(NONE), length_(message->size()), checksum_(0), header_(HEADER_LENGTH, 0), id_(id), type_(type) {
    contents_.swap(*message);
  }

 private:
  // Batching's marks, apart; see batchable() and batch_ids().
  struct BatchMarks {
    Batchable batchable = Batchable::UNKNOWN;
    bool keyed = false;
    bool confirmed = false;
    std::string key;
    std::shared_ptr<const std::vector<std::uint64_t>> ids;
  };
  BatchMarks& _batch_marks() const {
    if (!batch_marks_) {
      batch_marks_.reset(new BatchMarks());
    }
    return *batch_marks_;
  }
  static std::uint32_t Checksum(const std::string& message);
  void initialize_header() const;
  void switch_to_writing() const;

 private:
  // Mutable for seal(), which fills them once for a frame made unsealed;
  // nothing else changes them through a const frame.
  mutable Usability usability_;
  std::uint32_t length_;
  mutable std::uint32_t checksum_;
  mutable std::string header_;
  std::string contents_;
  std::uint64_t id_ = 0;    // see id()
  std::uint32_t type_ = 0;  // see type()
  mutable bool pinned_ = false;
  mutable bool for_all_ = false;
  mutable bool own_ = false;
  mutable std::uint64_t number_ = 0;
  mutable std::chrono::steady_clock::time_point deadline_ = std::chrono::steady_clock::time_point::max();
  mutable bool deadline_marked_ = false;
  mutable std::unique_ptr<BatchMarks> batch_marks_;  // see batchable() and batch_ids()
  // The header's buffer and the body's, which writing a frame gathers; two
  // in place, where a list allocated a node for each, in every frame read
  // and again in every write that copied the list.
  mutable std::array<asio::const_buffer, 2> writing_buffers_;
};

};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_IO_RAW_MESSAGE_H_
