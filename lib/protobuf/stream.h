// Length-delimited protocol buffer streams over std::ostream or a file
// descriptor. Used by the logging library for its binary stream and by the
// mxcontrol log commands.
#ifndef MX_LIB_PROTOBUF_STREAM_H_
#define MX_LIB_PROTOBUF_STREAM_H_

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/common.h>
#include <unistd.h>

#include <climits>
#include <cstdint>
#include <memory>

#include "lib/fd.h"
#include "lib/logging/logging.h"
#include "lib/repr.h"

namespace mx {
namespace protobuf {

// Writes messages as varint length followed by the serialized message. This
// is the format of the binary log stream (--logging-file) and of what
// mxcontrol streamlogs reads.
struct MessageOutputStream {
  virtual ~MessageOutputStream() {}
  virtual bool write(const google::protobuf::Message& m) = 0;
  virtual void flush() {}

  // helper
  static inline bool Write(const google::protobuf::Message& m, google::protobuf::io::ZeroCopyOutputStream& zcos) {
    google::protobuf::io::CodedOutputStream coded(&zcos);
    coded.WriteVarint64(m.ByteSizeLong());
    m.SerializeToCodedStream(&coded);
    return !coded.HadError();
  }
};

// The reader for the same format; read() returns false at end of stream or
// on a message it cannot read whole, cut short or garbled.
struct MessageInputStream {
  virtual ~MessageInputStream() {}
  virtual bool read(google::protobuf::Message& m) = 0;

  // helper
  static inline bool Read(google::protobuf::Message& m, google::protobuf::io::CodedInputStream& cis) {
    std::uint64_t length;
    if (!cis.ReadVarint64(&length) || length > static_cast<std::uint64_t>(INT_MAX)) {
      return false;  // no length, or one no message of this format has
    }
    google::protobuf::io::CodedInputStream::Limit limit = cis.PushLimit(static_cast<int>(length));
    // Whole: a message that ends with the stream before its length, at a
    // field's end, parses without complaint.
    bool ok = m.ParseFromCodedStream(&cis) && cis.ConsumedEntireMessage() && cis.BytesUntilLimit() == 0;
    cis.PopLimit(limit);
    return ok;
  }
};

struct OstreamMessageOutputStream : MessageOutputStream {
  OstreamMessageOutputStream(std::ostream* output, bool own_ostream) : output_(output), own_ostream_(own_ostream) {}

  virtual bool write(const google::protobuf::Message& m) {
    google::protobuf::io::OstreamOutputStream oos(output_);
    return Write(m, oos);
  }

  virtual void flush() { output_->flush(); }

  ~OstreamMessageOutputStream() {
    if (own_ostream_) {
      delete output_;
    }
  }

 private:
  std::ostream* output_;
  bool own_ostream_;
};

struct FileMessageOutputStream : MessageOutputStream {
  FileMessageOutputStream(const FileMessageOutputStream&) = delete;
  FileMessageOutputStream& operator=(const FileMessageOutputStream&) = delete;
  explicit FileMessageOutputStream(int fd, bool own_fd = false) : fd_(fd, own_fd) {}

  virtual bool write(const google::protobuf::Message& m) {
    if (!file_output_stream_) {
      file_output_stream_.reset(new google::protobuf::io ::FileOutputStream(fd_.fd()));
    }
    if (Write(m, *file_output_stream_)) {
      return true;
    }
    error_ = file_output_stream_->GetErrno();
    return false;
  }

  virtual void flush() {
    if (file_output_stream_ && !file_output_stream_->Flush()) {
      error_ = file_output_stream_->GetErrno();
    }
    file_output_stream_.reset();
  }

  // The errno of the last write to the descriptor that failed, 0 while
  // none has.
  int error() const { return error_; }

 private:
  util::Fd fd_;
  std::unique_ptr<google::protobuf::io::FileOutputStream> file_output_stream_;
  int error_ = 0;
};

struct FileMessageInputStream : MessageInputStream {
  explicit FileMessageInputStream(int fd, bool own_fd = false)
      : fd_(fd, own_fd), file_input_stream_(new google::protobuf::io ::FileInputStream(fd_.fd())) {}

  // Each message through a CodedInputStream of its own, whose limit on the
  // bytes it reads, 2 GiB, counts from that message, so that a stream of
  // any length reads whole, where one for the whole stream stopped there as
  // if the stream ended. Its destructor gives what it read past the message
  // back to the file stream, for the next.
  virtual bool read(google::protobuf::Message& m) {
    google::protobuf::io::CodedInputStream coded(file_input_stream_.get());
    const void* data;
    int size;
    if (!coded.GetDirectBufferPointer(&data, &size)) {
      failed_ = file_input_stream_->GetErrno() != 0;  // the end, or a read error
      return false;
    }
    failed_ = !Read(m, coded);
    return !failed_;
  }

  // Whether the last read() that returned false met a broken stream, a
  // message cut short or garbled, or a read error, rather than its end.
  bool failed() const { return failed_; }

 private:
  util::Fd fd_;
  std::unique_ptr<google::protobuf::io::FileInputStream> file_input_stream_;
  bool failed_ = false;
};

};  // namespace protobuf
};  // namespace mx

#endif  // MX_LIB_PROTOBUF_STREAM_H_
