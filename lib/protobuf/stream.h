// Length-delimited protocol buffer streams over std::ostream or a file
// descriptor. Used by the logging library for its binary stream and by the
// mxcontrol log commands.
#ifndef MX_LIB_PROTOBUF_STREAM_H_
#define MX_LIB_PROTOBUF_STREAM_H_

#include <fcntl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/common.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <climits>
#include <cstdint>
#include <memory>
#include <string>

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
    const bool written = Write(m, oos);
    bytes_ += static_cast<std::uint64_t>(oos.ByteCount());  // the length and the message, as protobuf wrote them
    return written;
  }

  virtual void flush() { output_->flush(); }

  // How many bytes write() has handed the ostream, buffered ones included,
  // as protobuf's output stream counted them.
  std::uint64_t bytes() const { return bytes_; }

  ~OstreamMessageOutputStream() {
    if (own_ostream_) {
      delete output_;
    }
  }

 private:
  std::ostream* output_;
  bool own_ostream_;
  std::uint64_t bytes_ = 0;
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

// Cuts the file at `path` after its last whole message, `scratch` a
// message of the type it holds, parsing each in turn: what a writer that
// died left of a message it was writing goes, so that what is appended to
// the file reads on. The bytes cut, 0 for none or for a file that does not
// exist, -1 when the file could not be read or cut. Reads the whole file.
inline std::int64_t cut_torn_tail(const std::string& path, google::protobuf::Message& scratch) {
  util::Fd fd(::open(path.c_str(), O_RDWR | O_CLOEXEC), true);
  if (fd.fd() < 0) {
    return errno == ENOENT ? 0 : -1;
  }
  struct stat file;
  if (::fstat(fd.fd(), &file) != 0) {
    return -1;
  }
  google::protobuf::io::FileInputStream input(fd.fd());
  std::int64_t whole = 0;  // where the last whole message ends
  for (;;) {
    {
      // Destroyed before the count is taken, giving back what it read past the message.
      google::protobuf::io::CodedInputStream coded(&input);
      const void* data;
      int size;
      if (!coded.GetDirectBufferPointer(&data, &size)) {
        if (input.GetErrno() != 0) {
          return -1;
        }
        break;  // the end, after a whole message
      }
      if (!MessageInputStream::Read(scratch, coded)) {
        break;  // a message cut short or garbled: the file is cut where it starts
      }
    }
    whole = input.ByteCount();
  }
  const std::int64_t torn = static_cast<std::int64_t>(file.st_size) - whole;
  if (torn > 0 && ::ftruncate(fd.fd(), static_cast<off_t>(whole)) != 0) {
    return -1;
  }
  return torn > 0 ? torn : 0;
}

};  // namespace protobuf
};  // namespace mx

#endif  // MX_LIB_PROTOBUF_STREAM_H_
