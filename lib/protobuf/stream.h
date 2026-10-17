// Length-delimited protocol buffer streams over std::ostream or a file
// descriptor. Used by the logging library for its binary stream, by the
// mxcontrol log commands, and by the recordings, whose files
// cut_torn_tail() mends before they are appended to.
#ifndef MX_LIB_PROTOBUF_STREAM_H_
#define MX_LIB_PROTOBUF_STREAM_H_

#include <fcntl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/common.h>
#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <climits>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

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
  explicit FileMessageOutputStream(int fd, bool own_fd = false) : fd_(fd, own_fd) {
    struct stat status;
    if (::fstat(fd, &status) == 0 && S_ISREG(status.st_mode)) {
      whole_ = status.st_size;
    }
  }

  // A message is written whole by write() and the flush() after it.
  virtual bool write(const google::protobuf::Message& m) {
    if (!file_output_stream_) {
      error_ = 0;  // a new message
      file_output_stream_.reset(new google::protobuf::io ::FileOutputStream(fd_.fd()));
    }
    if (Write(m, *file_output_stream_)) {
      return true;
    }
    error_ = file_output_stream_->GetErrno();
    return false;
  }

  virtual void flush() {
    if (file_output_stream_) {
      if (!file_output_stream_->Flush()) {
        error_ = file_output_stream_->GetErrno();
      } else if (whole_ >= 0 && !error_) {
        whole_ += file_output_stream_->ByteCount();
      }
    }
    file_output_stream_.reset();
  }

  // The errno of the descriptor's failure on the last message written, 0
  // when it took it whole.
  int error() const { return error_; }

  // Cuts a regular file back to the end of its last message written whole,
  // and the descriptor's offset with it, which a descriptor opened without
  // O_APPEND writes the next message at: what a failed write left of one
  // goes, so that a reader, which stops at a message cut short, reads the
  // ones after it. False for a pipe or a socket, which keep what they
  // took, or when the cut failed. The file is this stream's alone: another
  // process appending to it would lose what it wrote since.
  bool cut_back() {
    return whole_ >= 0 && ::ftruncate(fd_.fd(), whole_) == 0 && ::lseek(fd_.fd(), whole_, SEEK_SET) == whole_;
  }

 private:
  util::Fd fd_;
  std::unique_ptr<google::protobuf::io::FileOutputStream> file_output_stream_;
  int error_ = 0;
  off_t whole_ = -1;  // a regular file's size after its last whole message; -1 for any other descriptor
};

struct FileMessageInputStream : MessageInputStream {
  // `idle`, when given, runs before a read of the descriptor that would
  // wait, nothing being ready there: what a reader that gathers messages
  // hands on then, rather than when more comes. It may run in the middle
  // of a message, which is not its to touch.
  explicit FileMessageInputStream(int fd, bool own_fd = false, std::function<void()> idle = std::function<void()>())
      : fd_(fd, own_fd), input_(fd_.fd(), std::move(idle)), adaptor_(&input_) {}

  // Each message through a CodedInputStream of its own, whose limit on the
  // bytes it reads, 2 GiB, counts from that message, so that a stream of
  // any length reads whole, where one for the whole stream stopped there as
  // if the stream ended. Its destructor gives what it read past the message
  // back to the stream, for the next.
  virtual bool read(google::protobuf::Message& m) {
    google::protobuf::io::CodedInputStream coded(&adaptor_);
    const void* data;
    int size;
    if (!coded.GetDirectBufferPointer(&data, &size)) {
      failed_ = input_.error() != 0;  // the end, or a read error
      return false;
    }
    failed_ = !Read(m, coded);
    return !failed_;
  }

  // Whether the last read() that returned false met a broken stream, a
  // message cut short or garbled, or a read error, rather than its end.
  bool failed() const { return failed_; }

 private:
  // The descriptor, read a buffer at a time for the parser, `idle` run
  // before a read that would wait; a read a signal interrupted is made
  // again, as protobuf's own file stream does.
  class Input : public google::protobuf::io::CopyingInputStream {
   public:
    Input(int fd, std::function<void()> idle) : fd_(fd), idle_(std::move(idle)) {}
    int Read(void* buffer, int size) override {
      if (idle_) {
        pollfd ready = {fd_, POLLIN, 0};
        if (::poll(&ready, 1, 0) == 0) {
          idle_();
        }
      }
      for (;;) {
        const ssize_t got = ::read(fd_, buffer, static_cast<std::size_t>(size));
        if (got >= 0) {
          return static_cast<int>(got);
        }
        if (errno != EINTR) {
          error_ = errno;
          return -1;
        }
      }
    }
    // The errno of the read that failed, 0 while none has.
    int error() const { return error_; }

   private:
    const int fd_;
    std::function<void()> idle_;
    int error_ = 0;
  };

  util::Fd fd_;
  Input input_;
  google::protobuf::io::CopyingInputStreamAdaptor adaptor_;
  bool failed_ = false;
};

// cut_torn_tail()'s answers besides the bytes it cut: the file could not
// be read or cut; or it is not a stream of the message type, or one
// damaged before its end, and is left as it is.
const std::int64_t UNREADABLE = -1;
const std::int64_t NOT_THIS_FORMAT = -2;

// Whether the bytes from `at` to `size`, the end of the file `fd`, are the
// start of a varint the end cut short: fewer than a varint's most, each one
// saying that more follow.
inline bool varint_cut_short(int fd, std::int64_t at, std::int64_t size) {
  unsigned char bytes[10];
  const std::int64_t left = size - at;
  if (left <= 0 || left >= static_cast<std::int64_t>(sizeof bytes) ||
      ::pread(fd, bytes, static_cast<std::size_t>(left), static_cast<off_t>(at)) != left) {
    return false;
  }
  for (std::int64_t index = 0; index < left; ++index) {
    if (!(bytes[index] & 0x80)) {
      return false;
    }
  }
  return true;
}

// Cuts the file at `path` after its last whole message, `scratch` a
// message of the type it holds, parsing each in turn: what a writer that
// died left of a message it was writing goes, so that what is appended to
// the file reads on. Only a torn tail is cut, a message whose length or
// whose bytes the file ends inside, after one whole message at least. A
// message whose length and bytes are all there but do not parse, or that
// `ours(scratch)` says no stream of this kind holds, a length no message
// has, or a file that ends inside its first message, is a file of another
// kind, or one damaged before its end, which only a person can mend:
// NOT_THIS_FORMAT, the file left as it is. The bytes cut, 0 for none, an
// empty file or none at all; UNREADABLE when the file could not be read or
// cut. Reads the whole file.
template <typename Message, typename Ours>
std::int64_t cut_torn_tail(const std::string& path, Message& scratch, Ours ours) {
  util::Fd fd(::open(path.c_str(), O_RDWR | O_CLOEXEC), true);
  if (fd.fd() < 0) {
    return errno == ENOENT ? 0 : UNREADABLE;
  }
  struct stat file;
  if (::fstat(fd.fd(), &file) != 0) {
    return UNREADABLE;
  }
  const std::int64_t size = static_cast<std::int64_t>(file.st_size);
  google::protobuf::io::FileInputStream input(fd.fd());
  std::int64_t whole = 0;  // where the last whole message ends
  bool torn = false;
  while (whole < size && !torn) {
    {
      // Destroyed before the count is taken, giving back what it read past the message.
      google::protobuf::io::CodedInputStream coded(&input);
      std::uint64_t length;
      if (!coded.ReadVarint64(&length)) {
        if (input.GetErrno() != 0) {
          return UNREADABLE;
        }
        if (!varint_cut_short(fd.fd(), whole, size)) {
          return NOT_THIS_FORMAT;  // no varint
        }
        torn = true;
      } else if (length > static_cast<std::uint64_t>(INT_MAX)) {
        return NOT_THIS_FORMAT;  // a length no message of this format has
      } else if (whole + coded.CurrentPosition() + static_cast<std::int64_t>(length) > size) {
        torn = true;  // the file ends inside the message
      } else {
        google::protobuf::io::CodedInputStream::Limit limit = coded.PushLimit(static_cast<int>(length));
        const bool parsed =
            scratch.ParseFromCodedStream(&coded) && coded.ConsumedEntireMessage() && coded.BytesUntilLimit() == 0;
        coded.PopLimit(limit);
        if (!parsed && input.GetErrno() != 0) {
          return UNREADABLE;
        }
        if (!parsed || !ours(scratch)) {
          return NOT_THIS_FORMAT;  // whole, and not a message of this stream
        }
      }
    }
    if (torn && whole == 0) {
      return NOT_THIS_FORMAT;  // not one whole message: nothing says the file is a stream of this kind
    }
    if (!torn) {
      whole = input.ByteCount();
    }
  }
  if (torn && ::ftruncate(fd.fd(), static_cast<off_t>(whole)) != 0) {
    return UNREADABLE;
  }
  return torn ? size - whole : 0;
}

};  // namespace protobuf
};  // namespace mx

#endif  // MX_LIB_PROTOBUF_STREAM_H_
