// Length-delimited protocol buffer streams over std::ostream or a file
// descriptor. Used by the logging library for its binary stream and by the
// mxcontrol log commands.
#ifndef MX_LIB_PROTOBUF_STREAM_H_
#define MX_LIB_PROTOBUF_STREAM_H_

#include "lib/fd.h"
#include "lib/logging/logging.h"
#include "lib/repr.h"
#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/common.h>
#include <memory>
#include <unistd.h>

namespace mx {
namespace protobuf {

// Writes messages as varint length followed by the serialized message. This
// is the format of the binary log stream (--logging-file) and of what
// mxcontrol streamlogs reads.
struct MessageOutputStream {
  virtual ~MessageOutputStream() {}
  virtual bool write(const google::protobuf::Message &m) = 0;
  virtual void flush() {}

  // helper
  static inline bool Write(const google::protobuf::Message &m, google::protobuf::io::ZeroCopyOutputStream &zcos) {
    google::protobuf::io::CodedOutputStream coded(&zcos);
    coded.WriteVarint64(m.ByteSizeLong());
    m.SerializeToCodedStream(&coded);
    return !coded.HadError();
  }
};

// The reader for the same format; read() returns false at end of stream or
// on a truncated message.
struct MessageInputStream {
  virtual ~MessageInputStream() {}
  virtual bool read(google::protobuf::Message &m) = 0;

  // helper
  static inline bool Read(google::protobuf::Message &m, google::protobuf::io::CodedInputStream &cis) {
    boost::uint64_t length;
    if (!cis.ReadVarint64(&length))
      return false;
    google::protobuf::io::CodedInputStream::Limit limit = cis.PushLimit(length);
    bool ok = m.ParseFromCodedStream(&cis) && cis.ConsumedEntireMessage();
    cis.PopLimit(limit);
    return ok;
  }
};

struct OstreamMessageOutputStream : MessageOutputStream {
  OstreamMessageOutputStream(std::ostream *output, bool own_ostream) : output_(output), own_ostream_(own_ostream) {}

  virtual bool write(const google::protobuf::Message &m) {
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
  std::ostream *output_;
  bool own_ostream_;
};

struct FileMessageOutputStream : MessageOutputStream, boost::noncopyable {
  explicit FileMessageOutputStream(int fd, bool own_fd = false) : fd_(fd, own_fd) {}

  virtual bool write(const google::protobuf::Message &m) {
    if (!file_output_stream_)
      file_output_stream_.reset(new google::protobuf::io ::FileOutputStream(fd_.fd()));
    return Write(m, *file_output_stream_);
  }

  virtual void flush() { file_output_stream_.reset(); }

private:
  util::Fd fd_;
  boost::scoped_ptr<google::protobuf::io::FileOutputStream> file_output_stream_;
};

struct FileMessageInputStream : MessageInputStream {
  explicit FileMessageInputStream(int fd, bool own_fd = false)
      : fd_(fd, own_fd), file_input_stream_(new google::protobuf::io ::FileInputStream(fd_.fd())),
        coded_input_stream_(new google::protobuf::io ::CodedInputStream(file_input_stream_.get())) {}

  virtual bool read(google::protobuf::Message &m) { return Read(m, *coded_input_stream_); }

private:
  util::Fd fd_;
  boost::scoped_ptr<google::protobuf::io::FileInputStream> file_input_stream_;
  boost::scoped_ptr<google::protobuf::io::CodedInputStream> coded_input_stream_;
};

}; // namespace protobuf
}; // namespace mx

#endif // MX_LIB_PROTOBUF_STREAM_H_
