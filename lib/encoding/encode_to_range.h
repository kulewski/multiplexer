// EncodeToRange: an encoder writing bytes into an iterator range.
#ifndef MX_LIB_ENCODING_ENCODE_TO_RANGE_H_
#define MX_LIB_ENCODING_ENCODE_TO_RANGE_H_

#include "lib/assertion.h"
#include "lib/encoding/base_encoder.h"

namespace mx {
namespace encoders {

template <typename OutputIterator, template <typename> class Encoding>
struct EncodeToRange : public BaseEncoder<EncodeToRange<OutputIterator, Encoding>, Encoding> {

  EncodeToRange(OutputIterator begin, OutputIterator end) : begin_(begin), end_(end) {}

  void write_byte(unsigned char byte) {
    Assert(begin_ != end_);
    *(begin_)++ = byte;
  }

private:
  OutputIterator begin_, end_;
};

}; // namespace encoders
}; // namespace mx

#endif // MX_LIB_ENCODING_ENCODE_TO_RANGE_H_
