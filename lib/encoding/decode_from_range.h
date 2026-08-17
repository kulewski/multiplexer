// DecodeFromRange: a decoder reading bytes from an iterator range; running
// past the end is an assertion, so callers size the range first.
#ifndef MX_LIB_ENCODING_DECODE_FROM_RANGE_H_
#define MX_LIB_ENCODING_DECODE_FROM_RANGE_H_

#include "lib/assertion.h"
#include "lib/encoding/base_decoder.h"

namespace mx {
namespace encoders {

template <typename InputIterator, template <typename> class Encoding>
struct DecodeFromRange : public BaseDecoder<DecodeFromRange<InputIterator, Encoding>, Encoding> {

  DecodeFromRange(InputIterator begin, InputIterator end) : begin_(begin), end_(end) {}

  unsigned char read_byte() {
    Assert(begin_ != end_);
    return *(begin_)++;
  }

private:
  InputIterator begin_, end_;
};

}; // namespace encoders
}; // namespace mx

#endif // MX_LIB_ENCODING_DECODE_FROM_RANGE_H_
