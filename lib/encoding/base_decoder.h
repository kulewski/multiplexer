// BaseDecoder: CRTP base that dispatches decoder(t) to Encoding<T>::decode.
// See little_endian.h for the one encoding in use.
#ifndef MX_LIB_ENCODING_BASE_DECODER_H_
#define MX_LIB_ENCODING_BASE_DECODER_H_

namespace mx {
namespace encoders {

template <typename Base, template <typename> class Encoding> struct BaseDecoder {
protected:
  BaseDecoder() {}

public:
  template <typename T> void operator()(T &t) { Encoding<T>::decode(*static_cast<Base *>(this), t); }
};

}; // namespace encoders

}; // namespace mx

#endif // MX_LIB_ENCODING_BASE_DECODER_H_
