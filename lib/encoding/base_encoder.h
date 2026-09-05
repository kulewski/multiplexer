// BaseEncoder: CRTP base that dispatches encoder(t) to Encoding<T>::encode.
#ifndef MX_LIB_ENCODING_BASE_ENCODER_H_
#define MX_LIB_ENCODING_BASE_ENCODER_H_

namespace mx {
namespace encoders {

template <typename Base, template <typename> class Encoding> struct BaseEncoder {
protected:
  BaseEncoder() {}

public:
  template <typename T> void operator()(const T &t) { Encoding<T>::encode(*static_cast<Base *>(this), t); }
};

}; // namespace encoders

}; // namespace mx

#endif // MX_LIB_ENCODING_BASE_ENCODER_H_
