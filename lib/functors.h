// Tiny function objects used as the pluggable pieces of
// ConnectionsManagerTraits (see multiplexer/connections_manager.h): how a
// frame becomes a queue entry, and how a queue entry yields its frame.
#ifndef MX_LIB_FUNCTORS_H_
#define MX_LIB_FUNCTORS_H_

#include <functional>

namespace mx {

template <typename What, typename From> struct ConstructingFunctor : std::function<What(From)> {
  What operator()(From &f) const { return What(f); }
  What operator()(const From &f) const { return What(f); }
};

template <typename What> struct ReferencingFunctor : std::function<What(What)> {
  What &operator()(What &w) const { return w; }
  What operator()(const What &w) const { return w; }
};

template <typename What> struct DefaultConstructingFactory {
  What operator()() const { return What(); }
};

template <typename Pair> struct FirstFromPairExtractor : std::function<typename Pair::first_type(Pair)> {
  typename Pair::first_type &operator()(Pair &p) const { return p.first; }
  const typename Pair::first_type &operator()(const Pair &p) const { return p.first; }
};

template <typename Pair> struct SecondFromPairExtractor : std::function<typename Pair::second_type(Pair)> {
  typename Pair::second_type &operator()(Pair &p) const { return p.second; }
  const typename Pair::second_type &operator()(const Pair &p) const { return p.second; }
};
}; // namespace mx

#endif // MX_LIB_FUNCTORS_H_
