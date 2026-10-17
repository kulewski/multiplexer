// triple<A, B, C>: std::pair with a third member. The client's incoming
// message is one: (raw frame, connection, parsed message).
#ifndef MX_LIB_TRIPLE_H_
#define MX_LIB_TRIPLE_H_

#include <type_traits>
#include <utility>

namespace mx {

template <typename First, typename Second, typename Third>
struct triple {
  typedef First first_type;
  typedef Second second_type;
  typedef Third third_type;

  // Each member taken by value and moved in: an rvalue argument, a shared
  // pointer the client hands on, costs no reference count.
  triple(first_type f, second_type s, third_type t) : first(std::move(f)), second(std::move(s)), third(std::move(t)) {}

  template <typename FirstP, typename SecondP, typename ThirdP>
  triple(const triple<FirstP, SecondP, ThirdP>& other) : first(other.first), second(other.second), third(other.third) {}

  triple() {}

  // TODO write assignments et al

  first_type first;
  second_type second;
  third_type third;
};

template <typename First, typename Second, typename Third>
triple<typename std::decay<First>::type, typename std::decay<Second>::type, typename std::decay<Third>::type>
make_triple(First&& f, Second&& s, Third&& t) {
  return triple<typename std::decay<First>::type, typename std::decay<Second>::type, typename std::decay<Third>::type>(
      std::forward<First>(f), std::forward<Second>(s), std::forward<Third>(t));
}

};  // namespace mx

#endif  // MX_LIB_TRIPLE_H_
