// mx::SpanSet against the plainest set that does the same, a std::set and a
// deque of the order: every answer of insert() the same over a long run of
// values that repeat within the span and beyond it, with forget_last() now
// and then; the table filling, wrapping round and shifting probe chains back
// at a small span and at the client's; and the oldest value going first.
#include "lib/spanset.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <deque>
#include <random>
#include <set>

namespace {

// What SpanSet<T, N> answers, the obvious way.
template <int N>
struct Model {
  std::set<std::uint64_t> values;
  std::deque<std::uint64_t> order;

  bool insert(std::uint64_t value) {
    if (!values.insert(value).second) {
      return false;
    }
    if (order.size() == static_cast<std::size_t>(N)) {
      values.erase(order.front());
      order.pop_front();
    }
    order.push_back(value);
    return true;
  }
  void forget_last() {
    values.erase(order.back());
    order.pop_back();
  }
};

// `inserts` values from 1 to `range`, every answer compared, forget_last()
// after one new value in `forget_every`.
template <int N>
void agrees(std::uint64_t range, int inserts, int forget_every) {
  mx::SpanSet<std::uint64_t, N> set;
  Model<N> model;
  std::mt19937_64 random(N * 7919 + range);
  for (int index = 0; index < inserts; ++index) {
    const std::uint64_t value = 1 + random() % range;
    const bool expected = model.insert(value);
    ASSERT_EQ(expected, set.insert(value)) << "insert " << index << " of " << value;
    if (expected && index % forget_every == 0) {
      model.forget_last();
      set.forget_last();
    }
  }
}

TEST(SpanSet, AnswersAsASetOfTheLastNValuesWould) {
  agrees<1>(3, 10000, 5);
  agrees<4>(10, 100000, 7);
  agrees<37>(100, 200000, 11);
  agrees<2048>(6000, 3000000, 97);  // the client's span, ids repeating within it and beyond
}

TEST(SpanSet, FreshValuesNeverRepeat) {
  // Random 64-bit ids, as messages carry, the table filling and its probe
  // chains shifting with every value the ring lets go.
  mx::SpanSet<std::uint64_t, 2048> set;
  std::mt19937_64 random(12345);
  for (int index = 0; index < 1000000; ++index) {
    ASSERT_TRUE(set.insert(random() | 1));
  }
}

TEST(SpanSet, TheOldestGoesFirst) {
  mx::SpanSet<std::uint64_t, 3> set;
  EXPECT_TRUE(set.insert(10));
  EXPECT_TRUE(set.insert(20));
  EXPECT_TRUE(set.insert(30));
  EXPECT_FALSE(set.insert(10));  // still remembered
  EXPECT_TRUE(set.insert(40));   // 10 goes
  EXPECT_TRUE(set.insert(10));   // 20 goes
  EXPECT_FALSE(set.insert(30));
  EXPECT_FALSE(set.insert(40));
  EXPECT_TRUE(set.insert(20));
  set.forget_last();  // 20 again, which pushed 30 out: 30 is not brought back
  EXPECT_TRUE(set.insert(20));
  EXPECT_TRUE(set.insert(30));
}

}  // namespace
