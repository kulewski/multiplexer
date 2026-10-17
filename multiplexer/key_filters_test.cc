// KeyPatterns on its own: a key matches a pattern whole; exact keys are
// spotted as RE2 itself reads them, every ASCII character checked against
// it; peers with the same pattern or key share one, and it leaves with its
// last peer, a type with its last filter; an empty filter keeps its type
// filtered and takes no key; a refusal leaves the filters as they were; a
// replacement keeps what the old and the new filters share; tens of
// thousands of keys and hundreds of patterns of one type need no limit; a
// thousand long patterns of one type are matched in one pass, their set
// sized from what they compile to; and the stamps stay right as patterns,
// owners and their numbers come and go.
#include "multiplexer/key_filters.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "multiplexer/defaults.h"
#include "re2/re2.h"

namespace {

using multiplexer::KeyFilter;
using multiplexer::KeyPatterns;
using multiplexer::MAX_KEY_PATTERN_SIZE;
using Filters = google::protobuf::RepeatedPtrField<KeyFilter>;
using Owner = KeyPatterns::Owner;

const std::uint32_t TYPE = 300;
const std::uint32_t OTHER_TYPE = 301;

Filters filters(std::uint32_t type, const std::vector<std::string>& patterns) {
  Filters result;
  KeyFilter* filter = result.Add();
  filter->set_type(type);
  for (const std::string& pattern : patterns) {
    filter->add_pattern(pattern);
  }
  return result;
}

// A new owner with `peer` as its filters, set or failing the test.
Owner add(KeyPatterns& patterns, const Filters& peer) {
  const Owner owner = patterns.add_owner();
  std::string error;
  EXPECT_TRUE(patterns.set_filters(owner, peer, &error)) << error;
  return owner;
}

// Whether `owner` takes `key` of `type`, as routing asks it.
bool takes(KeyPatterns& patterns, Owner owner, std::uint32_t type, const std::string& key) {
  std::uint64_t evaluation = 0;
  if (!patterns.match(type, key, &evaluation)) {
    return true;  // nobody filters the type: every peer takes every key
  }
  return patterns.takes(owner, type, evaluation);
}

TEST(KeyPatternsTest, ATypeNobodyFiltersIsNotMatched) {
  KeyPatterns patterns;
  std::uint64_t evaluation = 7;
  EXPECT_FALSE(patterns.match(TYPE, "anything", &evaluation));
  add(patterns, filters(OTHER_TYPE, {"a"}));
  EXPECT_FALSE(patterns.match(TYPE, "a", &evaluation));
}

TEST(KeyPatternsTest, APatternMatchesTheWholeKey) {
  KeyPatterns patterns;
  const Owner acme = add(patterns, filters(TYPE, {"tenant\\.acme\\..*", "acme"}));
  EXPECT_TRUE(takes(patterns, acme, TYPE, "tenant.acme.orders"));
  EXPECT_TRUE(takes(patterns, acme, TYPE, "acme"));
  EXPECT_FALSE(takes(patterns, acme, TYPE, "acmeco"));
  EXPECT_FALSE(takes(patterns, acme, TYPE, "the acme"));
  EXPECT_FALSE(takes(patterns, acme, TYPE, "tenant.acmeco.orders"));
  EXPECT_FALSE(takes(patterns, acme, TYPE, ""));
}

TEST(KeyPatternsTest, ExactKeysAreLiteralTextAndTheRestIsLeftToRe2) {
  const std::vector<std::pair<std::string, std::string>> exact = {
      {"acme", "acme"},
      {"model\\.resnet\\-50", "model.resnet-50"},  // as Python's re.escape() writes it
      {"model.resnet-50", ""},                     // a bare dot is any character
      {"a b,c:d/e=f'g\"h<i>j@k%l&m~n#o!p;q`r", "a b,c:d/e=f'g\"h<i>j@k%l&m~n#o!p;q`r"},
      {"\\a", ""},  // a code, as are \\d, \\x41 and \\Q...\\E
      {"a\\_b", ""},
      {"a\\\\b", "a\\b"},
      {"", ""},
      {"(acme)", ""},
      {"acme|globex", ""},
      {"a\\", ""},
      {"\xc3\xa9t\xc3\xa9", ""},  // UTF-8: RE2's
  };
  for (const auto& entry : exact) {
    std::string key = "junk";
    const bool is_exact = KeyPatterns::exact(entry.first, &key);
    const bool expected = !entry.second.empty() || entry.first.empty();
    EXPECT_EQ(is_exact, expected) << entry.first;
    if (is_exact) {
      EXPECT_EQ(key, entry.second) << entry.first;
    }
  }
}

// What exact() takes for a key is what RE2 matches, and nothing else, for
// every ASCII character alone and behind a backslash, and for some
// patterns made of them.
TEST(KeyPatternsTest, ExactAgreesWithRe2) {
  std::vector<std::string> candidates;
  for (int character = 1; character < 0x80; ++character) {
    candidates.push_back(std::string(1, char(character)));
    candidates.push_back("\\" + std::string(1, char(character)));
  }
  const std::vector<std::string> pieces = {"a", "Z", "0", "-", "\\.", "\\-", " ", "\\ ", "\\\\", ":", "/", "\\*"};
  for (const std::string& first : pieces) {
    for (const std::string& second : pieces) {
      candidates.push_back("x" + first + second + "y");
    }
  }
  unsigned int exact_ones = 0;
  for (const std::string& pattern : candidates) {
    std::string key;
    if (!KeyPatterns::exact(pattern, &key)) {
      continue;
    }
    ++exact_ones;
    RE2::Options options;
    options.set_log_errors(false);
    RE2 regex(pattern, options);
    ASSERT_TRUE(regex.ok()) << "exact but does not compile: " << pattern;
    EXPECT_TRUE(RE2::FullMatch(key, regex)) << pattern << " does not match " << key;
    for (const std::string& other :
         {key + "a", key + key, std::string("q") + key, key.substr(0, key.size() - 1), std::string(key.size(), 'q')}) {
      if (other != key) {
        EXPECT_FALSE(RE2::FullMatch(other, regex)) << pattern << " matches " << other << " too";
      }
    }
  }
  EXPECT_GT(exact_ones, 150u);  // most characters are plain, escaped or not
}

TEST(KeyPatternsTest, PeersShareAPatternOrAKeyAndItLeavesWithTheLastOfThem) {
  KeyPatterns patterns;
  const Owner first = add(patterns, filters(TYPE, {"acme", "tenant\\..*"}));
  const Owner second = add(patterns, filters(TYPE, {"acme", "tenant\\..*"}));
  EXPECT_EQ(patterns.exact_keys(TYPE), 1u);
  EXPECT_EQ(patterns.patterns(TYPE), 1u);
  patterns.remove_owner(first);
  EXPECT_EQ(patterns.exact_keys(TYPE), 1u);
  EXPECT_EQ(patterns.patterns(TYPE), 1u);
  EXPECT_TRUE(takes(patterns, second, TYPE, "acme"));
  EXPECT_TRUE(takes(patterns, second, TYPE, "tenant.globex"));
  patterns.remove_owner(second);
  EXPECT_EQ(patterns.exact_keys(TYPE), 0u);
  EXPECT_EQ(patterns.patterns(TYPE), 0u);
  std::uint64_t evaluation = 0;
  EXPECT_FALSE(patterns.match(TYPE, "acme", &evaluation));  // nobody filters the type any more
}

TEST(KeyPatternsTest, EachPeerTakesWhatItsOwnPatternsMatch) {
  KeyPatterns patterns;
  const Owner acme = add(patterns, filters(TYPE, {"acme"}));
  const Owner words = add(patterns, filters(TYPE, {"[a-z]+"}));
  const Owner numbers = add(patterns, filters(TYPE, {"[0-9]+"}));
  const Owner group = add(patterns, filters(TYPE, {"(acme)"}));  // one key too, by RE2
  EXPECT_EQ(patterns.exact_keys(TYPE), 1u);
  EXPECT_EQ(patterns.patterns(TYPE), 3u);
  EXPECT_TRUE(takes(patterns, acme, TYPE, "acme"));
  EXPECT_TRUE(takes(patterns, words, TYPE, "acme"));
  EXPECT_FALSE(takes(patterns, numbers, TYPE, "acme"));
  EXPECT_TRUE(takes(patterns, group, TYPE, "acme"));
  EXPECT_FALSE(takes(patterns, acme, TYPE, "globex"));
  EXPECT_TRUE(takes(patterns, words, TYPE, "globex"));
  EXPECT_FALSE(takes(patterns, group, TYPE, "globex"));
  EXPECT_TRUE(takes(patterns, numbers, TYPE, "42"));
}

TEST(KeyPatternsTest, APeerHasAFilterPerTypeAndEachIsMatchedForItsType) {
  KeyPatterns patterns;
  Filters peer = filters(TYPE, {"acme"});
  KeyFilter* other = peer.Add();
  other->set_type(OTHER_TYPE);
  other->add_pattern("e[uw]");
  const Owner owner = add(patterns, peer);
  EXPECT_TRUE(takes(patterns, owner, TYPE, "acme"));
  EXPECT_FALSE(takes(patterns, owner, TYPE, "eu"));
  EXPECT_TRUE(takes(patterns, owner, OTHER_TYPE, "eu"));
  EXPECT_FALSE(takes(patterns, owner, OTHER_TYPE, "acme"));
  const Owner unfiltered = add(patterns, filters(OTHER_TYPE, {"x"}));  // no filter for TYPE: every key
  EXPECT_TRUE(takes(patterns, unfiltered, TYPE, "anything"));
}

TEST(KeyPatternsTest, AnEmptyFilterTakesNoKeyAndKeepsItsTypeFiltered) {
  KeyPatterns patterns;
  const Owner none = add(patterns, filters(TYPE, {}));
  EXPECT_FALSE(takes(patterns, none, TYPE, "acme"));
  const Owner acme = add(patterns, filters(TYPE, {"acme"}));
  patterns.remove_owner(acme);
  std::uint64_t evaluation = 0;
  EXPECT_TRUE(patterns.match(TYPE, "acme", &evaluation));  // the empty filter still holds the type
  EXPECT_FALSE(patterns.takes(none, TYPE, evaluation));
  patterns.remove_owner(none);
  EXPECT_FALSE(patterns.match(TYPE, "acme", &evaluation));
}

TEST(KeyPatternsTest, APatternRepeatedInOneFilterIsGivenBackWhole) {
  KeyPatterns patterns;
  const Owner owner = add(patterns, filters(TYPE, {"a-b", "a-b", "a\\-b", "a.*", "a.*"}));  // one key, twice spelled
  EXPECT_EQ(patterns.exact_keys(TYPE), 1u);
  EXPECT_EQ(patterns.patterns(TYPE), 1u);
  EXPECT_TRUE(takes(patterns, owner, TYPE, "a-b"));
  patterns.remove_owner(owner);
  EXPECT_EQ(patterns.exact_keys(TYPE), 0u);
  EXPECT_EQ(patterns.patterns(TYPE), 0u);
  std::uint64_t evaluation = 0;
  EXPECT_FALSE(patterns.match(TYPE, "a-b", &evaluation));
}

// A refusal says why and changes nothing: the owner keeps its filters.
void expect_refused(KeyPatterns& patterns, Owner owner, const Filters& peer, const std::string& why) {
  const std::size_t keys = patterns.exact_keys(TYPE);
  const std::size_t before = patterns.patterns(TYPE);
  std::string error;
  EXPECT_FALSE(patterns.set_filters(owner, peer, &error));
  EXPECT_NE(error.find(why), std::string::npos) << error;
  EXPECT_EQ(patterns.exact_keys(TYPE), keys);
  EXPECT_EQ(patterns.patterns(TYPE), before);
}

TEST(KeyPatternsTest, RefusalsSayWhyAndChangeNothing) {
  KeyPatterns patterns;
  const Owner acme = add(patterns, filters(TYPE, {"acme", "tenant\\..*"}));

  expect_refused(patterns, acme, filters(5, {"a"}), "a protocol type");
  Filters twice = filters(TYPE, {"globex"});
  *twice.Add() = twice.Get(0);
  expect_refused(patterns, acme, twice, "two key filters for message type 300");
  expect_refused(patterns, acme, filters(TYPE, {"globex", std::string(MAX_KEY_PATTERN_SIZE + 1, 'a')}),
                 "over the limit of 1024 (MAX_KEY_PATTERN_SIZE)");
  expect_refused(patterns, acme, filters(TYPE, {"globex", "tenant\\.(acme"}), "does not compile");

  EXPECT_TRUE(takes(patterns, acme, TYPE, "acme"));
  EXPECT_TRUE(takes(patterns, acme, TYPE, "tenant.initech"));
  EXPECT_FALSE(takes(patterns, acme, TYPE, "globex"));

  // A newcomer refused keeps no filters, and so would take every key.
  const Owner newcomer = patterns.add_owner();
  std::string error;
  EXPECT_FALSE(patterns.set_filters(newcomer, filters(TYPE, {"("}), &error));
  EXPECT_TRUE(takes(patterns, newcomer, TYPE, "globex"));
}

TEST(KeyPatternsTest, AReplacementKeepsWhatTheOldAndTheNewFiltersShare) {
  KeyPatterns patterns;
  const Owner owner = add(patterns, filters(TYPE, {"a.*", "b.*", "acme", "initech"}));
  std::string error;
  ASSERT_TRUE(patterns.set_filters(owner, filters(TYPE, {"b.*", "c.*", "acme", "globex"}), &error)) << error;
  EXPECT_EQ(patterns.patterns(TYPE), 2u);
  EXPECT_EQ(patterns.exact_keys(TYPE), 2u);
  EXPECT_FALSE(takes(patterns, owner, TYPE, "apple"));
  EXPECT_FALSE(takes(patterns, owner, TYPE, "initech"));
  EXPECT_TRUE(takes(patterns, owner, TYPE, "bee"));
  EXPECT_TRUE(takes(patterns, owner, TYPE, "cat"));
  EXPECT_TRUE(takes(patterns, owner, TYPE, "acme"));
  EXPECT_TRUE(takes(patterns, owner, TYPE, "globex"));
  ASSERT_TRUE(patterns.set_filters(owner, Filters(), &error)) << error;  // none: every key, of every type
  EXPECT_TRUE(takes(patterns, owner, TYPE, "apple"));
  std::uint64_t evaluation = 0;
  EXPECT_FALSE(patterns.match(TYPE, "apple", &evaluation));
}

// Far past a 64-bit mask's 64: every key and pattern is found, for each of
// its peers, and every other peer passed over.
TEST(KeyPatternsTest, ThousandsOfKeysAndHundredsOfPatternsNeedNoLimit) {
  KeyPatterns patterns;
  const unsigned int PEERS = 100, KEYS = 200, PREFIXES = 5;
  std::vector<Owner> owners;
  for (unsigned int peer = 0; peer < PEERS; ++peer) {
    std::vector<std::string> mine;
    for (unsigned int key = 0; key < KEYS; ++key) {
      mine.push_back("model\\." + std::to_string(peer) + "\\-" + std::to_string(key));
    }
    for (unsigned int prefix = 0; prefix < PREFIXES; ++prefix) {
      mine.push_back("family\\." + std::to_string(peer * PREFIXES + prefix) + "\\..*");
    }
    owners.push_back(add(patterns, filters(TYPE, mine)));
  }
  const Owner everyone = add(patterns, filters(TYPE, {"model\\..*"}));
  EXPECT_EQ(patterns.exact_keys(TYPE), PEERS * KEYS);
  EXPECT_EQ(patterns.patterns(TYPE), PEERS * PREFIXES + 1);
  for (unsigned int peer = 0; peer < PEERS; peer += 7) {
    const std::string key = "model." + std::to_string(peer) + "-" + std::to_string(peer % KEYS);
    std::uint64_t evaluation = 0;
    ASSERT_TRUE(patterns.match(TYPE, key, &evaluation));
    for (unsigned int other = 0; other < PEERS; ++other) {
      EXPECT_EQ(patterns.takes(owners[other], TYPE, evaluation), other == peer) << key << " " << other;
    }
    EXPECT_TRUE(patterns.takes(everyone, TYPE, evaluation));
    const std::string family = "family." + std::to_string(peer * PREFIXES + 3) + ".x";
    ASSERT_TRUE(patterns.match(TYPE, family, &evaluation));
    for (unsigned int other = 0; other < PEERS; ++other) {
      EXPECT_EQ(patterns.takes(owners[other], TYPE, evaluation), other == peer) << family << " " << other;
    }
    EXPECT_FALSE(patterns.takes(everyone, TYPE, evaluation));
  }
}

// A thousand long patterns of one type, each with a repetition that RE2
// unrolls, about 260 instructions apiece: their set is built, sized from
// what the patterns compile to, and a key is matched in one pass, where a
// budget of 4 KiB a pattern ran out, the set was not built, and every
// keyed message was matched against each pattern alone. Each peer's own
// key still finds that peer alone.
TEST(KeyPatternsTest, ThousandsOfLongPatternsAreMatchedInOnePass) {
  KeyPatterns patterns;
  const unsigned int PEERS = 1000;
  std::vector<Owner> owners;
  for (unsigned int peer = 0; peer < PEERS; ++peer) {
    owners.push_back(add(patterns, filters(TYPE, {"t" + std::to_string(peer) + "\\.[a-z0-9-]{1,63}"})));
  }
  for (unsigned int peer = 0; peer < PEERS; peer += 97) {
    const std::string key = "t" + std::to_string(peer) + ".tenant-" + std::to_string(peer);
    std::uint64_t evaluation = 0;
    ASSERT_TRUE(patterns.match(TYPE, key, &evaluation));
    for (unsigned int other = 0; other < PEERS; other += 1) {
      ASSERT_EQ(patterns.takes(owners[other], TYPE, evaluation), other == peer) << key << " " << other;
    }
  }
  EXPECT_TRUE(patterns.matched_in_one_pass(TYPE)) << "matched pattern by pattern";
}

TEST(KeyPatternsTest, TheStampsStayRightAsPatternsAndOwnersComeAndGo) {
  KeyPatterns patterns;
  const Owner first = add(patterns, filters(TYPE, {"a.*"}));
  const Owner second = add(patterns, filters(TYPE, {"b.*"}));
  const Owner third = add(patterns, filters(TYPE, {"c.*"}));
  EXPECT_TRUE(takes(patterns, third, TYPE, "car"));
  patterns.remove_owner(second);  // the set is built again without the middle one
  EXPECT_TRUE(takes(patterns, first, TYPE, "apple"));
  EXPECT_FALSE(takes(patterns, first, TYPE, "car"));
  EXPECT_TRUE(takes(patterns, third, TYPE, "car"));
  EXPECT_FALSE(takes(patterns, third, TYPE, "apple"));
  // The freed number and slot are handed out again, with nothing of before.
  std::uint64_t evaluation = 0;
  ASSERT_TRUE(patterns.match(TYPE, "bee", &evaluation));
  const Owner fourth = add(patterns, filters(TYPE, {"d.*"}));
  EXPECT_EQ(fourth, second);
  EXPECT_FALSE(patterns.takes(fourth, TYPE, evaluation));
  EXPECT_TRUE(takes(patterns, fourth, TYPE, "dog"));
  EXPECT_FALSE(takes(patterns, third, TYPE, "dog"));
  EXPECT_FALSE(takes(patterns, fourth, TYPE, "bee"));
}

}  // namespace
