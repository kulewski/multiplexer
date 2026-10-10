// KeyPatterns: the multiplexer's side of key filters (Multiplexer.proto's
// KeyFilter), the patterns its peers declare per message type for the keys
// of that type they take, and the matching of a message's key against them.
//
// A pattern that can match one key only, RE2's syntax without its
// operators (exact()), is an exact key: a hash map of the type's exact keys
// finds the peers that listed a key in one lookup, however many keys there
// are. Every other distinct pattern of a type is compiled once and shared
// by all the peers that declared it, and one RE2::Set over them, matched
// once, gives the patterns that take the key. Either way the peers found
// are stamped with the match's number, so whether a peer takes the key is
// one comparison, and what a keyed message costs follows the patterns that
// match it, never the number of peers or of patterns. The set is built
// again only after a pattern new to the type arrived or the last peer of
// one left, at the next keyed message of the type, so a burst of welcomes
// costs one build; exact keys never need one. A keyed message of a type no
// peer filters costs one hash lookup, and a message without a key nothing.
// There is no limit on patterns but the frame that carries them. Measured
// at -c opt on one core: a match costs 20 to 40 ns for exact keys, 100,000
// of them included, and 60 to 100 ns for a set of 64 to 10,000 patterns,
// a peer's comparison about 1 ns more; a set build costs 0.25 ms for 64
// patterns, 0.9 ms for 400 and 28 ms for 10,000, and compiling a pattern
// new to its type alone about 5 us, all on the multiplexer's one thread.
// RE2 stays inside key_filters.cc: only the multiplexer links it.
//
// Should the set fail to build, or run out of memory while matching, the
// patterns are matched one by one instead, which costs more and gives the
// same answer. Not thread-safe: the multiplexer's one thread.
#ifndef MX_MULTIPLEXER_KEY_FILTERS_H_
#define MX_MULTIPLEXER_KEY_FILTERS_H_

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "multiplexer/Multiplexer.pb.h" /* generated */

namespace multiplexer {

class KeyPatterns {
 public:
  // One peer's filters, by the number add_owner() hands out, which the
  // peer's connection keeps until remove_owner().
  typedef std::uint32_t Owner;
  static const Owner NO_OWNER = 0xffffffffu;

  KeyPatterns();
  ~KeyPatterns();
  KeyPatterns(const KeyPatterns&) = delete;
  KeyPatterns& operator=(const KeyPatterns&) = delete;

  // A new owner, with no filters, so that it takes every key; a number
  // given back by remove_owner() is handed out again.
  Owner add_owner();
  // Gives back the owner's filters: a pattern no peer has any more leaves
  // its type, and a type no peer filters any more leaves the index.
  void remove_owner(Owner owner);

  // Replaces `owner`'s filters with `filters`: a pattern new to its type is
  // compiled, each counts one user more, and the old ones are given back.
  // Refused, `error` saying why and nothing changed, for a filter of a
  // protocol message type (99 and below), a second filter of one type, or
  // a pattern over MAX_KEY_PATTERN_SIZE or that does not compile.
  bool set_filters(Owner owner, const google::protobuf::RepeatedPtrField<KeyFilter>& filters, std::string* error);

  // Matches `key` against the filters of `type`, stamping the owners whose
  // filter takes it with the number of this match, in `evaluation`, for
  // takes(). False when no owner filters the type, so that every peer takes
  // every key of it; the work is then one lookup. A match's stamps hold
  // until the next match, which a keyed message's routing finishes before.
  bool match(std::uint32_t type, const std::string& key, std::uint64_t* evaluation);

  // Whether `owner` takes the key `evaluation` matched for message type
  // `type`: its filter for the type took it, or it has none for the type.
  // Once per peer per keyed message, so inline.
  bool takes(Owner owner, std::uint32_t type, std::uint64_t evaluation) const {
    if (owner >= owners_.size()) {
      return true;  // NO_OWNER: no filters
    }
    const OwnerState& state = owners_[owner];
    if (state.taken == evaluation) {
      return true;
    }
    for (std::uint32_t filtered : state.types) {
      if (filtered == type) {
        return false;
      }
    }
    return true;
  }

  // `pattern` as an exact key, in `key`: true when it is literal text in
  // RE2's syntax, ASCII characters that are not operators and backslashes
  // before ASCII punctuation, so that it matches that one key whole. False
  // for anything else, which RE2 then compiles; some patterns that also
  // match one key only, `(acme)` say, take that path too, at a set's cost.
  static bool exact(const std::string& pattern, std::string* key);

  // How many distinct exact keys and how many distinct other patterns the
  // owners declared for `type`; for the tests.
  std::size_t exact_keys(std::uint32_t type) const;
  std::size_t patterns(std::uint32_t type) const;

 private:
  struct Type;      // its RE2 objects stay in key_filters.cc
  struct Compiled;  // the patterns set_filters() compiled, new to their types
  struct OwnerState {
    bool live = false;
    std::uint64_t taken = 0;           // the match that last found the owner's filter taking its key
    std::vector<std::uint32_t> types;  // the types it filters, few, so searched in order
    google::protobuf::RepeatedPtrField<KeyFilter> filters;
  };
  // Adds `owner` to what `filters` name, the patterns new to their types
  // taken from `fresh`.
  void _take(Owner owner, const google::protobuf::RepeatedPtrField<KeyFilter>& filters, Compiled& fresh);
  // Removes `owner` from what `filters` name, once for each place _take()
  // added it.
  void _give_back(Owner owner, const google::protobuf::RepeatedPtrField<KeyFilter>& filters);
  void _stamp(const std::vector<Owner>& owners, std::uint64_t evaluation);

  std::unordered_map<std::uint32_t, std::unique_ptr<Type>> types_;
  std::vector<OwnerState> owners_;
  std::vector<Owner> free_owners_;
  std::uint64_t evaluations_ = 0;  // the last match's number; 0 is none, which no stamp equals
};

};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_KEY_FILTERS_H_
