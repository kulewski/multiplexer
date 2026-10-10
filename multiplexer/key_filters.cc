// KeyPatterns: exact keys in a hash map, the other patterns of a message
// type compiled once each and matched together by one RE2::Set, and the
// owners a match finds stamped with its number. See key_filters.h.
#include "multiplexer/key_filters.h"

#include <algorithm>
#include <cstring>
#include <set>

#include "multiplexer/defaults.h"
#include "multiplexer/multiplexer.constants.h" /* generated */
// Quoted, as RE2 includes its own: a workspace that builds @mx without
// external_include_paths has RE2's root on the -iquote path only.
#include "re2/re2.h"
#include "re2/set.h"

namespace multiplexer {

namespace {

// Whole-key matches, by RE2's syntax, which reports its own errors through
// set_filters()' `error` rather than to stderr.
RE2::Options pattern_options() {
  RE2::Options options;
  options.set_log_errors(false);
  return options;
}

// A set of a few thousand patterns fits RE2's default memory budget, 8
// MiB; past that the budget grows with the patterns. It is a cap: RE2
// takes only what the patterns need.
RE2::Options set_options(std::size_t patterns) {
  RE2::Options options = pattern_options();
  options.set_max_mem(std::max<std::int64_t>(options.max_mem(), std::int64_t(4096) * std::int64_t(patterns)));
  return options;
}

bool ascii_word_character(unsigned char character) {
  return ('0' <= character && character <= '9') || ('a' <= character && character <= 'z') ||
         ('A' <= character && character <= 'Z') || character == '_';
}

// Removes one `owner` from `owners`, whose order does not matter: false
// when it was not there.
bool remove_one(std::vector<KeyPatterns::Owner>& owners, KeyPatterns::Owner owner) {
  for (std::size_t index = 0; index < owners.size(); ++index) {
    if (owners[index] == owner) {
      owners[index] = owners.back();
      owners.pop_back();
      return true;
    }
  }
  return false;
}

}  // namespace

struct KeyPatterns::Compiled {
  std::unordered_map<std::uint32_t, std::unordered_map<std::string, std::unique_ptr<RE2>>> by_type;
};

// One message type's filters. An owner is listed once for each time one of
// its filters names the key or the pattern, so that giving a filter back
// removes exactly what taking it added. A slot is a pattern's place in the
// set; a free one has no regex.
struct KeyPatterns::Type {
  struct Pattern {
    std::string text;
    // Compiled alone: set_filters()' check, and the matching when the set
    // cannot be used.
    std::unique_ptr<RE2> regex;
    std::vector<Owner> holders;
  };
  std::unordered_map<std::string, std::vector<Owner>> exact;
  std::vector<Pattern> slots;
  std::vector<unsigned int> free_slots;
  std::unordered_map<std::string, unsigned int> slot_by_text;
  // Every used slot's pattern, in slot order: the set's index i is slot
  // slot_by_index[i]. Null while stale, with no pattern, and when it could
  // not be built.
  std::unique_ptr<RE2::Set> set;
  std::vector<unsigned int> slot_by_index;
  bool stale = true;
  std::vector<int> hits;  // RE2::Set::Match's answer, kept to reuse its memory
  // The owners with a filter for the type, an empty one among them: a
  // filter with no pattern takes no key, and keeps the type filtered.
  unsigned int owners = 0;

  // Builds the set again from the used slots; on failure the slots are
  // matched one by one until the next build.
  void build() {
    stale = false;
    slot_by_index.clear();
    set.reset();
    if (slot_by_text.empty()) {
      return;
    }
    set.reset(new RE2::Set(set_options(slot_by_text.size()), RE2::ANCHOR_BOTH));
    for (unsigned int slot = 0; slot < slots.size(); ++slot) {
      if (slots[slot].regex) {
        if (set->Add(slots[slot].text, nullptr) < 0) {
          set.reset();
          return;
        }
        slot_by_index.push_back(slot);
      }
    }
    if (!set->Compile()) {
      set.reset();
    }
  }
};

KeyPatterns::KeyPatterns() = default;
KeyPatterns::~KeyPatterns() = default;

KeyPatterns::Owner KeyPatterns::add_owner() {
  Owner owner;
  if (free_owners_.empty()) {
    owner = Owner(owners_.size());
    owners_.emplace_back();
  } else {
    owner = free_owners_.back();
    free_owners_.pop_back();
  }
  owners_[owner].live = true;
  return owner;
}

void KeyPatterns::remove_owner(Owner owner) {
  if (owner >= owners_.size() || !owners_[owner].live) {
    return;
  }
  _give_back(owner, owners_[owner].filters);
  owners_[owner] = OwnerState();  // no stamp a later match could equal
  free_owners_.push_back(owner);
}

bool KeyPatterns::exact(const std::string& pattern, std::string* key) {
  key->clear();
  for (std::size_t index = 0; index < pattern.size(); ++index) {
    const unsigned char character = pattern[index];
    if (character == 0 || character >= 0x80) {
      return false;  // RE2's to read: it takes UTF-8 and refuses what is not
    }
    if (character == '\\') {
      if (++index == pattern.size()) {
        return false;  // a trailing backslash, which does not compile
      }
      // Before ASCII punctuation a backslash makes it plain; before a
      // letter, a digit or '_' it is a class, an anchor, a code or an error.
      const unsigned char escaped = pattern[index];
      if (escaped == 0 || escaped >= 0x80 || ascii_word_character(escaped)) {
        return false;
      }
      key->push_back(char(escaped));
    } else if (std::strchr(".+*?()|[]{}^$", character)) {
      return false;
    } else {
      key->push_back(char(character));
    }
  }
  return true;
}

bool KeyPatterns::set_filters(Owner owner, const google::protobuf::RepeatedPtrField<KeyFilter>& filters,
                              std::string* error) {
  // Everything is checked before anything changes, so that a refusal
  // leaves the index as it was; the patterns new to their type are
  // compiled once, here, and kept for the taking.
  std::set<std::uint32_t> seen;
  Compiled fresh;
  std::string key;
  for (const KeyFilter& filter : filters) {
    const std::string type = std::to_string(filter.type());
    if (filter.type() <= types::MAX_MULTIPLEXER_META_PACKET) {
      *error = "a key filter for message type " + type + ", a protocol type";
      return false;
    }
    if (!seen.insert(filter.type()).second) {
      *error = "two key filters for message type " + type;
      return false;
    }
    std::unordered_map<std::uint32_t, std::unique_ptr<Type>>::const_iterator known = types_.find(filter.type());
    std::unordered_map<std::string, std::unique_ptr<RE2>>& compiled = fresh.by_type[filter.type()];
    for (const std::string& pattern : filter.pattern()) {
      if (pattern.size() > MAX_KEY_PATTERN_SIZE) {
        *error = "a key pattern of " + std::to_string(pattern.size()) + " bytes for message type " + type +
                 ", over the limit of " + std::to_string(MAX_KEY_PATTERN_SIZE) + " (MAX_KEY_PATTERN_SIZE)";
        return false;
      }
      if (exact(pattern, &key) || (known != types_.end() && known->second->slot_by_text.count(pattern)) ||
          compiled.count(pattern)) {
        continue;
      }
      std::unique_ptr<RE2> regex(new RE2(pattern, pattern_options()));
      if (!regex->ok()) {
        *error =
            "the key pattern \"" + pattern + "\" for message type " + type + " does not compile: " + regex->error();
        return false;
      }
      compiled.emplace(pattern, std::move(regex));
    }
  }

  // The new filters are taken before the old ones are given back, so that
  // a pattern in both keeps its slot and its compiled form.
  google::protobuf::RepeatedPtrField<KeyFilter> old;
  old.Swap(&owners_[owner].filters);
  _take(owner, filters, fresh);
  _give_back(owner, old);
  OwnerState& state = owners_[owner];
  state.filters = filters;
  state.types.clear();
  for (const KeyFilter& filter : filters) {
    state.types.push_back(filter.type());
  }
  return true;
}

void KeyPatterns::_take(Owner owner, const google::protobuf::RepeatedPtrField<KeyFilter>& filters, Compiled& fresh) {
  std::string key;
  for (const KeyFilter& filter : filters) {
    std::unique_ptr<Type>& entry = types_[filter.type()];
    if (!entry) {
      entry.reset(new Type());
    }
    Type& type = *entry;
    ++type.owners;
    for (const std::string& pattern : filter.pattern()) {
      if (exact(pattern, &key)) {
        type.exact[key].push_back(owner);
        continue;
      }
      std::unordered_map<std::string, unsigned int>::const_iterator found = type.slot_by_text.find(pattern);
      unsigned int slot;
      if (found != type.slot_by_text.end()) {
        slot = found->second;
      } else {
        if (type.free_slots.empty()) {
          slot = (unsigned int)type.slots.size();
          type.slots.emplace_back();
        } else {
          slot = type.free_slots.back();
          type.free_slots.pop_back();
        }
        type.slots[slot].text = pattern;
        type.slots[slot].regex = std::move(fresh.by_type[filter.type()][pattern]);  // checked: compiled
        type.slot_by_text.emplace(pattern, slot);
        type.stale = true;
      }
      type.slots[slot].holders.push_back(owner);
    }
  }
}

void KeyPatterns::_give_back(Owner owner, const google::protobuf::RepeatedPtrField<KeyFilter>& filters) {
  std::string key;
  for (const KeyFilter& filter : filters) {
    std::unordered_map<std::uint32_t, std::unique_ptr<Type>>::iterator found_type = types_.find(filter.type());
    if (found_type == types_.end()) {
      continue;  // never taken
    }
    Type& type = *found_type->second;
    for (const std::string& pattern : filter.pattern()) {
      if (exact(pattern, &key)) {
        std::unordered_map<std::string, std::vector<Owner>>::iterator found = type.exact.find(key);
        if (found != type.exact.end() && remove_one(found->second, owner) && found->second.empty()) {
          type.exact.erase(found);
        }
        continue;
      }
      std::unordered_map<std::string, unsigned int>::iterator found = type.slot_by_text.find(pattern);
      if (found == type.slot_by_text.end()) {
        continue;
      }
      Type::Pattern& slot = type.slots[found->second];
      if (remove_one(slot.holders, owner) && slot.holders.empty()) {
        type.free_slots.push_back(found->second);
        slot.text.clear();
        slot.regex.reset();
        type.slot_by_text.erase(found);
        type.stale = true;
      }
    }
    if (type.owners && !--type.owners) {
      types_.erase(found_type);
    }
  }
}

void KeyPatterns::_stamp(const std::vector<Owner>& owners, std::uint64_t evaluation) {
  for (Owner owner : owners) {
    owners_[owner].taken = evaluation;
  }
}

bool KeyPatterns::match(std::uint32_t type, const std::string& key, std::uint64_t* evaluation) {
  std::unordered_map<std::uint32_t, std::unique_ptr<Type>>::iterator found = types_.find(type);
  if (found == types_.end()) {
    return false;
  }
  Type& patterns = *found->second;
  const std::uint64_t now = ++evaluations_;
  *evaluation = now;
  std::unordered_map<std::string, std::vector<Owner>>::const_iterator listed = patterns.exact.find(key);
  if (listed != patterns.exact.end()) {
    _stamp(listed->second, now);
  }
  if (patterns.slot_by_text.empty()) {
    return true;
  }
  if (patterns.stale) {
    patterns.build();
  }
  if (patterns.set) {
    RE2::Set::ErrorInfo info;
    if (patterns.set->Match(key, &patterns.hits, &info)) {
      for (int index : patterns.hits) {
        _stamp(patterns.slots[patterns.slot_by_index[index]].holders, now);
      }
      return true;
    }
    if (info.kind == RE2::Set::kNoError) {
      return true;  // no pattern matched
    }
  }
  for (const Type::Pattern& slot : patterns.slots) {  // the set is out of memory, or could not be built
    if (slot.regex && RE2::FullMatch(key, *slot.regex)) {
      _stamp(slot.holders, now);
    }
  }
  return true;
}

std::size_t KeyPatterns::exact_keys(std::uint32_t type) const {
  std::unordered_map<std::uint32_t, std::unique_ptr<Type>>::const_iterator found = types_.find(type);
  return found == types_.end() ? 0 : found->second->exact.size();
}

std::size_t KeyPatterns::patterns(std::uint32_t type) const {
  std::unordered_map<std::uint32_t, std::unique_ptr<Type>>::const_iterator found = types_.find(type);
  return found == types_.end() ? 0 : found->second->slot_by_text.size();
}

};  // namespace multiplexer
