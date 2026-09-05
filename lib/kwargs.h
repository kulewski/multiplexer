// Kwargs: named, optionally typed arguments, see the struct comment.
#ifndef MX_LIB_KWARGS_H_
#define MX_LIB_KWARGS_H_

#include <map>
#include <set>
#include <string>

#include <boost/any.hpp>

#include "lib/exception.h"

namespace mx {
namespace util {
namespace kwargs {

struct KeyError : mx::Exception {};

struct Kwargs;
struct KwargsKeys;

// Keyword arguments for C++: a map from name to boost::any, so that a call
// with many optional parameters (BaseMultiplexerServer::send_message) reads
// like Python. Values keep their static type: get<T> throws bad_any_cast on
// a mismatch, so callers must pass exactly the type the callee expects, e.g.
// boost::uint32_t rather than int. A Kwargs is a value: a copy is
// independent of the original.
struct Kwargs {

  typedef std::map<std::string, boost::any> KwValuesMap;
  typedef KwValuesMap::value_type KwValue;

  Kwargs() {}

  // Store `value` under `key`, replacing any earlier value; chainable.
  template <typename T> Kwargs &set(const std::string &key, const T &value) {
    __values[key] = value;
    return *this;
  }

  // The value under `key` as T, or `default_` when absent.
  template <typename T> T get(const std::string &key, const T &default_) {
    try {
      return get<T>(key);
    } catch (const KeyError &) {
      return default_;
    }
  }

  // The value under `key` as T; KeyError when absent, bad_any_cast on a type mismatch.
  template <typename T> T get(const std::string &key) {
    KwValuesMap::const_iterator pos = __values.find(key);
    if (pos != __values.end()) {
      return __cast<T>(pos);
    } else {
      throw KeyError();
    }
  }

  // get<T> without the presence check: the key must exist.
  template <typename T> T inline unsafe_get(const std::string &key) { return __cast<T>(__values.find(key)); }

  bool has_key(const std::string &key) {
    KwValuesMap::const_iterator pos = __values.find(key);
    return (pos != __values.end());
  }

  // Absent, or present with exactly type T.
  template <typename T> bool empty_or(const std::string &key) { return !has_key(key) || unsafe_is<T>(key); }

  // Present with exactly type T; the key must exist.
  template <typename T> bool unsafe_is(const std::string &key) {
    return __values.find(key)->second.type() == typeid(T);
  }

  // Store `value` under `key` only if nothing is there yet; chainable.
  template <typename T> Kwargs &set_default(const std::string &key, const T &value) {
    __values.insert(KwValue(key, boost::any(value)));
    return *this;
  }

  // Whether every key present is one of `keys`, for debug assertions.
  bool check_keys(const KwargsKeys &keys);

private:
  template <typename T> T inline __cast(KwValuesMap::const_iterator pos) { return boost::any_cast<T>(pos->second); }

private:
  KwValuesMap __values;
};

// A set of permitted key names, for Kwargs::check_keys in debug builds:
// KwargsKeys("message")("type")("to").
struct KwargsKeys {
  KwargsKeys() {}

  KwargsKeys(const std::string &key) { (*this)(key); }

  KwargsKeys &operator()(const std::string &key) {
    __keys.insert(key);
    return *this;
  }

private:
  std::set<std::string> __keys;
  friend struct Kwargs;
};

}; // namespace kwargs
}; // namespace util
}; // namespace mx

#endif // MX_LIB_KWARGS_H_
