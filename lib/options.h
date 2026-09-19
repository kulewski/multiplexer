// A small command-line options parser: declare options bound to variables,
// parse a vector of arguments, print help. --name VALUE, --name=VALUE,
// -S VALUE and -SVALUE forms, switches, repeatable options, defaults,
// required options, hidden options and positional arguments, which is what
// mxcontrol and the test roles need. Values go through mx::from_string
// (lib/repr.h), strings verbatim. Nothing more: no config files, no
// option groups.
#ifndef MX_LIB_OPTIONS_H_
#define MX_LIB_OPTIONS_H_

#include <functional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "lib/repr.h"

namespace mx {
namespace options {

// A malformed command line: an unknown option, a missing or invalid value,
// a required option not given, too many positional arguments.
class Error : public std::runtime_error {
public:
  explicit Error(const std::string &what) : std::runtime_error(what) {}
};

namespace detail {
// One value into its variable: verbatim for a string, parsed otherwise.
inline void assign(std::string *target, const std::string &text) { *target = text; }
template <typename T> inline void assign(T *target, const std::string &text) { *target = from_string<T>(text); }
template <typename T> inline void assign(std::vector<T> *target, const std::string &text) {
  T value;
  assign(&value, text);
  target->push_back(value);
}
template <typename T> inline std::string default_text(const T &value) {
  std::ostringstream out;
  out << value;
  return out.str();
}
} // namespace detail

class Options {
public:
  // `caption` heads the help text, as "Options:".
  explicit Options(const std::string &caption = "Options") : caption_(caption) {}

  // --name VALUE into *target. `spec` is "name" or "name,S" for a short
  // -S form too.
  template <typename T> Options &add(const std::string &spec, T *target, const std::string &description) {
    return _add(spec, description, false, [target](const std::string &text) { detail::assign(target, text); });
  }
  // As above with a default, assigned now and shown in the help.
  template <typename T, typename Default>
  Options &add(const std::string &spec, T *target, const Default &default_value, const std::string &description) {
    *target = default_value;
    add(spec, target, description);
    options_.back().default_text = detail::default_text(*target);
    options_.back().has_default = true;
    return *this;
  }
  // Repeatable: every occurrence appended to *target.
  template <typename T> Options &add(const std::string &spec, std::vector<T> *target, const std::string &description) {
    return _add(spec, description, false, [target](const std::string &text) { detail::assign(target, text); })
        ._repeatable();
  }
  // --name with no value: *target becomes true.
  Options &add_switch(const std::string &spec, bool *target, const std::string &description);

  // The option added last must be given.
  Options &required();
  // The option added last stays out of the help.
  Options &hidden();
  // Arguments that are not options fill the option `name`, `count` of
  // them in a row; -1 for all that remain. Declared in the order the
  // arguments come.
  Options &positional(const std::string &name, int count = 1);

  // Parses `args`, assigning every option's variable. Throws Error on a
  // malformed line. With `allow_unrecognized`, options not declared here
  // and positional arguments beyond the declared ones are returned, in
  // their order, instead of being errors.
  std::vector<std::string> parse(const std::vector<std::string> &args, bool allow_unrecognized = false);
  // Whether `name` was on the last parsed line.
  bool given(const std::string &name) const;
  // The help: one line per visible option with its default.
  void print(std::ostream &out) const;

private:
  struct Option {
    std::string name;
    char short_name = 0;
    std::string description;
    bool is_switch = false;
    bool repeatable = false;
    bool required = false;
    bool hidden = false;
    std::string default_text;
    bool has_default = false;
    std::function<void(const std::string &)> assign;
    unsigned int count = 0;
  };
  struct Positional {
    std::string name;
    int count;
  };

  Options &_add(const std::string &spec, const std::string &description, bool is_switch,
                std::function<void(const std::string &)> assign);
  Options &_repeatable();
  Option *_find(const std::string &name);
  Option *_find(char short_name);
  void _take(Option &option, const std::string &value_text);

  std::string caption_;
  std::vector<Option> options_;
  std::vector<Positional> positionals_;
};

inline std::ostream &operator<<(std::ostream &out, const Options &options) {
  options.print(out);
  return out;
}

} // namespace options
} // namespace mx

#endif // MX_LIB_OPTIONS_H_
