// See options.h.
#include "lib/options.h"

#include <algorithm>

namespace mx {
namespace options {

Options &Options::_add(const std::string &spec, const std::string &description, bool is_switch,
                       std::function<void(const std::string &)> assign) {
  Option option;
  std::string::size_type comma = spec.find(',');
  option.name = spec.substr(0, comma);
  if (comma != std::string::npos && comma + 1 < spec.size())
    option.short_name = spec[comma + 1];
  option.description = description;
  option.is_switch = is_switch;
  option.assign = assign;
  options_.push_back(option);
  return *this;
}

Options &Options::add_switch(const std::string &spec, bool *target, const std::string &description) {
  *target = false;
  return _add(spec, description, true, [target](const std::string &) { *target = true; });
}

Options &Options::_repeatable() {
  options_.back().repeatable = true;
  return *this;
}

Options &Options::required() {
  options_.back().required = true;
  return *this;
}

Options &Options::hidden() {
  options_.back().hidden = true;
  return *this;
}

Options &Options::positional(const std::string &name, int count) {
  if (!_find(name))
    throw std::logic_error("positional argument '" + name + "' names no option");
  Positional positional = {name, count};
  positionals_.push_back(positional);
  return *this;
}

Options::Option *Options::_find(const std::string &name) {
  for (Option &option : options_)
    if (option.name == name)
      return &option;
  return NULL;
}

Options::Option *Options::_find(char short_name) {
  for (Option &option : options_)
    if (short_name && option.short_name == short_name)
      return &option;
  return NULL;
}

void Options::_take(Option &option, const std::string &value_text) {
  if (option.count && !option.repeatable && !option.is_switch)
    throw Error("option '--" + option.name + "' cannot be given more than once");
  ++option.count;
  try {
    option.assign(value_text);
  } catch (const std::invalid_argument &) {
    throw Error("the argument ('" + value_text + "') for option '--" + option.name + "' is invalid");
  }
}

std::vector<std::string> Options::parse(const std::vector<std::string> &args, bool allow_unrecognized) {
  for (Option &option : options_)
    option.count = 0;
  // What was not an option of ours, in order; the positional ones fill the
  // declared slots below.
  std::vector<std::pair<std::string, bool>> leftover; // (token, is_positional)
  bool only_positional = false;
  for (std::vector<std::string>::size_type index = 0; index < args.size(); ++index) {
    const std::string &token = args[index];
    if (only_positional || token.size() < 2 || token[0] != '-') {
      leftover.push_back(std::make_pair(token, true));
      continue;
    }
    if (token == "--") {
      only_positional = true;
      continue;
    }
    Option *option;
    std::string name;
    std::string value;
    bool has_value = false;
    if (token[1] == '-') {
      std::string::size_type equals = token.find('=');
      name = token.substr(2, equals == std::string::npos ? std::string::npos : equals - 2);
      if (equals != std::string::npos) {
        value = token.substr(equals + 1);
        has_value = true;
      }
      option = _find(name);
    } else {
      name = token.substr(1, 1);
      option = _find(token[1]);
      if (token.size() > 2) {
        value = token.substr(2);
        has_value = true;
      }
    }
    if (!option) {
      if (!allow_unrecognized)
        throw Error("unrecognised option '" + token + "'");
      leftover.push_back(std::make_pair(token, false));
      continue;
    }
    if (option->is_switch) {
      if (has_value)
        throw Error("option '--" + option->name + "' takes no value");
      _take(*option, "");
      continue;
    }
    if (!has_value) {
      if (index + 1 >= args.size())
        throw Error("the required argument for option '--" + option->name + "' is missing");
      value = args[++index];
    }
    _take(*option, value);
  }
  std::vector<std::string> unrecognized;
  std::vector<Positional>::size_type slot = 0;
  int taken_in_slot = 0;
  for (const std::pair<std::string, bool> &entry : leftover) {
    if (entry.second) {
      while (slot < positionals_.size() && positionals_[slot].count >= 0 && taken_in_slot >= positionals_[slot].count) {
        ++slot;
        taken_in_slot = 0;
      }
      if (slot < positionals_.size()) {
        _take(*_find(positionals_[slot].name), entry.first);
        ++taken_in_slot;
        continue;
      }
      if (!allow_unrecognized)
        throw Error("too many positional arguments: '" + entry.first + "'");
    }
    unrecognized.push_back(entry.first);
  }
  for (const Option &option : options_)
    if (option.required && !option.count)
      throw Error("the option '--" + option.name + "' is required but missing");
  return unrecognized;
}

bool Options::given(const std::string &name) const {
  for (const Option &option : options_)
    if (option.name == name)
      return option.count > 0;
  return false;
}

// Word wrap at `width`, every line but the first indented to `column`.
static void print_wrapped(std::ostream &out, const std::string &text, std::string::size_type column,
                          std::string::size_type width) {
  std::string::size_type used = column;
  bool at_line_start = true;
  std::istringstream lines(text);
  std::string line;
  bool first_line = true;
  while (std::getline(lines, line)) {
    if (!first_line) {
      out << "\n" << std::string(column, ' ');
      used = column;
      at_line_start = true;
    }
    first_line = false;
    std::istringstream words(line);
    std::string word;
    while (words >> word) {
      if (!at_line_start && used + 1 + word.size() > width) {
        out << "\n" << std::string(column, ' ');
        used = column;
        at_line_start = true;
      }
      if (!at_line_start) {
        out << " ";
        ++used;
      }
      out << word;
      used += word.size();
      at_line_start = false;
    }
  }
}

void Options::print(std::ostream &out) const {
  const std::string::size_type width = 100;
  const std::string::size_type max_column = 40;
  std::vector<std::string> labels;
  std::string::size_type column = 0;
  for (const Option &option : options_) {
    if (option.hidden)
      continue;
    std::string label = "  ";
    if (option.short_name)
      label += std::string("-") + option.short_name + ", ";
    label += "--" + option.name;
    if (!option.is_switch)
      label += " ARG";
    if (option.has_default)
      label += " (=" + option.default_text + ")";
    labels.push_back(label);
    if (label.size() + 2 <= max_column)
      column = std::max(column, label.size() + 2);
  }
  if (caption_.size())
    out << caption_ << ":\n";
  std::vector<std::string>::size_type label_index = 0;
  for (const Option &option : options_) {
    if (option.hidden)
      continue;
    const std::string &label = labels[label_index++];
    out << label;
    if (label.size() + 2 > column)
      out << "\n" << std::string(column, ' ');
    else
      out << std::string(column - label.size(), ' ');
    print_wrapped(out, option.description, column, width);
    out << "\n";
  }
}

} // namespace options
} // namespace mx
