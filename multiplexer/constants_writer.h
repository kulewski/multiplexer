// The constants from a rules file, written for one language: what
// generate_constants.cc, the build-time tool, and `mxcontrol
// generate_constants`, the same for a program installed from a release,
// both do. multiplexer.constants.h holds namespaces multiplexer::types and
// multiplexer::peers, multiplexer_constants.py the classes types and peers,
// and the .pyi their stub for type checkers; each with a get_name() that
// maps a number back to its name, for logs, and the rules file's CRC-32 as
// RULES_FINGERPRINT, which a recording's header carries too.
//
// Header only, templated on the Config, because the tool compiles config.h
// without the logging library (IS_GENERATE_CONSTANTS) and mxcontrol with
// it, and one translation unit must not see both. check_config() refuses a
// rules file where a name or a number repeats, which the multiplexer's own
// Config<std::map> would silently collapse, hence the callers' multimap,
// and one with a name the generated files cannot hold as it is.
#ifndef MX_MULTIPLEXER_CONSTANTS_WRITER_H_
#define MX_MULTIPLEXER_CONSTANTS_WRITER_H_

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <fstream>
#include <ostream>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_set>

#include "lib/assertion.h"
#include "lib/exception.h"
#include "lib/fingerprint.h"
#include "lib/repr.h"

namespace multiplexer {
namespace constants {

template <typename ValueType>
struct RepeatedKeyException : mx::Exception {
  explicit RepeatedKeyException(const ValueType& value) : mx::Exception("value of '" + mx::repr(value) + "' repeats") {}
  RepeatedKeyException(const ValueType& value, const std::string& hint)
      : mx::Exception("value of '" + mx::repr(value) + "' repeats (" + hint + ")") {}
};

struct UnusableNameException : mx::Exception {
  UnusableNameException(const std::string& name, std::uint32_t type, const char* why)
      : mx::Exception("the name '" + name + "' of type " + std::to_string(type) + " cannot be a constant: " + why) {}
};

namespace detail {

template <typename SetType, typename ValueType>
void checked_add(SetType& values, const ValueType& value) {
  if (!values.insert(value).second) {
    MXTHROW(RepeatedKeyException<ValueType>(value));
  }
}

template <typename SetType, typename ValueType>
void checked_add(SetType& values, const ValueType& value, const std::string& hint) {
  if (!values.insert(value).second) {
    MXTHROW(RepeatedKeyException<ValueType>(value, hint));
  }
}

// Why `name` cannot be a constant of the generated files, NULL when it
// can: the Python classes and the C++ namespaces hold every name as it is,
// next to the names the generated code itself uses there, idtoname and
// get_name, and the C++ get_name()'s parameters, t and default_, which
// would hide a constant of the same name in its switch.
inline const char* unusable_name(const std::string& name) {
  static const std::set<std::string> KEYWORDS = {
      // Python's
      "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class", "continue", "def", "del",
      "elif", "else", "except", "finally", "for", "from", "global", "if", "import", "in", "is", "lambda", "nonlocal",
      "not", "or", "pass", "raise", "return", "try", "while", "with", "yield",
      // C++'s
      "alignas", "alignof", "and_eq", "asm", "auto", "bitand", "bitor", "bool", "case", "catch", "char", "char8_t",
      "char16_t", "char32_t", "co_await", "co_return", "co_yield", "compl", "concept", "const", "const_cast",
      "consteval", "constexpr", "constinit", "decltype", "default", "delete", "do", "double", "dynamic_cast", "enum",
      "explicit", "export", "extern", "false", "float", "friend", "goto", "inline", "int", "long", "mutable",
      "namespace", "new", "noexcept", "not_eq", "nullptr", "operator", "or_eq", "private", "protected", "public",
      "register", "reinterpret_cast", "requires", "short", "signed", "sizeof", "static", "static_assert", "static_cast",
      "struct", "switch", "template", "this", "thread_local", "throw", "true", "typedef", "typeid", "typename", "union",
      "unsigned", "using", "virtual", "void", "volatile", "wchar_t", "xor", "xor_eq"};
  static const std::set<std::string> GENERATED = {"idtoname", "get_name", "t", "default_"};
  auto letter = [](char c) { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_'; };
  if (name.empty() || !letter(name[0])) {
    return "not an identifier";
  }
  for (char c : name) {
    if (!letter(c) && !(c >= '0' && c <= '9')) {
      return "not an identifier";
    }
  }
  if (name.compare(0, 2, "__") == 0) {
    return "reserved, it starts with two underscores";
  }
  if (KEYWORDS.count(name)) {
    return "a keyword of Python or C++";
  }
  if (GENERATED.count(name)) {
    return "a name the generated code uses";
  }
  return NULL;
}

template <typename Map>
void check_names_usable(const Map& entries) {
  for (const typename Map::value_type& value : entries) {
    if (const char* why = unusable_name(value.second.name())) {
      MXTHROW(UnusableNameException(value.second.name(), value.second.type(), why));
    }
  }
}

template <typename Map>
void check_names_and_numbers_unique(const Map& entries) {
  std::set<std::string> names;
  std::unordered_set<std::uint32_t> ids;
  for (const typename Map::value_type& value : entries) {
    checked_add(names, value.second.name());
    checked_add(ids, value.second.type(), "somewhere about " + value.second.name());
  }
  Assert(entries.size() == names.size());
  Assert(entries.size() == ids.size());
}

inline void write_signature(const char* comment, std::ostream& out, const std::string& source_file) {
  out << comment << "\n"
      << comment << " this file is generated from " << source_file << " (mxcontrol generate_constants); do not edit\n"
      << comment << "\n"
      << "\n";
}

// One class of the Python module, in the form black leaves alone, so that
// a committed copy of the module never differs from a regenerated one.
template <typename Map>
void write_python_mapping(std::ostream& out, const Map& map, const std::string& set_name) {
  out << "class " << set_name << ":\n\n";
  for (const typename Map::value_type& entry : map) {
    out << "    " << entry.second.name() << " = " << entry.second.type() << "\n";
  }
  out << "\n    idtoname = {}\n";
  for (const typename Map::value_type& entry : map) {
    out << "    idtoname[" << entry.second.name() << "] = \"" << entry.second.name() << "\"\n";
  }
}

// The stub of the Python module, for type checkers: the same names, typed.
template <typename Map>
void write_python_stub_mapping(std::ostream& out, const Map& map, const std::string& set_name) {
  out << "class " << set_name << ":\n";
  for (const typename Map::value_type& entry : map) {
    out << "    " << entry.second.name() << ": int\n";
  }
  out << "\n";
}

template <typename Map>
void write_cxx_mapping(std::ostream& out, const Map& map, const std::string& set_name) {
  const char* const PREF = "\t";

  out << PREF << "namespace " << set_name << " {\n";
  for (const typename Map::value_type& entry : map) {
    out << PREF << "\t"
        << "static const std::uint32_t " << entry.second.name() << " = " << entry.second.type() << ";\n";
  }

  // generate get_name() function using great switch() statement
  out << "\n"
      << PREF << "\t"
      << "static inline const char* get_name(const std::uint32_t t, const "
         "char* default_ = \"UNKNOWN\") {\n"
      << PREF << "\t"
      << "\t"
      << "switch(t) {\n";
  for (const typename Map::value_type& entry : map) {
    out << PREF << "\t"
        << "\t"
        << "\t"
        << "case " << entry.second.name() << ": return \"" << entry.second.name() << "\";\n";
  }
  out << PREF << "\t"
      << "\t"
      << "\t"
      << "default: return default_;\n"
      << PREF << "\t"
      << "\t"
      << "}\n"
      << PREF << "\t"
      << "} // get_name\n";
  out << PREF << "} // namespace " << set_name << "\n"
      << "\n";
}

}  // namespace detail

// Refuses a rules file where a peer or message name or number repeats, or
// a name could not be a constant in Python and C++ (unusable_name).
template <typename Config>
void check_config(const Config& config) {
  detail::check_names_and_numbers_unique(config.message_description_by_id());
  detail::check_names_and_numbers_unique(config.peer_by_type());
  detail::check_names_usable(config.message_description_by_id());
  detail::check_names_usable(config.peer_by_type());
}

// The rules file's text, read once: the constants are parsed from it and
// its fingerprint, the CRC-32 that tells a recording made with other rules
// apart, is its own. Read a second time for the fingerprint, a pipe or a
// process substitution gave nothing, and the files said 00000000. Throws
// std::runtime_error saying why it could not be read.
inline std::string read_rules(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    throw std::runtime_error("cannot read " + path + ": " + std::system_category().message(errno));
  }
  std::string text;
  char buffer[16384];
  for (;;) {
    const ssize_t got = ::read(fd, buffer, sizeof buffer);
    if (got > 0) {
      text.append(buffer, static_cast<std::size_t>(got));
    } else if (got == 0) {
      break;
    } else if (errno != EINTR) {
      const int error = errno;
      ::close(fd);
      throw std::runtime_error("cannot read " + path + ": " + std::system_category().message(error));
    }
  }
  ::close(fd);
  return text;
}

// `fingerprint` is mx::fingerprint of the text `config` was parsed from,
// in every writer that writes one.
template <typename Config>
void write_python(const Config& config, std::ostream& out, const std::string& source_file,
                  const std::string& fingerprint) {
  detail::write_signature("#", out, source_file);
  out << "# CRC-32 of the rules file these constants were generated from; a recording's header carries the same.\n"
      << "RULES_FINGERPRINT = \"" << fingerprint << "\"\n\n";
  out << "\n"
      << "class _constants_base:\n"
      << "    idtoname = None  # dict defined by a subclass\n"
      << "\n"
      << "    @classmethod\n"
      << "    def get_name(cls, type, default=\"UNKNOWN\"):\n"
      << "        return cls.idtoname.get(type, default)\n"
      << "\n\n";
  detail::write_python_mapping(out, config.message_description_by_id(), "types(_constants_base)");
  out << "\n\n";
  detail::write_python_mapping(out, config.peer_by_type(), "peers(_constants_base)");
}

template <typename Config>
void write_python_stub(const Config& config, std::ostream& out, const std::string& source_file) {
  detail::write_signature("#", out, source_file);
  out << "RULES_FINGERPRINT: str\n\n"
      << "class _constants_base:\n"
      << "    idtoname: dict[int, str]\n"
      << "    @classmethod\n"
      << "    def get_name(cls, type: int, default: str = ...) -> str: ...\n\n";
  detail::write_python_stub_mapping(out, config.message_description_by_id(), "types(_constants_base)");
  detail::write_python_stub_mapping(out, config.peer_by_type(), "peers(_constants_base)");
}

// The include guard comes from the rules file's fingerprint, not from a
// path, since the same header is generated under different names in
// different workspaces (this one, and every consumer of @mx): headers of
// different rules get different guards, and the same rules give the same
// header, byte for byte, in every build.
template <typename Config>
void write_cxx(const Config& config, std::ostream& out, const std::string& source_file,
               const std::string& fingerprint) {
  out << "#ifndef GENERATED_MX_CONSTANTS_" << fingerprint << "\n"
      << "#define GENERATED_MX_CONSTANTS_" << fingerprint << "\n"
      << "\n"
      << "#include <cstdint>\n"
      << "\n";

  detail::write_signature("//", out, source_file);
  out << "namespace multiplexer {\n"
      << "\t// CRC-32 of the rules file these constants were generated from.\n"
      << "\tstatic const char *const RULES_FINGERPRINT = \"" << fingerprint << "\";\n\n";
  detail::write_cxx_mapping(out, config.message_description_by_id(), "types");
  detail::write_cxx_mapping(out, config.peer_by_type(), "peers");

  out << "}; // namespace multiplexer\n"
      << "#endif\n";
}

// Writes the file `path` in the language its extension names: .h, .py or
// .pyi. Throws mx::Exception on a rules file with a repeated name or
// number, std::runtime_error on an extension it does not know or a file
// it cannot open.
template <typename Config>
void write_constants(const Config& config, const std::string& rules_file, const std::string& fingerprint,
                     const std::string& path) {
  enum Language { PYTHON, PYTHON_STUB, CXX } language;
  auto ends_with = [&path](const char* suffix) {
    std::string with(suffix);
    return path.size() >= with.size() && path.compare(path.size() - with.size(), with.size(), with) == 0;
  };
  if (ends_with(".h")) {
    language = CXX;
  } else if (ends_with(".pyi")) {
    language = PYTHON_STUB;
  } else if (ends_with(".py")) {
    language = PYTHON;
  } else {
    throw std::runtime_error("cannot tell the language from the name of " + path + ": .h, .py or .pyi expected");
  }
  std::ofstream out(path.c_str(), std::ofstream::binary | std::ofstream::out);
  if (!out) {
    throw std::runtime_error("cannot open " + path + " for writing");
  }
  switch (language) {
    case PYTHON:
      write_python(config, out, rules_file, fingerprint);
      break;
    case PYTHON_STUB:
      write_python_stub(config, out, rules_file);
      break;
    case CXX:
      write_cxx(config, out, rules_file, fingerprint);
      break;
  }
}

}  // namespace constants
}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_CONSTANTS_WRITER_H_
