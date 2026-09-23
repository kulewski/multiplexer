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
// Config<std::map> would silently collapse; hence the callers' multimap.
#ifndef MX_MULTIPLEXER_CONSTANTS_WRITER_H_
#define MX_MULTIPLEXER_CONSTANTS_WRITER_H_

#include <cstdint>
#include <fstream>
#include <iterator>
#include <ostream>
#include <set>
#include <string>
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

// The generated header's include guard is random rather than derived from
// the path, because the same header is generated under different names in
// different workspaces (this one, and every consumer of @mx).
inline std::string random_identifier(unsigned int length) {
  std::ifstream in("/dev/urandom");
  std::string identifier;
  while (identifier.size() < length) {
    char character;
    do {
      in >> character;
    } while (!((character >= 'A' && character <= 'Z') | (character >= 'a' && character <= 'z') |
               (character >= '0' && character <= '9')));
    identifier += character;
  }
  return identifier;
}

}  // namespace detail

// Refuses a rules file where a peer or message name or number repeats.
template <typename Config>
void check_config(const Config& config) {
  detail::check_names_and_numbers_unique(config.message_description_by_id());
  detail::check_names_and_numbers_unique(config.peer_by_type());
}

// The CRC-32 of the rules file's text, so that a recording made by a
// multiplexer running with a different rules file can be told apart.
inline std::string rules_fingerprint(const std::string& source_file) {
  std::ifstream in(source_file.c_str(), std::ifstream::binary);
  std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  return mx::fingerprint(text);
}

template <typename Config>
void write_python(const Config& config, std::ostream& out, const std::string& source_file) {
  detail::write_signature("#", out, source_file);
  out << "# CRC-32 of the rules file these constants were generated from; a recording's header carries the same.\n"
      << "RULES_FINGERPRINT = \"" << rules_fingerprint(source_file) << "\"\n\n";
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

template <typename Config>
void write_cxx(const Config& config, std::ostream& out, const std::string& source_file) {
  std::string identifier = detail::random_identifier(10);
  out << "#ifndef GENERATED_" << identifier << "\n"
      << "#define GENERATED_" << identifier << "\n"
      << "\n"
      << "#include <cstdint>\n"
      << "\n";

  detail::write_signature("//", out, source_file);
  out << "namespace multiplexer {\n"
      << "\t// CRC-32 of the rules file these constants were generated from.\n"
      << "\tstatic const char *const RULES_FINGERPRINT = \"" << rules_fingerprint(source_file) << "\";\n\n";
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
void write_constants(const Config& config, const std::string& rules_file, const std::string& path) {
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
      write_python(config, out, rules_file);
      break;
    case PYTHON_STUB:
      write_python_stub(config, out, rules_file);
      break;
    case CXX:
      write_cxx(config, out, rules_file);
      break;
  }
}

}  // namespace constants
}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_CONSTANTS_WRITER_H_
