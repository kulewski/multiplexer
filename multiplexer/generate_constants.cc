// Build-time tool: reads a rules file and writes the constants for one
// language, multiplexer.constants.h (namespaces multiplexer::types and
// multiplexer::peers) or multiplexer_constants.py (classes types and peers),
// chosen by the output file's extension. Both include a get_name() that maps
// a number back to its name, for logs. Run by the genrules in
// multiplexer/BUILD on the file selected by //:multiplexer_rules.
//
// It refuses a rules file where a name or a number repeats, which the
// multiplexer's own Config<std::map> would silently collapse; hence the
// multimap instantiation below. IS_GENERATE_CONSTANTS keeps config.h from
// pulling in the logging library, so this tool stays small and builds early.
#define IS_GENERATE_CONSTANTS 1

#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <fstream>
#include <iostream>
#include <iterator>
#include <set>
#include <unordered_set>

#include "lib/sha1.h"
#include "lib/type_utils.h"
#include "multiplexer/config.h"

using namespace std;

typedef multiplexer::Config<std::multimap> Config;

// TODO(findepi) implement MX_DEFAULT_PROGRAM_LOGGING_LEVEL
// MX_DEFAULT_PROGRAM_LOGGING_LEVEL(mx::logging::INFO);

bool endswith(const std::string &str, const std::string &with) {
  if (str.size() < with.size())
    return false;
  for (unsigned int i = str.size() - with.size(); i != str.size(); ++i)
    if (str[i] != with[i + with.size() - str.size()])
      return false;
  return true;
}

template <typename ValueType> struct RepeatedKeyException : mx::Exception {
  RepeatedKeyException(const ValueType &value)
      : mx::Exception("value of '" + boost::lexical_cast<std::string>(value) + "' repeats") {}
  RepeatedKeyException(const ValueType &value, const std::string &hint)
      : mx::Exception("value of '" + boost::lexical_cast<std::string>(value) + "' repeats (" + hint + ")") {}
};

template <typename SetType, typename ValueType> void __set_checked_add(SetType &values, const ValueType &value) {
  if (!values.insert(value).second)
    MXTHROW(RepeatedKeyException<ValueType>(value));
}

template <typename SetType, typename ValueType>
void __set_checked_add(SetType &values, const ValueType &value, const std::string &hint) {
  if (!values.insert(value).second)
    MXTHROW(RepeatedKeyException<ValueType>(value, hint));
}

template <typename Map> void check_map_values_name_type_uniqueness(const Map &entry) {
  std::set<std::string> names;
  std::unordered_set<boost::uint32_t> ids;

  BOOST_FOREACH (const typename Map::value_type &value, entry) {
    __set_checked_add(names, value.second.name());
    __set_checked_add(ids, value.second.type(), "somewhere about " + value.second.name());
  }
  Assert(entry.size() == names.size());
  Assert(entry.size() == ids.size());
}

void check_config(Config &config) {

  check_map_values_name_type_uniqueness(config.message_description_by_id());
  check_map_values_name_type_uniqueness(config.peer_by_type());
}

void write_signature(const char *comment, ostream &out, const std::string &source_file) {
  out << "\n"
      << comment << "\n"
      << comment << " this file is generated from " << source_file << "\n"
      << comment << " by a program from " << __FILE__ << " compiled " << __DATE__ " " __TIME__ << "\n"
      << comment << "\n"
      << "\n";
}

template <typename Map>
void __write_python_name_to_type_mapping(ostream &out, const Map &map, const std::string &set_name) {
  out << "class " << set_name << ":\n";
  out << "\n";
  BOOST_FOREACH (const typename Map::value_type &entry, map)
    out << "\t" << entry.second.name() << " = " << entry.second.type() << "\n";
  out << "\n";
  out << "\t"
      << "idtoname = {}\n";
  BOOST_FOREACH (const typename Map::value_type &entry, map)
    out << "\t"
        << "idtoname[" << entry.second.name() << "] = '" << entry.second.name() << "'\n";
  out << "\t"
      << "pass\n"
      << "\n";
}

// The SHA-1 of the rules file's text, so that a recording made by a
// multiplexer running with a different rules file can be told apart.
std::string rules_sha1(const std::string &source_file) {
  ifstream in(source_file.c_str(), ifstream::binary);
  std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  return mx::sha1_hex(text);
}

int write_python(Config &config, ostream &out, const std::string &source_file) {
  write_signature("#", out, source_file);
  out << "# SHA-1 of the rules file these constants were generated from; a recording's header carries the same.\n"
      << "RULES_SHA1 = \"" << rules_sha1(source_file) << "\"\n\n";
  out << "class _constants_base:\n"
      << "\t"
      << "idtoname = None # dict defined by a subclass\n"
      << "\t"
      << "@classmethod\n"
      << "\t"
      << "def get_name(cls, type, default = 'UNKNOWN'):\n"
      << "\t"
      << "\t"
      << "return cls.idtoname.get(type, default)\n"
      << "\t"
      << "pass\n"
      << "\n";
  __write_python_name_to_type_mapping(out, config.message_description_by_id(), "types(_constants_base)");
  __write_python_name_to_type_mapping(out, config.peer_by_type(), "peers(_constants_base)");

  return 0;
}

// The generated header's include guard is random rather than derived from
// the path, because the same header is generated under different names in
// different workspaces (this one, and every consumer of @mx).
std::string get_random_identifier(unsigned int length) {
  ifstream in("/dev/urandom");
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

template <typename Map>
void __write_cxx_name_to_type_mapping(ostream &out, const Map &map, const std::string &set_name) {
  const char *const PREF = "\t";

  out << PREF << "namespace " << set_name << " {\n";
  BOOST_FOREACH (const typename Map::value_type &entry, map)
    out << PREF << "\t"
        << "static const boost::uint32_t " << entry.second.name() << " = " << entry.second.type() << ";\n";

  // generate get_name() function using great switch() statement
  out << "\n"
      << PREF << "\t"
      << "static inline const char* get_name(const boost::uint32_t t, const "
         "char* default_ = \"UNKNOWN\") {\n"
      << PREF << "\t"
      << "\t"
      << "switch(t) {\n";
  BOOST_FOREACH (const typename Map::value_type &entry, map)
    out << PREF << "\t"
        << "\t"
        << "\t"
        << "case " << entry.second.name() << ": return \"" << entry.second.name() << "\";\n";
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

int write_cxx(Config &config, ostream &out, const std::string &source_file) {
  std::string identifier = get_random_identifier(10);
  out << "#ifndef GENERATED_" << identifier << "\n"
      << "#define GENERATED_" << identifier << "\n"
      << "\n"
      << "#include <boost/cstdint.hpp>\n"
      << "\n";

  write_signature("//", out, source_file);
  out << "namespace multiplexer {\n"
      << "\t// SHA-1 of the rules file these constants were generated from.\n"
      << "\tstatic const char *const RULES_SHA1 = \"" << rules_sha1(source_file) << "\";\n\n";
  __write_cxx_name_to_type_mapping(out, config.message_description_by_id(), "types");
  __write_cxx_name_to_type_mapping(out, config.peer_by_type(), "peers");

  out << "}; // namespace multiplexer\n"
      << "#endif\n";
  return 0;
}

int MxMain(int argc, char **argv) {

  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <multiplexer.rules file> (C++ header file | Python file)\n"
              << "  Program generates definition of constants found in "
                 "multiplexer.rules file.\n";
    return 1;
  }

  enum OUTFILETYPE { PYTHON, CXX };
  OUTFILETYPE filetype;
  if (endswith(argv[2], ".h"))
    filetype = CXX;
  else if (endswith(argv[2], ".py"))
    filetype = PYTHON;
  else {
    std::cerr << "Unknown file type that is " << argv[2] << "\n";
    return 2;
  }

  ofstream out(argv[2], ofstream::binary | ofstream::out);
  if (!out) {
    std::cerr << "Failed to open " << argv[2] << " for writing.\n";
    return 2;
  }

  Config config(argv[1]);

  check_config(config);

  switch (filetype) {
  case PYTHON:
    return write_python(config, out, argv[1]);
  case CXX:
    return write_cxx(config, out, argv[1]);
  }
  assert(false);
  std::cerr << "unreachable code.\n";
  abort();
}

int main(int argc, char **argv) {
  using std::cerr;
  using std::endl;

  try {
    return MxMain(argc, argv);

  } catch (mx::Exception &e) {
    cerr << mx::type_utils::type_name(e) << " in " << e.file() << ":" << e.line() << " (" << e.function() << ")\n"
         << "    " << e.what() << endl;
    return 1;

  } catch (std::exception &e) {
    cerr << mx::type_utils::type_name(e) << ": " << e.what() << "\n";
    return 1;
  }
}
