// Build-time tool: reads a rules file and writes the constants for one
// language, multiplexer.constants.h, multiplexer_constants.py or its .pyi
// stub, chosen by the output file's extension; multiplexer/constants_writer.h
// does the writing, and `mxcontrol generate_constants` is the same tool for
// a program installed from a release. Run by the genrules in
// multiplexer/BUILD on the file selected by //:multiplexer_rules.
//
// It refuses a rules file where a name or a number repeats, which the
// multiplexer's own Config<std::map> would silently collapse; hence the
// multimap instantiation below. IS_GENERATE_CONSTANTS keeps config.h from
// pulling in the logging library, so this tool stays small and builds early.
#define IS_GENERATE_CONSTANTS 1

#include <iostream>

#include "lib/type_utils.h"
#include "multiplexer/config.h"
#include "multiplexer/constants_writer.h"

int MxMain(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <multiplexer.rules file> (C++ header file | Python file | Python stub)\n"
              << "  Program generates definition of constants found in "
                 "multiplexer.rules file.\n";
    return 1;
  }
  multiplexer::Config<std::multimap> config(argv[1]);
  multiplexer::constants::check_config(config);
  multiplexer::constants::write_constants(config, argv[1], argv[2]);
  return 0;
}

int main(int argc, char** argv) {
  using std::cerr;
  using std::endl;

  try {
    return MxMain(argc, argv);

  } catch (mx::Exception& e) {
    cerr << mx::type_utils::type_name(e) << " in " << e.file() << ":" << e.line() << " (" << e.function() << ")\n"
         << "    " << e.what() << endl;
    return 1;

  } catch (std::exception& e) {
    cerr << mx::type_utils::type_name(e) << ": " << e.what() << "\n";
    return 1;
  }
}
