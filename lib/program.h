// Provides main() for programs that define MxMain(); see the note below.
#ifndef MX_LIB_PROGRAM_H_
#define MX_LIB_PROGRAM_H_

// Defines main() for a program that provides int MxMain(int, char**): runs
// it and returns its result; an uncaught mx::Exception is printed with its
// throw site, any other std::exception with its type, and the program exits
// with 1. Include it from exactly one file of a binary.

#include "lib/exception.h"
#include "lib/type_utils.h"
#include <iostream>

int MxMain(int argc, char **argv);

int main(int argc, char **argv) {
  using std::cerr;
  using std::endl;

  try {
    return MxMain(argc, argv);

  } catch (mx::Exception &error) {
    cerr << mx::type_utils::type_name(error) << " in " << error.file() << ":" << error.line() << " ("
         << error.function() << ")\n"
         << "    " << error.what() << endl;
    return 1;

  } catch (std::exception &error) {
    cerr << mx::type_utils::type_name(error) << ": " << error.what() << "\n";
    return 1;
  }
}

#endif // MX_LIB_PROGRAM_H_
