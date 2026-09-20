// run_tasks(): the main loop of an mxcontrol-style binary. Parses the general
// options (--help, --logging-fd, --logging-file), treats the first
// unrecognized word as the subcommand, sets up logging, and runs it.
#include "mxcontrol/driver.h"

#include <cstdlib>
#include <deque>
#include <iostream>
#include <map>
#include <vector>

#include "lib/assertion.h"
#include "lib/logging/logging.h"
#include "lib/options.h"
#include "mxcontrol/tasks_holder.h"

using namespace std;

namespace mxcontrol {

int run_tasks(int argc, char** argv) {
  Assert(argc > 0);
  std::vector<std::string> args(argv + 1, argv + argc);

  // TasksHolder tasks_holder;
  tasks_holder().set_original_args(argc, argv);

  // build program options
  bool show_help;
  std::uint16_t logging_fd = 0;
  std::string logging_file;
  std::string verbosity_name;
  mx::options::Options& general = tasks_holder().general_options;
  general.add_switch("help", &show_help, "produce help message");
  general.add("logging-fd", &logging_fd, "descriptor, to which binary logging stream is sent");
  general.add("logging-file", &logging_file, "binary logging stream file (if --logging-fd not set)");
  general.add("verbosity", &verbosity_name, "MEDIUMVERBOSITY",
              "most verbose DEBUG entries emitted: ZEROVERBOSITY, LOWVERBOSITY, MEDIUMVERBOSITY, HIGHVERBOSITY or "
              "CHATTERBOX");

  // parse the commandline: what is not a general option is the subcommand
  // and its own options
  std::vector<std::string> unrecognized_;
  try {
    unrecognized_ = general.parse(args, true);
  } catch (const mx::options::Error& error) {
    cerr << argv[0] << ": " << error.what() << "\n";
    return EXIT_FAILURE;
  }
  std::deque<std::string> unrecognized(unrecognized_.begin(), unrecognized_.end());
  std::vector<std::string>().swap(unrecognized_);  // free all memory

  // see what're the results of parsing
  if (!unrecognized.size()) {
    unrecognized.push_back("help");
    show_help = false;
  }
  if (show_help /* && unrecognized[0] != "help"*/) {
    unrecognized.push_front("help");
    show_help = false;
  }

  if (!tasks_holder().is_command(unrecognized[0])) {
    cerr << argv[0] << ": unknown command: " << unrecognized[0] << "\n";

    // print what commands are available
    if (tasks_holder().tasks().size()) {
      cerr << "Available commands:\n";
      for (const TasksHolder::TasksMap::value_type& te : tasks_holder().tasks()) {
        cerr << "\t" << te.first << "\n";
      }
    } else {
      cerr << "There are no available commands.\n";
    }

    return EXIT_FAILURE;
  }

  // initialize env
  if (general.given("logging-fd")) {
    mx::logging::set_logging_fd(logging_fd);
  } else if (general.given("logging-file")) {
    mx::logging::set_logging_file(logging_file);
  }

  // The cap on DEBUG entries; the other levels are always emitted. The
  // option's default gives way to MX_LOG_VERBOSITY, applied when the
  // library loaded; an explicit --verbosity wins over both.
  bool verbosity_known = false;
  const char* from_environment = getenv(mx::logging::VERBOSITY_ENVIRONMENT_VARIABLE);
  for (unsigned int verbosity = 0; verbosity <= mx::logging::consts::MAX_VERBOSITY; ++verbosity) {
    if (verbosity_name == mx::logging::consts::logging_get_verbosity_name(verbosity)) {
      if (general.given("verbosity") || !from_environment || !*from_environment) {
        mx::logging::set_maximal_logging_verbosity(DEBUG, verbosity);
      }
      verbosity_known = true;
    }
  }
  if (!verbosity_known) {
    cerr << argv[0] << ": unknown --verbosity " << verbosity_name << "\n";
    return EXIT_FAILURE;
  }

  // run the command
  return tasks_holder().run(unrecognized);
}

}  // namespace mxcontrol
