// generate_constants: the constants of a rules file, for a program built
// outside Bazel, from a release: `mxcontrol generate_constants RULES
// --python multiplexer_constants.py --pyi multiplexer_constants.pyi --cxx
// multiplexer.constants.h`, any one or more of the three. The same writer
// the build-time tool uses (multiplexer/constants_writer.h), so the files
// are the ones a Bazel build would have produced.
#include <iostream>
#include <string>

#include "multiplexer/config.h"
#include "multiplexer/constants_writer.h"
#include "mxcontrol/task.h"
#include "mxcontrol/tasks_holder.h"

namespace mxcontrol {

class GenerateConstants : public Task {
 public:
  virtual std::string short_description() const {
    return "write the constants of a rules file for Python, its type stub, or C++";
  }
  virtual std::string short_synopsis(const std::string& commandname) {
    return "<" + commandname + "-options> RULES [--python FILE] [--pyi FILE] [--cxx FILE]";
  }
  virtual void print_help(std::ostream& out) {
    out << "Reads a rules file and writes its peer and message types as constants: a Python module\n"
           "(classes peers and types), the module's stub for type checkers, and a C++ header\n"
           "(namespaces multiplexer::peers and multiplexer::types), whichever of the three are asked\n"
           "for. A rules file where a name or a number repeats is refused. This is what a Bazel build\n"
           "does for you; a program installed with pip or from a release runs it once per rules file.\n"
        << "\n"
        << _options();
  }
  virtual int run();

 protected:
  virtual void _initialize_options(mx::options::Options& options) {
    options.add("rules", &rules_, "the rules file, docs/rules.md").positional("rules").required();
    options.add("python", &python_, "write the Python module here, multiplexer_constants.py by convention");
    options.add("pyi", &pyi_, "write the Python module's type stub here, next to the module");
    options.add("cxx", &cxx_, "write the C++ header here, multiplexer/multiplexer.constants.h on the include path");
  }

 private:
  std::string rules_;
  std::string python_;
  std::string pyi_;
  std::string cxx_;
};

REGISTER_MXCONTROL_SUBCOMMAND(generate_constants, GenerateConstants);

int GenerateConstants::run() {
  if (python_.empty() && pyi_.empty() && cxx_.empty()) {
    std::cerr << "generate_constants: nothing to write; give --python, --pyi or --cxx\n";
    return 2;
  }
  const std::string text = multiplexer::constants::read_rules(rules_);  // once: a pipe gives it once
  const multiplexer::Config<std::multimap> config = multiplexer::Config<std::multimap>::from_text(text, rules_);
  multiplexer::constants::check_config(config);
  const std::string fingerprint = mx::fingerprint(text);
  for (const std::string& path : {python_, pyi_, cxx_}) {
    if (!path.empty()) {
      multiplexer::constants::write_constants(config, rules_, fingerprint, path);
      std::cout << path << "\n";
    }
  }
  return 0;
}

}  // namespace mxcontrol
