// The help subcommand: lists commands, or prints one command's options.
#ifndef MX_MXCONTROL_HELP_H_
#define MX_MXCONTROL_HELP_H_

#include "mxcontrol/task.h"

namespace mxcontrol {

class Help : public Task {
public:
  virtual int run();
  virtual std::string short_description() const { return "get some help"; }
  virtual std::string short_synopsis(const std::string &) { return "[subcommand]"; }
  virtual void print_help(std::ostream &out) {
    out << "Get a list of available commands.\n";
    out << "If subcommand is given, get subcommand options and usage "
           "information insted.\n";
  }

protected:
  virtual void _initialize_options(mx::options::Options &options) {
    options.add("_subcommand_", &subcommand_, "").hidden().positional("_subcommand_");
  }

private:
  inline std::string _program_name() const {
    return tasks_holder().original_argc() ? tasks_holder().original_argv()[0] : "program";
  }

private:
  std::string subcommand_;
};
}; // namespace mxcontrol

#endif // MX_MXCONTROL_HELP_H_
