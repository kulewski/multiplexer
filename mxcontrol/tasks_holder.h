// The subcommand registry; see TasksHolder and REGISTER_MXCONTROL_SUBCOMMAND.
#ifndef MX_MXCONTROL_TASKS_HOLDER_H_
#define MX_MXCONTROL_TASKS_HOLDER_H_

#include "lib/assertion.h"
#include "lib/initialization.h"
#include "mxcontrol/task.h"
#include <iostream>
#include <string>

namespace mxcontrol {

// The registry of subcommands, filled at static initialization by
// REGISTER_MXCONTROL_SUBCOMMAND in each subcommand's .cc file. Tasks are
// held as factories (TaskProxy) and constructed only when run, so
// registering is cheap and order-independent.
class TasksHolder {

public:
  /*
   * class representing a Task without instantiating it
   */
  struct TaskProxy {
    virtual ~TaskProxy() {}

    // get the Task pointer by TaskProxy
    virtual boost::shared_ptr<Task> operator()() = 0;

    // alias
    inline boost::shared_ptr<Task> task() { return (*this)(); }
  };

  TasksHolder() : original_argc_(0), original_argv_(NULL), general_options("General options") {}

  typedef std::map<std::string, boost::shared_ptr<TaskProxy>> TasksMap;

  // takes ownership
  void register_(const std::string &name, TaskProxy *task_proxy) throw();

  bool inline is_command(const std::string &name) const { return named_tasks_.count(name); }
  const TasksMap &tasks() const { return named_tasks_; }

  template <typename ArgsVector> int run(ArgsVector &args) {
    using namespace mx;

    TasksMap::iterator ti = named_tasks_.find(args.front());
    if (named_tasks_.end() == ti) {
      std::cerr << "ERROR: unknown command: " << args.front() << "\n";
      return 1;
    }
    logging::set_process_context(logging::process_context() + "." + args.front());
    args.pop_front();

    std::vector<std::string> args_copy(args.begin(), args.end());
    return __run(ti, args_copy);
  }

  TasksHolder &set_original_args(int argc, const char *const *argv) {
    original_argc_ = argc;
    original_argv_ = argv;
    return *this;
  }

  int original_argc() { return original_argc_; }
  const char *const *original_argv() { return original_argv_; }

private:
  int __run(TasksMap::iterator, std::vector<std::string> &args);

private:
  TasksMap named_tasks_;
  int original_argc_;
  const char *const *original_argv_;

public:
  boost::program_options::options_description general_options;
};

namespace tasks_holder_detail {
// TasksHolder singleton
TasksHolder &tasks_holder();

template <typename subcommand> struct TaskProxyImpl : TasksHolder::TaskProxy {
  virtual boost::shared_ptr<Task> operator()() { return boost::shared_ptr<subcommand>(new subcommand()); }
};

}; // namespace tasks_holder_detail

using tasks_holder_detail::tasks_holder;

// Put at file scope in the subcommand's .cc: `name` becomes the word on the
// command line. Linking the .cc into a binary is what makes the subcommand
// exist, so the binary's deps decide its command set.
#define REGISTER_MXCONTROL_SUBCOMMAND(name, subcommand)                                                                \
  MX_TRIGGER_STATIC_INITIALIZATION_CODE(                                                                               \
      (::mxcontrol::tasks_holder_detail::tasks_holder().register_(                                                     \
           BOOST_PP_STRINGIZE(name), new ::mxcontrol::tasks_holder_detail::TaskProxyImpl<subcommand>());),             \
       true);

}; // namespace mxcontrol

#endif // MX_MXCONTROL_TASKS_HOLDER_H_
