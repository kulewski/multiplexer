// TasksHolder: registration (from static initializers, hence no logging
// and abort() on a duplicate) and running a task by name.

#include "mxcontrol/tasks_holder.h"
#include "lib/logging/logging.h"
#include <iostream>

using namespace mx::logging;

namespace mxcontrol {

void TasksHolder::register_(const std::string &name, TaskProxy *task_proxy) throw() {
  if (named_tasks_.count(name) != 0) {
    // TODO(findepi) we can't call die() here, this involces logging and
    // logging module might have been not initialized yet -- we are
    // called from static initializers
    // die("Task '" + name + "' already registered");
    std::cerr << "Error: Task '" << name << "' already registered " << named_tasks_.count(name) << " time(s).\n";
    abort();
  }
  named_tasks_.insert(std::make_pair(name, task_proxy));
}

int TasksHolder::__run(TasksMap::iterator ti, std::vector<std::string> &args) {
  boost::shared_ptr<Task> task = ti->second->task();
  Assert(task);
  task->parse_options(args);
  return task->run();
}

namespace tasks_holder_detail {

TasksHolder &tasks_holder() {
  static TasksHolder *tasks_holder_ = NULL;
  if (!tasks_holder_) {
    tasks_holder_ = new TasksHolder();
  }
  return *tasks_holder_;
}
}; // namespace tasks_holder_detail
}; // namespace mxcontrol
