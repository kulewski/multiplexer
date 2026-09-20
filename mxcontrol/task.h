// The subcommand framework of mxcontrol: Task is one subcommand with its
// options (lib/options.h); tasks_holder.h registers them by name and
// driver.cc dispatches. Test roles (tests/roles/cc) reuse it.
#ifndef MX_MXCONTROL_TASK_H_
#define MX_MXCONTROL_TASK_H_

#include <asio/io_service.hpp>
#include <functional>
#include <memory>
#include <ostream>
#include <vector>

#include "lib/logging/logging.h"
#include "lib/options.h"
#include "multiplexer/client.h"

namespace mxcontrol {

class TasksHolder;

// One subcommand. Subclass, override run() and _initialize_options(),
// which declares the options, and register with
// REGISTER_MXCONTROL_SUBCOMMAND (tasks_holder.h). Options are declared
// lazily, the first time they are needed, so a subcommand that never runs
// costs nothing at startup.
class Task {
 public:
  virtual ~Task() {}

  /*
   * call parse_options before calling run()
   */
  virtual void parse_options(std::vector<std::string>& args);

  /*
   * do actual work
   */
  virtual int run() = 0;

  /*
   * describe itself
   */
  virtual std::string short_description() const { return ""; }
  virtual std::string short_synopsis(const std::string& commandname) { return "<" + commandname + "-options>"; }

  /*
   * used by help when printing subcommand extended info
   */
  virtual void print_help(std::ostream&);

 protected:
  // Override to declare options: add(), add_switch(), and for the ones
  // positional arguments fill, positional(); hidden() keeps one out of
  // the help.
  virtual void _initialize_options(mx::options::Options&) {}

  /*
   * For subcommands that are themselves peers: adds the repeatable
   * --multiplexer option, and _multiplexer_client() then returns a Client
   * connected to every address given.
   */
  void _add_multiplexer_client_options(mx::options::Options&);

  // The declared options, built on first use through the hook above.
  inline mx::options::Options& _options() {
    if (!options_) {
      options_.reset(new mx::options::Options("Options"));
      _initialize_options(*options_);
    }
    return *options_;
  }

  // The shared client, connected to every --multiplexer on first use.
  inline multiplexer::Client& _multiplexer_client(std::uint32_t default_peer_type) {
    if (!multiplexer_client_) {
      __create_multiplexer_client(default_peer_type);
    }
    return *multiplexer_client_;
  }

  // The io_service the shared client runs on, created on first use.
  inline asio::io_service& io_service() {
    if (!io_service_) {
      io_service_.reset(new asio::io_service());
    }
    return *io_service_;
  }

 private:
  void __create_multiplexer_client(std::uint32_t peer_type);

 private:
  std::unique_ptr<mx::options::Options> options_;

  std::shared_ptr<asio::io_service> io_service_;
  std::unique_ptr<multiplexer::Client> multiplexer_client_;

  std::vector<std::string> multiplexers_;
};  // class Task
};  // namespace mxcontrol

#endif  // MX_MXCONTROL_TASK_H_
