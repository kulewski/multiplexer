// The subcommand framework of mxcontrol: Task is one subcommand with its
// boost::program_options declarations; tasks_holder.h registers them by
// name and driver.cc dispatches. Test roles (tests/roles/cc) reuse it.
#ifndef MX_MXCONTROL_TASK_H_
#define MX_MXCONTROL_TASK_H_

#include "lib/logging/logging.h"
#include "multiplexer/client.h"
#include <boost/asio/io_service.hpp>
#include <boost/bind/bind.hpp>
#include <boost/function.hpp>
#include <boost/program_options.hpp>
#include <boost/scoped_ptr.hpp>
#include <ostream>
#include <vector>

namespace mxcontrol {

namespace po = boost::program_options;

class TasksHolder;

// One subcommand. Subclass, override run() and the _initialize_*
// hooks that declare options, and register with
// REGISTER_MXCONTROL_SUBCOMMAND (tasks_holder.h). Options are declared
// lazily, the first time they are needed, so a subcommand that never runs
// costs nothing at startup.
class Task {
public:
  virtual ~Task() {}

  /*
   * call parse_options before calling run()
   */
  virtual void parse_options(std::vector<std::string> &args);

  /*
   * do actual work
   */
  virtual int run() = 0;

  /*
   * describe itself
   */
  virtual std::string short_description() const { return ""; }
  virtual std::string short_synopsis(const std::string &commandname) { return "<" + commandname + "-options>"; }

  /*
   * used by help when printing subcommand extended info
   */
  virtual void print_help(std::ostream &);

protected:
  // Override to declare options: visible ones, hidden ones (not in help),
  // and which options positional arguments fill.
  virtual void _initialize_options_description(po::options_description &) {}
  virtual void _initialize_hidden_options_description(po::options_description &) {}
  virtual void _initialize_positional_options_description(po::positional_options_description &) {}

  /*
   * For subcommands that are themselves peers: adds the repeatable
   * --multiplexer option, and _multiplexer_client() then returns a Client
   * connected to every address given.
   */
  void _add_multiplexer_client_options(po::options_description &);

  // The declared options, built on first use through the hooks above.
  inline po::options_description &_options_description() {
    return _generic_create_options_description(
        options_description_, "Options",
        boost::bind(&Task::_initialize_options_description, this, boost::placeholders::_1));
  }
  inline po::options_description &_hidden_options_description() {
    return _generic_create_options_description(
        hidden_options_description_, "Options",
        boost::bind(&Task::_initialize_hidden_options_description, this, boost::placeholders::_1));
  }
  inline po::positional_options_description _positional_options_description() {
    return _generic_create_options_description(
        positional_options_description_, "",
        boost::bind(&Task::_initialize_positional_options_description, this, boost::placeholders::_1));
  }

  // The shared client, connected to every --multiplexer on first use.
  inline multiplexer::Client &_multiplexer_client(boost::uint32_t default_peer_type) {
    if (!multiplexer_client_) {
      __create_multiplexer_client(default_peer_type);
    }
    return *multiplexer_client_;
  }

  // The io_service the shared client runs on, created on first use.
  inline boost::asio::io_service &io_service() {
    if (!io_service_) {
      io_service_.reset(new boost::asio::io_service());
    }
    return *io_service_;
  }

private:
  template <typename ValueType> inline ValueType *__construct(ValueType *, const std::string &param) const throw() {
    return new ValueType(param);
  }
  inline po::positional_options_description *__construct(po::positional_options_description *,
                                                         const std::string &) const throw() {
    return new po::positional_options_description();
  }

  void __create_multiplexer_client(boost::uint32_t peer_type);

  /*
   * _generic_create_options_description
   *	If `opptr' is a NULL ptr, create new ValueType passing name as a ctor
   *argument (maybe changed by overloading __construct()). If new value is
   *created, it's initialized using `initializer', which should be of type
   *`void(ValueType&)'.
   */
  template <typename ValueType, typename InitializerFunction>
  inline ValueType &_generic_create_options_description(boost::scoped_ptr<ValueType> &opptr, const std::string &name,
                                                        InitializerFunction initializer) const {
    if (!opptr) {
      opptr.reset(__construct((ValueType *)NULL, name));
      initializer(*opptr);
    }
    return *opptr;
  }

private:
  boost::scoped_ptr<po::options_description> options_description_;
  boost::scoped_ptr<po::options_description> hidden_options_description_;
  boost::scoped_ptr<po::positional_options_description> positional_options_description_;

  boost::shared_ptr<boost::asio::io_service> io_service_;
  boost::scoped_ptr<multiplexer::Client> multiplexer_client_;

  std::vector<std::string> multiplexers_;
}; // class Task
}; // namespace mxcontrol

#endif // MX_MXCONTROL_TASK_H_
