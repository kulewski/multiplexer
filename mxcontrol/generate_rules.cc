// generate_rules: the system rules, the peer and message types the
// multiplexer, the libraries and mxcontrol use themselves, written as a new
// rules file for a deployment to add its own types to: `mxcontrol
// generate_rules multiplexer.rules`. The text is this repository's
// multiplexer.rules, compiled in by embed_rules.sh, so every mxcontrol
// writes the rules it was built with. A rules file is edited after it is
// written, so an existing one is never replaced; `-` writes stdout.
#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>

#include "mxcontrol/task.h"
#include "mxcontrol/tasks_holder.h"

namespace mxcontrol {

extern const char kSystemRules[];

class GenerateRules : public Task {
 public:
  virtual std::string short_description() const {
    return "write the system rules, which every rules file starts from, to a new file";
  }
  virtual std::string short_synopsis(const std::string& commandname) { return "<" + commandname + "-options> FILE"; }
  virtual void print_help(std::ostream& out) {
    out << "Writes the system rules to FILE: the peer and message types the multiplexer, the\n"
           "libraries and mxcontrol use themselves, which every rules file starts from. Add the\n"
           "deployment's own peer and message types after them (docs/rules.md). FILE must not\n"
           "exist: a rules file is edited after it is written, so an existing one is never\n"
           "replaced. With FILE -, the rules go to stdout.\n";
  }
  virtual int run();

 protected:
  virtual void _initialize_options(mx::options::Options& options) {
    options.add("file", &file_, "the rules file to create, or - for stdout").positional("file").required();
  }

 private:
  std::string file_;
};

REGISTER_MXCONTROL_SUBCOMMAND(generate_rules, GenerateRules);

namespace {

// Writes all `size` bytes at `data` to `fd`; false on an error, with errno set.
bool write_all(int fd, const char* data, std::size_t size) {
  while (size > 0) {
    const ssize_t written = ::write(fd, data, size);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    data += written;
    size -= static_cast<std::size_t>(written);
  }
  return true;
}

}  // namespace

// Writes the rules to stdout, or creates FILE with them and prints its path,
// the way generate_constants prints what it wrote.
int GenerateRules::run() {
  const std::size_t size = std::strlen(kSystemRules);
  if (file_ == "-") {
    std::cout.write(kSystemRules, static_cast<std::streamsize>(size)).flush();
    return std::cout ? 0 : 1;
  }
  // O_EXCL: the file is created here or not at all, so one that holds a
  // deployment's own types is never replaced.
  const int fd = ::open(file_.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
  if (fd < 0) {
    const int error = errno;
    std::cerr << "generate_rules: cannot create " << file_ << ": " << std::strerror(error)
              << (error == EEXIST ? "; an existing rules file is never replaced" : "") << "\n";
    return 1;
  }
  bool written = write_all(fd, kSystemRules, size);
  int error = errno;
  if (::close(fd) != 0 && written) {
    written = false;
    error = errno;
  }
  if (!written) {
    std::cerr << "generate_rules: cannot write " << file_ << ": " << std::strerror(error) << "\n";
    ::unlink(file_.c_str());  // created above; a partial one would still read as a rules file
    return 1;
  }
  std::cout << file_ << "\n";
  return 0;
}

}  // namespace mxcontrol
