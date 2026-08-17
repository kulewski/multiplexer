// run_tasks(): the entry point of an mxcontrol-style binary; see below.
#ifndef MX_MXCONTROL_DRIVER_H_
#define MX_MXCONTROL_DRIVER_H_

namespace mxcontrol {

// Parses the general options, picks the subcommand named on the command line
// and runs it. Shared by mxcontrol and by other binaries that register their
// own subcommands with REGISTER_MXCONTROL_SUBCOMMAND.
int run_tasks(int argc, char **argv);

} // namespace mxcontrol

#endif // MX_MXCONTROL_DRIVER_H_
