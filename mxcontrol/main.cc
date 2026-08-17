// The mxcontrol binary: enable core dumps, then dispatch to the subcommand.
// The subcommands themselves are linked in through the BUILD deps.
#include "lib/core_dump.h"
#include "lib/program.h" /* main() */
#include "mxcontrol/driver.h"

int MxMain(int argc, char **argv) {
  mx::enable_core_dump();
  return mxcontrol::run_tasks(argc, argv);
}
