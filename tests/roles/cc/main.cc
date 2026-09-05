// mxtestroles <role> [options]: the C++ counterparts of tests/roles/py, as
// subcommands of one mxcontrol-style binary (one file per role: backend.cc, client.cc, event_client.cc,
// event_backend.cc; common.h is what they share).
#include "lib/program.h" /* main() */
#include "mxcontrol/driver.h"

int MxMain(int argc, char **argv) { return mxcontrol::run_tasks(argc, argv); }
