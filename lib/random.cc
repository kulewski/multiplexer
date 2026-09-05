// Seeding: 8 bytes from /dev/urandom, falling back to the pid if the read
// comes up short.
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>

#include "lib/random.h"

using namespace mx;

AutoSeedingRand48::AutoSeedingRand48() {
  std::ifstream rf("/dev/urandom", std::ifstream::binary | std::ifstream::in);
  boost::uint64_t seed = getpid();
  rf.read((char *)&seed, sizeof(seed));
  this->seed(seed);
}
