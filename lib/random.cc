// Seeding: 8 bytes from /dev/urandom, mixed with the pid and the time in
// case the read comes up short.
#include "lib/random.h"

#include <unistd.h>

#include <chrono>
#include <fstream>

using namespace mx;

Random64::Random64() {
  std::ifstream urandom("/dev/urandom", std::ifstream::binary | std::ifstream::in);
  std::uint64_t seed = 0;
  urandom.read(reinterpret_cast<char*>(&seed), sizeof(seed));
  seed ^= static_cast<std::uint64_t>(getpid()) << 32;
  seed ^= static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
  engine_.seed(seed);
}
