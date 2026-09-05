// Version strings attached to every log entry. Placeholders; a release
// process may overwrite release.cc.
#ifndef MX_LIB_RELEASE_H_
#define MX_LIB_RELEASE_H_

namespace mx {
namespace release {
extern const char *const version;
extern const char *const version_hash;
extern const char *const version_short_hash;
} // namespace release
} // namespace mx

#endif // MX_LIB_RELEASE_H_
