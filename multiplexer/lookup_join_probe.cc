// lookup_join_probe.so, preloaded into a child Python by
// sync_client_destroy_test.py: whether a SyncClient's destruction holds
// the GIL while it waits for a name lookup of its. getaddrinfo() of
// lookup-under-way.test holds its lookup, on asio's resolver thread, until
// that thread is being joined, which the client's io_service does as it
// ends; then, the joining thread waiting in pthread_join(), the lookup
// takes the GIL, which it gets only if the joining thread let go of it,
// says so in gil_free_while_joined, and fails. A destruction that holds the
// GIL waits there for good, the lookup waiting for the GIL: the test sees
// its child never end. Every other name, and every other join, goes to the
// C library as it is, found with dlsym(RTLD_NEXT); the Python API is found
// in the interpreter that loaded the probe.
#include <dlfcn.h>
#include <netdb.h>
#include <pthread.h>

#include <cstring>

namespace {

typedef int (*LookupFunction)(const char*, const char*, const struct addrinfo*, struct addrinfo**);
typedef int (*JoinFunction)(pthread_t, void**);
typedef int (*EnsureFunction)();
typedef void (*ReleaseFunction)(int);

const char NAME[] = "lookup-under-way.test";

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t changed = PTHREAD_COND_INITIALIZER;
bool lookup_held = false;  // a lookup of NAME waits in getaddrinfo()
pthread_t lookup_thread;   // the thread it waits on
bool joined = false;       // that thread is being joined

}  // namespace

extern "C" {

// 1 once the held lookup took the GIL while its thread was being joined.
int gil_free_while_joined = 0;

// Waits until a lookup of NAME is held: for the test, before it destroys
// the client. Called through ctypes, which lets go of the GIL meanwhile.
void wait_for_lookup() {
  pthread_mutex_lock(&mutex);
  while (!lookup_held) {
    pthread_cond_wait(&changed, &mutex);
  }
  pthread_mutex_unlock(&mutex);
}

// NAME held until its thread is being joined, then the GIL taken and the
// lookup failed; any other name the C library's.
int getaddrinfo(const char* node, const char* service, const struct addrinfo* hints, struct addrinfo** result) {
  if (!node || std::strcmp(node, NAME) != 0) {
    static const LookupFunction system_lookup = reinterpret_cast<LookupFunction>(dlsym(RTLD_NEXT, "getaddrinfo"));
    return system_lookup(node, service, hints, result);
  }
  pthread_mutex_lock(&mutex);
  lookup_thread = pthread_self();
  lookup_held = true;
  pthread_cond_broadcast(&changed);
  while (!joined) {
    pthread_cond_wait(&changed, &mutex);
  }
  pthread_mutex_unlock(&mutex);
  const EnsureFunction ensure = reinterpret_cast<EnsureFunction>(dlsym(RTLD_DEFAULT, "PyGILState_Ensure"));
  const ReleaseFunction release = reinterpret_cast<ReleaseFunction>(dlsym(RTLD_DEFAULT, "PyGILState_Release"));
  const int state = ensure();
  gil_free_while_joined = 1;
  release(state);
  *result = nullptr;
  return EAI_NONAME;
}

// The C library's join, after saying when the held lookup's thread is the
// one joined.
int pthread_join(pthread_t thread, void** value) {
  static const JoinFunction system_join = reinterpret_cast<JoinFunction>(dlsym(RTLD_NEXT, "pthread_join"));
  pthread_mutex_lock(&mutex);
  if (lookup_held && pthread_equal(thread, lookup_thread)) {
    joined = true;
    pthread_cond_broadcast(&changed);
  }
  pthread_mutex_unlock(&mutex);
  return system_join(thread, value);
}

}  // extern "C"
