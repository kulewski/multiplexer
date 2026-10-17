// ThreadedClientInbox's hand-overs and takes; threaded_client_inbox.h
// describes it, threaded_client_plain.cc its two ends in the Core.
#include "multiplexer/threaded_client_inbox.h"

#include <string>
#include <utility>
#include <vector>

namespace multiplexer {

bool ThreadedClientInbox::_wake() {
  if (wake_pending_) {
    return false;
  }
  wake_pending_ = true;
  return true;
}

bool ThreadedClientInbox::push(std::function<void()> call) {
  mx::MutexLock lock(mutex_);
  entries_.emplace_back();
  entries_.back().call = std::move(call);
  return _wake();
}

bool ThreadedClientInbox::push_plain(PlainSend* plain, std::string* body) {
  mx::MutexLock lock(mutex_);
  entries_.emplace_back();
  PlainSend& entry = entries_.back().plain;
  std::swap(entry, *plain);
  entry.body.swap(*body);
  returned_.take(body);  // the next body, when one was handed back
  return _wake();
}

bool ThreadedClientInbox::push_confirmation(const HandedConfirmation& confirmation) {
  mx::MutexLock lock(mutex_);
  confirmations_.push_back(confirmation);
  return _wake();
}

void ThreadedClientInbox::take(std::vector<Entry>* taken, std::vector<HandedConfirmation>* confirmations,
                               SpentBodies* spent) {
  mx::MutexLock lock(mutex_);
  taken->swap(entries_);
  confirmations->swap(confirmations_);
  wake_pending_ = false;
  returned_.keep_all(spent);
}

std::size_t ThreadedClientInbox::returned_count() {
  mx::MutexLock lock(mutex_);
  return returned_.count();
}

std::size_t ThreadedClientInbox::returned_bytes() {
  mx::MutexLock lock(mutex_);
  return returned_.bytes();
}

}  // namespace multiplexer
