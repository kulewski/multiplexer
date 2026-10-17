// SharedOutgoing: the outgoing side of a connection that several event
// loops write to, a multiplexer's on several loops (Server::set_threads):
// the queue, the channel's state, the queue's limit, who writes and from
// where, and what the connection's own loop is owed, under a lock of their
// own that every loop takes, the connection's own included. A connection
// of one loop, a client's or a multiplexer's on one loop, has none: its
// outgoing side is its thread's (Connection, connection.h).
//
// Writing through. A loop that routes a frame to the connection queues it
// here (enqueue()). When nothing was being written it takes the writing
// on, and once it let go of the routing lock it writes the queue's front
// itself, frame after frame, each with one non-blocking sendmsg(2) and no
// lock held (write_through()); what other loops queue meanwhile goes out
// the same way, so a frame routed to a peer on another loop costs that
// loop no wake-up. A write that would block, writes part of a frame or
// fails, and the frame after WRITE_THROUGH_FRAMES of them, leave the rest
// to the connection's own loop, which goes on from where the write
// stopped with asio's async_write, as at one loop, until the queue is
// empty (owner_next(), owner_written()); a failed write fails there, as at
// one loop. One writer at a time, always at the queue's front, so frames
// go out in the order they were queued.
//
// The connection's own loop ends the channel (take()): the queue is taken,
// and the frame a write had under way, whose bytes may be in the kernel
// already, is settled by that write's end, written whole or lost, as one
// loop's set-aside frame is. What a writer finds owed when it leaves the
// queue empty, the connection's next heartbeat or its close once flushed,
// it hands to the connection's own loop (Ending). A close once flushed
// ends the channel and, in the same step, the connection's life as the
// routing reads it (the connection's living flag, start()): from then on
// every loop takes the connection for gone, as one loop does once it
// closes, and a frame routed to it meanwhile is refused as ENDED, never
// as a full queue.
//
// Header-only: a template over the queue entry, as Connection is.
#ifndef MX_MULTIPLEXER_IO_SHARED_OUTGOING_H_
#define MX_MULTIPLEXER_IO_SHARED_OUTGOING_H_

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iterator>
#include <thread>

#include "lib/adaptive_mutex.h"
#include "lib/thread_annotations.h"
#include "multiplexer/defaults.h"
#include "multiplexer/io/raw_message.h"

namespace multiplexer {

// One non-blocking sendmsg(2) of what is left of `frame` past `offset`
// bytes, its header and its body, to `descriptor`: the bytes written, or
// -1 with errno set, EAGAIN when the socket has no room.
inline ssize_t send_rest(int descriptor, const RawMessage& frame, std::size_t offset) {
  struct iovec parts[2];
  int count = 0;
  for (const asio::const_buffer& buffer : frame.get_message_buffer()) {
    if (count == 2) {
      break;
    }
    if (offset >= buffer.size()) {
      offset -= buffer.size();
      continue;
    }
    parts[count].iov_base = const_cast<char*>(static_cast<const char*>(buffer.data()) + offset);
    parts[count].iov_len = buffer.size() - offset;
    offset = 0;
    ++count;
  }
  struct msghdr message;
  std::memset(&message, 0, sizeof message);
  message.msg_iov = parts;
  message.msg_iovlen = count;
  ssize_t sent;
  do {
    sent = ::sendmsg(descriptor, &message, MSG_DONTWAIT | MSG_NOSIGNAL);
  } while (sent < 0 && errno == EINTR);
  return sent;
}

// `Entry` is a queue entry, `ToRaw` the connection's converter from an
// entry to its frame (ConnectionsManagerTraits::MessagesBufferTraits).
template <typename Entry, typename ToRaw>
class alignas(64) SharedOutgoing {  // its lock on a cache line of its own
 public:
  typedef std::deque<Entry> Queue;
  // A writer from another loop leaves the rest to the connection's own
  // loop after this many frames: that thread has its own peers to read.
  static constexpr unsigned int WRITE_THROUGH_FRAMES = 64;

  // What enqueue() did: refused the frame, the queue full; queued it;
  // queued it and gave the caller the writing; or refused it, the channel
  // ended, the connection gone or closing.
  enum class Queued { FULL, QUEUED, CLAIMED, ENDED };

  // Why a writer, or the connection's own loop, stopped writing, and what
  // it hands on. IDLE: the queue is empty. HEARTBEAT: the same, and the
  // connection's loop is owed a call of its heartbeat's (it asked while
  // the queue was not empty, quiet()). CLOSE: the queue is empty and the
  // close was owed (close_when_flushed()): the channel is ended, and the
  // connection closes. HAND_OVER: the connection's own loop goes on
  // (owner_next()). WRITTEN and LOST: the channel was ended while this
  // write was under way, whose frame the connection set aside: written
  // whole, or not. GONE: the channel was ended, and nothing is this
  // writer's to settle. MORE (the own loop's alone): the next frame.
  // FAILED (the own loop's alone): the write failed, and so does the
  // connection's writing.
  enum class Ending { IDLE, HEARTBEAT, CLOSE, HAND_OVER, WRITTEN, LOST, GONE, MORE, FAILED };

  SharedOutgoing() = default;
  SharedOutgoing(const SharedOutgoing&) = delete;
  SharedOutgoing& operator=(const SharedOutgoing&) = delete;

  // The socket the writes through go to, and the connection's living
  // flag, which a close once flushed clears as it ends the channel; by
  // the connection's own loop, before anything is queued.
  void start(int descriptor, std::atomic<bool>* living) MX_EXCLUDES(mutex_) {
    mx::AdaptiveMutexLock lock(mutex_);
    descriptor_ = descriptor;
    living_ = living;
  }

  // Queues `entry` from any thread, at the back or, `asap`, right behind
  // the frame being written. Refused, ENDED, when the channel ended, and,
  // FULL, when the queue holds its limit, `force` letting a protocol frame
  // past a full queue by FORCED_FRAMES_PAST_FULL_QUEUE frames. CLAIMED
  // when nothing was being written: `self` writes, with write_through(),
  // once it holds no lock.
  Queued enqueue(Entry entry, bool force, bool asap, std::thread::id self) MX_EXCLUDES(mutex_) {
    mx::AdaptiveMutexLock lock(mutex_);
    if (state_ == State::BROKEN) {
      return Queued::ENDED;
    }
    if (limit_ <= queue_.size() && (!force || queue_.size() >= std::size_t(limit_) + FORCED_FRAMES_PAST_FULL_QUEUE)) {
      return Queued::FULL;
    }
    if (asap && !queue_.empty()) {
      queue_.insert(std::next(queue_.begin()), std::move(entry));
    } else if (asap) {
      queue_.push_front(std::move(entry));
    } else {
      queue_.push_back(std::move(entry));
    }
    if (state_ != State::FREE) {
      return Queued::QUEUED;
    }
    state_ = State::WRITING;
    writer_ = self;
    return Queued::CLAIMED;
  }

  // Whether the queue holds its limit, from any thread: the routing skips
  // a full peer for whom: ANY.
  bool full() const MX_EXCLUDES(mutex_) {
    mx::AdaptiveMutexLock lock(mutex_);
    return limit_ <= queue_.size();
  }
  // The queue's limit, from any thread.
  void set_limit(std::uint32_t limit) MX_EXCLUDES(mutex_) {
    mx::AdaptiveMutexLock lock(mutex_);
    limit_ = limit;
  }

  // The writing enqueue() gave `self`: the queue's front, each frame with
  // `send(descriptor, frame, offset)`, sendmsg(2) as send_rest() makes it,
  // called with no lock held, until the queue is empty, the connection's
  // own loop takes over, or the channel ended. Says why it stopped.
  template <typename Send>
  Ending write_through(std::thread::id self, Send send) MX_EXCLUDES(mutex_) {
    unsigned int frames = 0;
    Entry front;
    std::size_t offset = 0;
    int descriptor = -1;
    {
      mx::AdaptiveMutexLock lock(mutex_);
      const Ending ending = _next_through(self, frames, &front, &offset, &descriptor);
      if (ending != Ending::MORE) {
        return ending;
      }
    }
    for (;;) {
      const RawMessage& frame = *to_raw_(front);
      const std::size_t rest = frame.get_header_length() + frame.get_body_length() - offset;
      const ssize_t sent = send(descriptor, frame, offset);
      mx::AdaptiveMutexLock lock(mutex_);
      if (state_ != State::WRITING || writer_ != self) {
        return _settle_through(self, sent >= 0 && static_cast<std::size_t>(sent) == rest);
      }
      if (sent < 0 || static_cast<std::size_t>(sent) != rest) {
        // Would block, wrote part of the frame, or failed: the own loop
        // goes on from here, or fails as at one loop.
        if (sent > 0) {
          front_written_ += static_cast<std::size_t>(sent);
        }
        state_ = State::OWNER;
        writer_ = std::thread::id();
        return Ending::HAND_OVER;
      }
      queue_.pop_front();
      front_written_ = 0;
      const Ending ending = _next_through(self, ++frames, &front, &offset, &descriptor);
      if (ending != Ending::MORE) {
        return ending;
      }
    }
  }

  // The connection's own loop takes its turn, handed over: the queue's
  // front into `front` and where its write starts into `offset`, for
  // async_write, the write then under way; false when there is nothing to
  // write, the channel ended meanwhile.
  bool owner_next(Entry* front, std::size_t* offset) MX_EXCLUDES(mutex_) {
    mx::AdaptiveMutexLock lock(mutex_);
    if (state_ != State::OWNER) {
      return false;
    }
    if (queue_.empty()) {
      state_ = State::FREE;  // never so: the writer that handed over left a frame
      return false;
    }
    owner_writes_ = true;
    *front = queue_.front();
    *offset = front_written_;
    return true;
  }

  // The own loop's write of the front ended, `whole` when it wrote all that
  // was left of it: MORE, the next; IDLE or CLOSE, the queue empty;
  // FAILED; or, the channel ended meanwhile, WRITTEN, LOST or GONE.
  Ending owner_written(bool whole) MX_EXCLUDES(mutex_) {
    mx::AdaptiveMutexLock lock(mutex_);
    owner_writes_ = false;
    if (state_ != State::OWNER) {
      return _settle_owned(whole);
    }
    if (!whole) {
      return Ending::FAILED;
    }
    queue_.pop_front();
    front_written_ = 0;
    if (!queue_.empty()) {
      return Ending::MORE;
    }
    heartbeat_owed_ = false;  // the own loop calls the heartbeat's itself now
    return _emptied();
  }

  // The heartbeat's question, by the connection's own loop: true when the
  // queue is empty, stamped quiet since `now`; false when it is not, the
  // writer that empties it then owing the own loop the question again.
  bool quiet(std::chrono::steady_clock::time_point now) MX_EXCLUDES(mutex_) {
    mx::AdaptiveMutexLock lock(mutex_);
    if (!queue_.empty()) {
      heartbeat_owed_ = true;
      return false;
    }
    quiet_since_ = now;
    return true;
  }
  // When the queue last emptied, or quiet() last found it empty.
  std::chrono::steady_clock::time_point quiet_since() const MX_EXCLUDES(mutex_) {
    mx::AdaptiveMutexLock lock(mutex_);
    return quiet_since_;
  }

  // The connection closes once what is queued is written: true when the
  // queue is empty now, the channel and the connection's life ended in the
  // same step so that nothing routed later is lost to the close; else the
  // writer that empties it ends both then, its Ending CLOSE.
  bool close_when_flushed() MX_EXCLUDES(mutex_) {
    mx::AdaptiveMutexLock lock(mutex_);
    if (state_ == State::FREE) {
      _end_for_close();
      return true;
    }
    close_owed_ = true;
    return false;
  }

  // Ends the channel, by the connection's own loop, and takes what is
  // queued. `under_way` says whether a write had the front under way,
  // which that write settles when it ends (WRITTEN or LOST).
  Queue take(bool* under_way) MX_EXCLUDES(mutex_) {
    mx::AdaptiveMutexLock lock(mutex_);
    *under_way = !queue_.empty() && (state_ == State::WRITING || owner_writes_);
    settler_ = state_ == State::WRITING ? writer_ : std::thread::id();
    owner_settles_ = owner_writes_;
    state_ = State::BROKEN;
    writer_ = std::thread::id();
    front_written_ = 0;
    limit_ = 0;
    close_owed_ = false;
    heartbeat_owed_ = false;
    Queue taken;
    taken.swap(queue_);
    return taken;
  }

 private:
  // FREE: the queue is empty. WRITING: a thread writes through (writer_).
  // OWNER: the connection's own loop has the writing, handed over, its
  // async_write under way while owner_writes_. BROKEN: ended.
  enum class State { FREE, WRITING, OWNER, BROKEN };

  // The writer's next step, `frames` written so far: MORE with the front,
  // its offset and the socket; else why it stops.
  Ending _next_through(std::thread::id self, unsigned int frames, Entry* front, std::size_t* offset, int* descriptor)
      MX_REQUIRES(mutex_) {
    if (state_ != State::WRITING || writer_ != self) {
      return _settle_through(self, /*whole=*/false);  // ended before the write began
    }
    if (queue_.empty()) {
      return _emptied();
    }
    if (frames == WRITE_THROUGH_FRAMES) {
      state_ = State::OWNER;
      writer_ = std::thread::id();
      return Ending::HAND_OVER;
    }
    *front = queue_.front();
    *offset = front_written_;
    *descriptor = descriptor_;
    return Ending::MORE;
  }

  // The queue emptied: the channel free, or, a close owed, ended.
  Ending _emptied() MX_REQUIRES(mutex_) {
    writer_ = std::thread::id();
    if (close_owed_) {
      close_owed_ = false;
      _end_for_close();
      return Ending::CLOSE;
    }
    state_ = State::FREE;
    quiet_since_ = std::chrono::steady_clock::now();
    if (heartbeat_owed_) {
      heartbeat_owed_ = false;
      return Ending::HEARTBEAT;
    }
    return Ending::IDLE;
  }

  // The channel ended for a close once flushed, and with it the
  // connection's life for the routing of every loop, before its own loop
  // runs the close, which a writer from another loop posts there: routed
  // to meanwhile, the connection was taken for one with room.
  void _end_for_close() MX_REQUIRES(mutex_) {
    state_ = State::BROKEN;
    if (living_) {
      living_->store(false, std::memory_order_relaxed);
    }
  }

  // A writer finds the channel ended: the settling of the frame set aside
  // when `self` was writing it, its write `whole` or not, else nothing.
  Ending _settle_through(std::thread::id self, bool whole) MX_REQUIRES(mutex_) {
    if (settler_ != self) {
      return Ending::GONE;
    }
    settler_ = std::thread::id();
    return whole ? Ending::WRITTEN : Ending::LOST;
  }
  // The own loop's write finds the channel ended: the same, when it was
  // under way at the end.
  Ending _settle_owned(bool whole) MX_REQUIRES(mutex_) {
    if (!owner_settles_) {
      return Ending::GONE;
    }
    owner_settles_ = false;
    return whole ? Ending::WRITTEN : Ending::LOST;
  }

  mutable mx::AdaptiveMutex mutex_;
  Queue queue_ MX_GUARDED_BY(mutex_);
  State state_ MX_GUARDED_BY(mutex_) = State::FREE;
  std::uint32_t limit_ MX_GUARDED_BY(mutex_) = 1;  // as a connection's, until its peer type's is set
  int descriptor_ MX_GUARDED_BY(mutex_) = -1;
  std::atomic<bool>* living_ MX_GUARDED_BY(mutex_) = nullptr;  // the connection's, see start()
  std::thread::id writer_ MX_GUARDED_BY(mutex_);               // WRITING's thread
  std::size_t front_written_ MX_GUARDED_BY(mutex_) = 0;        // what a write that stopped wrote of the front
  bool owner_writes_ MX_GUARDED_BY(mutex_) = false;            // the own loop's async_write is under way
  // Who settles the frame under way when the channel ended: the writer's
  // thread, or the own loop's write.
  std::thread::id settler_ MX_GUARDED_BY(mutex_);
  bool owner_settles_ MX_GUARDED_BY(mutex_) = false;
  std::chrono::steady_clock::time_point quiet_since_ MX_GUARDED_BY(mutex_);
  bool heartbeat_owed_ MX_GUARDED_BY(mutex_) = false;  // see quiet()
  bool close_owed_ MX_GUARDED_BY(mutex_) = false;      // see close_when_flushed()
  ToRaw to_raw_;                                       // stateless
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_IO_SHARED_OUTGOING_H_
