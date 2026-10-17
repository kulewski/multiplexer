//! The outbox's ends: a send's `done` hears its end once, and flush_all()
//! waits for what was sent before it (`Core::begin_flush`). Mirrors
//! multiplexer/outbox_flush.cc. Part of the outbox; see `super`.
//!
//! flush_all(): every message is numbered in the order sent; a flush waits
//! for those numbered up to the last one at its start: the ones that still
//! wait, and per connection the last of them queued there. The copies of a
//! followed send, written or given up on, are heard in one pass per loop
//! turn (`process_callbacks`), outside the connection's handlers and the
//! outbox's bookkeeping, after the drop observer heard of the drops
//! reported meanwhile; a flush that waits for those callbacks ends after
//! them.

use std::sync::Arc;

use super::state::{tracker_key, Flush, Follow};
use super::{FlushDone, FlushId, SendDone};
use crate::core::client::guarded;
use crate::core::{ConnId, Core, Posted, SendState, Tracker};

impl Core {
    /// `done` hears the end of the send whose copies are `copies`; a copy
    /// given up on already is heard as any end is, posted.
    pub(super) fn follow(&mut self, copies: Vec<Arc<Tracker>>, done: SendDone) {
        self.outbox.next_follow += 1;
        let id = self.outbox.next_follow;
        for copy in &copies {
            self.outbox.follows.insert(tracker_key(copy), id);
        }
        let lost: Vec<Arc<Tracker>> = copies.iter().filter(|copy| copy.get() == SendState::Lost).cloned().collect();
        let live = copies.len();
        self.outbox.follow_states.insert(id, Follow { done: Some(done), copies, live, over: false });
        for copy in lost {
            self.follow_event(&copy, false);
        }
    }

    /// A frame whose tracker somebody holds was written, or lost: a send's
    /// `done` may wait on it, or a flush.
    pub(crate) fn tracked_message_done(&mut self, tracker: &Arc<Tracker>, written: bool) {
        self.follow_event(tracker, written);
        self.check_flushes();
    }

    /// A followed copy was written or given up on: noted here, inside the
    /// connection's handlers, and handled in one pass per loop turn.
    fn follow_event(&mut self, tracker: &Arc<Tracker>, written: bool) {
        if !self.outbox.follows.contains_key(&tracker_key(tracker)) {
            return; // a flush's mark, or a send nobody follows
        }
        self.outbox.follow_events.push((Arc::clone(tracker), written));
        self.post_callbacks();
    }

    /// The pass that runs the program's callbacks, posted once for whatever
    /// gathers until it runs.
    pub(crate) fn post_callbacks(&mut self) {
        if self.outbox.callbacks_posted {
            return;
        }
        self.outbox.callbacks_posted = true;
        self.posted.push_back(Posted::ProcessCallbacks);
    }

    /// The program's callbacks of a loop turn: the drops reported, told to
    /// the observer, then the followed sends' ends, and then the flushes
    /// that waited for them.
    pub(crate) fn process_callbacks(&mut self) {
        self.tell_drops();
        self.process_follows();
    }

    /// The followed sends whose copies were written or given up on: one
    /// written ends a send with 1, the last given up on with 0.
    pub(crate) fn process_follows(&mut self) {
        for (tracker, written) in std::mem::take(&mut self.outbox.follow_events) {
            let Some(id) = self.outbox.follows.remove(&tracker_key(&tracker)) else {
                continue; // its send ended meanwhile
            };
            let Some(follow) = self.outbox.follow_states.get_mut(&id) else { continue };
            follow.live -= 1;
            if written {
                self.end_follow(id, 1);
            } else if follow.live == 0 {
                self.end_follow(id, 0);
            }
        }
        self.check_flushes();
    }

    /// A followed send ends: its copies are not followed any more, and
    /// `done`, called once, hears `written`.
    fn end_follow(&mut self, id: u64, written: u32) {
        let Some(mut follow) = self.outbox.follow_states.remove(&id) else { return };
        if follow.over {
            return;
        }
        follow.over = true;
        for copy in &follow.copies {
            let key = tracker_key(copy);
            if self.outbox.follows.get(&key) == Some(&id) {
                self.outbox.follows.remove(&key);
            }
        }
        match follow.done.take() {
            Some(SendDone::Callback(done)) => guarded(|| "a send's callback".to_string(), || done(written)),
            Some(SendDone::Settle(id)) => self.settle_send(id, written, written == 0),
            None => {}
        }
    }

    /// Ends every send still followed, each `done` hearing 0, at once, after
    /// the drop observer heard of every drop reported before: for a client
    /// whose loop will not run again.
    pub(crate) fn end_follows(&mut self) {
        self.tell_drops();
        self.outbox.follow_events.clear();
        let left: Vec<u64> = self.outbox.follow_states.keys().copied().collect();
        for id in left {
            self.end_follow(id, 0);
        }
        self.check_flushes();
    }

    /// flush_all(): waits for the messages numbered up to the last one now,
    /// those still waiting and the last of them queued on each connection.
    /// `done` is told once, when they are out, with whether every one was
    /// written.
    pub(crate) fn begin_flush(&mut self, done: Option<FlushDone>) -> FlushId {
        if !self.outbox.follow_events.is_empty() || !self.outbox.drop_events.is_empty() {
            self.process_callbacks(); // what ended before the flush began is heard before it ends
        }
        self.outbox.next_flush += 1;
        let id = FlushId(self.outbox.next_flush);
        let mut marks = Vec::new();
        let registered: Vec<ConnId> = self.by_peer_id.values().copied().collect();
        for conn in registered {
            if self.conn_living(conn) {
                if let Some(tracker) = self.track_last_queued(conn) {
                    marks.push((conn, tracker));
                }
            }
        }
        let flush = Flush {
            last_number: self.outbox.last_number,
            waiting: self.outbox.waiting,
            marks,
            done,
            flushed: false,
            lost: false,
        };
        self.outbox.flush_states.insert(id, flush);
        self.outbox.flushes.push(id);
        self.check_flushes();
        id
    }

    /// A tracker for the frame queued last on `conn` but its own frames
    /// (`Connection::last_queued`), which everything queued there before it
    /// is written ahead of; None when the queue holds none other.
    fn track_last_queued(&mut self, conn: ConnId) -> Option<Arc<Tracker>> {
        let entry = self.conns.get_mut(&conn)?.last_queued()?;
        if let Some(tracker) = entry.0.upgrade() {
            return Some(tracker);
        }
        let tracker = Tracker::new(SendState::Queued);
        entry.0 = Arc::downgrade(&tracker); // the queue holds it weakly, as any tracker
        Some(tracker)
    }

    /// Whether the flush is done.
    pub(crate) fn flushed(&self, flush: FlushId) -> bool {
        self.outbox.flush_states.get(&flush).is_some_and(|state| state.flushed)
    }

    /// Whether it is done with every message written.
    pub(crate) fn all_written(&self, flush: FlushId) -> bool {
        self.outbox.flush_states.get(&flush).is_some_and(|state| state.flushed && !state.lost)
    }

    /// Forgets a flush, one that ran out of time or was read.
    pub(crate) fn end_flush(&mut self, flush: FlushId) {
        self.outbox.flushes.retain(|each| *each != flush);
        self.outbox.flush_states.remove(&flush);
    }

    /// A message numbered `number` was given up on: a flush that waits for
    /// it ends with not all written.
    pub(crate) fn flushes_lost(&mut self, number: u64) {
        for id in &self.outbox.flushes {
            if let Some(flush) = self.outbox.flush_states.get_mut(id) {
                if number != 0 && number <= flush.last_number {
                    flush.lost = true;
                }
            }
        }
    }

    /// `copies` more, or fewer, of message `number` wait: counted by every
    /// flush that waits for that message.
    pub(super) fn wait_flushes(&mut self, number: u64, copies: i64) {
        for id in &self.outbox.flushes {
            if let Some(flush) = self.outbox.flush_states.get_mut(id) {
                if number <= flush.last_number {
                    flush.waiting = (flush.waiting as i64 + copies) as usize;
                }
            }
        }
    }

    /// A flush is done once the messages it counts no longer wait, every
    /// frame it marked is out, and the callbacks of the sends that ended
    /// meanwhile have run; `done` is told after it left the list.
    pub(crate) fn check_flushes(&mut self) {
        if self.outbox.flushes.is_empty()
            || !self.outbox.follow_events.is_empty()
            || !self.outbox.drop_events.is_empty()
        {
            return;
        }
        let mut finished = Vec::new();
        let outbox = &mut self.outbox;
        outbox.flushes.retain(|id| {
            let Some(flush) = outbox.flush_states.get_mut(id) else { return false };
            flush.marks.retain(|(_, tracker)| tracker.get() == SendState::Queued);
            if flush.waiting > 0 || !flush.marks.is_empty() {
                return true;
            }
            flush.flushed = true;
            finished.push(*id);
            false
        });
        for id in finished {
            let Some(flush) = self.outbox.flush_states.get_mut(&id) else { continue };
            let all_written = !flush.lost;
            match flush.done.take() {
                #[cfg(test)]
                Some(FlushDone::Callback(done)) => guarded(|| "a flush's callback".to_string(), || done(all_written)),
                Some(FlushDone::Wait(wait)) => self.end_flush_wait(wait, all_written),
                Some(FlushDone::Drain) => self.posted.push_back(Posted::WriteOutStep),
                None => {}
            }
        }
    }
}
