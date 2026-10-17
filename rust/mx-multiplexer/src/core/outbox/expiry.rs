//! The outbox's deadlines: a message waits `timeout` at most, and one
//! timer, run to the earliest deadline, drops what still waits then
//! (`Core::expire`). Mirrors multiplexer/outbox_expiry.cc. Part of the
//! outbox; see `super`.
//!
//! What still waits at its deadline is dropped, its tracker reading LOST,
//! and reported. A message with no time to wait at all is given up on at
//! once (`give_up_message`). The heap of deadlines skips the keys of
//! messages that left, and clears them out when it has doubled; a queue
//! clears out its ended entries once they are as many as the live ones and
//! 64 more (`clear_out`).

use std::cmp::Reverse;
use std::collections::VecDeque;
use std::sync::Arc;

use super::state::WaitKey;
use crate::core::client::DropReason;
use crate::core::{followed, ConnId, Core, Due, OutFrame, SendState, Tracker};

impl Core {
    /// Drops the message held for a connection whose tracker is `tracker`,
    /// when it is held, reported NO_CONNECTION as one out of time is.
    pub(crate) fn drop_held(&mut self, tracker: &Arc<Tracker>) {
        let found = self.outbox.held.iter().copied().find(|key| {
            self.outbox.get(*key).is_some_and(|waiting| !waiting.over && Arc::ptr_eq(&waiting.tracker, tracker))
        });
        if let Some(key) = found {
            self.drop_expired(key);
        }
    }

    /// A message with no time left to wait, which no connection takes now:
    /// reported dropped for `reason` at once, its tracker reading LOST.
    pub(super) fn give_up_message(
        &mut self,
        frame: &Arc<OutFrame>,
        tracker: Option<Arc<Tracker>>,
        reason: DropReason,
    ) -> Arc<Tracker> {
        self.report_drop(frame, reason);
        let Some(tracker) = tracker else {
            return Tracker::new(SendState::Lost);
        };
        if tracker.get() == SendState::Queued {
            tracker.set(SendState::Lost);
            if followed(&tracker) {
                self.tracked_message_done(&tracker, false);
            }
        }
        tracker
    }

    /// A waiting message's deadline goes into the heap, the timer moving to
    /// it when it is the earliest; entries of messages that left are
    /// cleared out when the heap has doubled since the last time.
    pub(super) fn expire_at(&mut self, key: WaitKey) {
        let Some(deadline) = self.outbox.get(key).and_then(|waiting| waiting.deadline) else {
            return; // no deadline: it waits until written or gone
        };
        let outbox = &mut self.outbox;
        if outbox.expiring.len() >= outbox.compact_at.max(64) {
            let entries = std::mem::take(&mut outbox.expiring).into_vec();
            outbox.expiring = entries
                .into_iter()
                .filter(|Reverse((_, each))| outbox.get(*each).is_some_and(|waiting| !waiting.over))
                .collect();
            outbox.compact_at = (2 * outbox.expiring.len()).max(64);
        }
        outbox.expiring.push(Reverse((deadline, key)));
        if outbox.armed.is_none_or(|armed| deadline < armed) {
            self.arm_expiry();
        }
    }

    /// The timer set to the earliest deadline of a message still waiting,
    /// or stopped when none waits.
    fn arm_expiry(&mut self) {
        while let Some(Reverse((_, key))) = self.outbox.expiring.peek().copied() {
            if self.outbox.get(key).is_some_and(|waiting| !waiting.over) {
                break;
            }
            self.outbox.expiring.pop();
        }
        let Some(Reverse((next, _))) = self.outbox.expiring.peek().copied() else {
            self.timers.cancel(&mut self.outbox.timer);
            self.outbox.armed = None;
            return;
        };
        if self.outbox.armed == Some(next) {
            return;
        }
        self.outbox.armed = Some(next);
        self.timers.rearm(&mut self.outbox.timer, next, Due::OutboxExpiry);
    }

    /// The timer: what waited past its deadline is dropped, its tracker
    /// reading LOST, and reported.
    pub(crate) fn expire(&mut self) {
        self.outbox.timer = None;
        self.outbox.armed = None;
        while let Some(Reverse((deadline, key))) = self.outbox.expiring.peek().copied() {
            if deadline > self.now {
                break;
            }
            self.outbox.expiring.pop();
            if self.outbox.get(key).is_none_or(|waiting| waiting.over) {
                continue;
            }
            self.drop_expired(key);
        }
        let empty: Vec<ConnId> =
            self.outbox.backlogs.iter().filter(|backlog| backlog.live == 0).map(|backlog| backlog.conn).collect();
        for conn in empty {
            self.remove_backlog(conn);
        }
        self.arm_expiry();
        self.check_flushes();
    }

    /// A waiting message whose time is up: dropped and reported, its
    /// tracker reading LOST. One that waited on a connection not registered
    /// yet waited for that connection, not for room on it.
    pub(super) fn drop_expired(&mut self, key: WaitKey) {
        let Some(waiting) = self.outbox.get_mut(key) else { return };
        let on = waiting.on;
        let held = on.is_none();
        waiting.over = true;
        let number = waiting.number;
        let frame = waiting.frame.take();
        let tracker = Arc::clone(&waiting.tracker);
        if let Some(conn) = on {
            if let Some(backlog) = self.outbox.backlog(conn) {
                backlog.live -= 1;
            }
        }
        if held {
            self.held_left();
        }
        self.outbox.waiting -= 1;
        self.wait_flushes(number, -1);
        let unregistered = on.is_some_and(|conn| self.conns.get(&conn).is_some_and(|state| !state.registered));
        let reason = if held || unregistered { DropReason::NoConnection } else { DropReason::NoRoom };
        if let Some(frame) = frame {
            self.report_drop(&frame, reason);
        }
        if tracker.get() == SendState::Queued {
            tracker.set(SendState::Lost);
            if Arc::strong_count(&tracker) > 2 {
                self.tracked_message_done(&tracker, false);
            }
        }
        if held {
            let live = self.outbox.held_live;
            let mut queue = std::mem::take(&mut self.outbox.held);
            self.clear_out(&mut queue, live);
            self.outbox.held = queue;
        } else if let Some(conn) = on {
            if let Some(position) = self.outbox.backlogs.iter().position(|backlog| backlog.conn == conn) {
                let live = self.outbox.backlogs[position].live;
                let mut queue = std::mem::take(&mut self.outbox.backlogs[position].waiting);
                self.clear_out(&mut queue, live);
                self.outbox.backlogs[position].waiting = queue;
            }
        }
    }

    /// Clears the entries of `queue` that ended there, `live` of them
    /// waiting still, once the ended ones are as many as the live ones and
    /// 64 more, or none is live.
    fn clear_out(&mut self, queue: &mut VecDeque<WaitKey>, live: usize) {
        if live != 0 && queue.len() < 2 * live + 64 {
            return;
        }
        queue.retain(|key| {
            let over = self.outbox.get(*key).is_none_or(|waiting| waiting.over);
            if over {
                self.outbox.release(*key);
            }
            !over
        });
    }
}
