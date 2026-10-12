//! The outbox's handover: what a connection that dies had not written goes
//! to another, or is held for one, and a lane that held the dead
//! connection follows it (`Core::handle_orphaned_outgoing_messages`).
//! Mirrors multiplexer/outbox_handover.cc. Part of the outbox; see `super`.
//!
//! A connection that dies hands its queue to another, and after it what
//! waited in its backlog: a pinned message is lost, an ALL copy dropped,
//! the rest goes, in order, to one other connection chosen once, and a lane
//! that held the dead connection follows it there. With no other connection
//! live, the rest is held for one, an ALL send once, whole.

use std::collections::VecDeque;
use std::sync::Arc;

use super::state::{Handover, WaitKey};
use crate::core::client::{DropReason, Wrapper};
use crate::core::{followed, ConnId, Core, OutFrame, QueueEntry, SendState, Tracker};

impl Core {
    /// Whether the ALL send whose copies share `frame` is held whole
    /// already.
    fn held_whole(&self, frame: &Arc<OutFrame>) -> bool {
        self.outbox.held.iter().any(|key| {
            self.outbox.get(*key).is_some_and(|waiting| {
                !waiting.over && waiting.frame.as_ref().is_some_and(|held| Arc::ptr_eq(held, frame))
            })
        })
    }

    /// A copy of an ALL send that waits for a connection whole already goes
    /// nowhere itself: its tracker reads LOST, unreported.
    fn supersede(&mut self, tracker: Option<&Arc<Tracker>>) {
        if let Some(tracker) = tracker {
            if tracker.get() == SendState::Queued {
                tracker.set(SendState::Lost);
                if followed(tracker) {
                    self.tracked_message_done(tracker, false);
                }
            }
        }
    }

    /// The connection that takes what `dead` had not written: chosen at its
    /// first handover and kept while it lives, remembered for `dead`'s
    /// target, for the lanes that held `dead`. None when none is live.
    fn successor(&mut self, dead: ConnId) -> Option<ConnId> {
        let target = self.conns.get(&dead).map(|state| state.target.clone())?;
        if let Some(entry) = self.outbox.handovers.iter().find(|each| each.target == target) {
            if entry.from == Some(dead) && self.conn_living(entry.to) {
                return Some(entry.to);
            }
        }
        let conn = self.choose_connection()?;
        let to_target = self.conns[&conn].target.clone();
        match self.outbox.handovers.iter_mut().find(|each| each.target == target) {
            Some(entry) => {
                entry.from = Some(dead);
                entry.to = conn;
                entry.to_target = to_target;
            }
            None => self.outbox.handovers.push(Handover { target, from: Some(dead), to: conn, to_target }),
        }
        Some(conn)
    }

    /// Where the messages of `gone`, a lane's connection, went once it
    /// died: the connection that took what it had not written, or the one
    /// that took over from that one, and so on, while it lives. None while
    /// `gone` lives, and when nothing is recorded or live.
    pub(super) fn handed_to(&self, gone: &Wrapper) -> Option<ConnId> {
        if gone.conn.is_some_and(|own| self.conn_living(own)) {
            return None;
        }
        let mut target = &gone.target;
        for _ in 0..self.outbox.handovers.len() {
            let entry = self.outbox.handovers.iter().find(|each| each.target == *target)?;
            if self.conn_living(entry.to) {
                return Some(entry.to);
            }
            target = &entry.to_target;
        }
        None
    }

    /// A waiting message that goes nowhere: its tracker reads LOST, and it
    /// is reported as a lost frame is.
    pub(super) fn lose(&mut self, key: WaitKey) {
        let shuts_down = self.client.shuts_down;
        let Some(waiting) = self.outbox.get_mut(key) else { return };
        waiting.over = true;
        let held = waiting.on.is_none();
        let number = waiting.number;
        let frame = waiting.frame.clone();
        let tracker = Arc::clone(&waiting.tracker);
        if held {
            self.outbox.held_live -= 1;
        }
        self.outbox.waiting -= 1;
        self.wait_flushes(number, -1);
        if let Some(frame) = frame {
            self.report_drop(&frame, if shuts_down { DropReason::ShutDown } else { DropReason::ConnectionLost });
        }
        if tracker.get() == SendState::Queued {
            tracker.set(SendState::Lost);
            if Arc::strong_count(&tracker) > 2 {
                self.tracked_message_done(&tracker, false);
            }
        }
    }

    /// A connection died: what waited for it is put aside, to be handed
    /// over after its queue.
    pub(crate) fn displace(&mut self, conn: ConnId) {
        let Some(position) = self.outbox.backlogs.iter().position(|backlog| backlog.conn == conn) else { return };
        let backlog = self.outbox.backlogs.remove(position);
        for key in backlog.waiting {
            if self.outbox.get(key).is_some_and(|waiting| !waiting.over) {
                self.outbox.displaced.push_back(key);
            } else {
                self.outbox.release(key);
            }
        }
    }

    /// A connection that shuts down offers what it had not written: every
    /// message goes to one other live connection, chosen once, and after
    /// them what waited in the dead connection's backlog, so that a
    /// stream's unwritten tail moves whole and in order. A pinned message,
    /// or an ALL copy while another connection lives, is left, to be
    /// reported lost; with no connection live the rest is held; at shutdown
    /// everything is left.
    pub(crate) fn handle_orphaned_outgoing_messages(&mut self, dead: ConnId, outgoing: &mut VecDeque<QueueEntry>) {
        let conn = if outgoing.is_empty() && self.outbox.displaced.is_empty() { None } else { self.successor(dead) };
        let mut left = VecDeque::new();
        let shuts_down = self.client.shuts_down;
        for (weak, frame) in std::mem::take(outgoing) {
            if frame.pinned || shuts_down {
                left.push_back((weak, frame));
                continue;
            }
            let tracker = weak.upgrade();
            let deadline = frame.deadline;
            if frame.for_all {
                if conn.is_some() {
                    left.push_back((weak, frame)); // the other connections have their copies
                } else if self.held_whole(&frame) {
                    self.supersede(tracker.as_ref());
                } else if self.past(deadline) {
                    left.push_back((weak, frame));
                } else {
                    self.hold(&frame, tracker, frame.number, deadline, true, None);
                }
                continue;
            }
            let Some(conn) = conn else {
                if self.past(deadline) {
                    left.push_back((weak, frame));
                } else {
                    self.hold(&frame, tracker, frame.number, deadline, false, None);
                }
                continue;
            };
            self.outbox.retries += 1;
            let tracker = tracker.unwrap_or_else(|| Tracker::new(SendState::Queued));
            let followed_before = followed(&tracker);
            let Some(placed) = self.place_on(
                conn,
                Arc::clone(&frame),
                Some(Arc::clone(&tracker)),
                frame.number,
                deadline,
                false,
                None,
            ) else {
                left.push_back((weak, frame));
                continue;
            };
            if placed.get() != SendState::Lost && followed_before {
                let wrapper = self.wrap(conn);
                self.record_moved(&tracker, wrapper);
            }
        }
        for (weak, frame) in &left {
            let _ = weak;
            self.report_drop(frame, if shuts_down { DropReason::ShutDown } else { DropReason::ConnectionLost });
        }
        *outgoing = left;
        self.replace_displaced(conn);
    }

    /// A message somebody follows went to another connection from a dead
    /// one, or from the held when a connection came up.
    pub(super) fn record_moved(&mut self, tracker: &Arc<Tracker>, conn: Wrapper) {
        let outbox = &mut self.outbox;
        if outbox.moved.len() >= outbox.moved_compact_at.max(64) {
            outbox.moved.retain(|(weak, _)| weak.strong_count() > 0);
            outbox.moved_compact_at = (2 * outbox.moved.len()).max(64);
        }
        outbox.moved.push((Arc::downgrade(tracker), conn));
    }

    /// The connection the message tracked by `tracker` went to last: the
    /// one a dead connection handed it to, or `first`.
    pub(crate) fn followed_connection(&mut self, tracker: &Arc<Tracker>, first: Option<Wrapper>) -> Option<Wrapper> {
        let mut last = first;
        if self.outbox.moved.is_empty() {
            return last;
        }
        self.outbox.moved.retain(|(weak, wrapper)| {
            if weak.upgrade().is_some_and(|moved| Arc::ptr_eq(&moved, tracker)) {
                last = Some(wrapper.clone());
                return false;
            }
            true
        });
        last
    }

    /// Whether `wrapper`'s connection is live.
    pub(crate) fn live(&self, wrapper: &Wrapper) -> bool {
        wrapper.conn.is_some_and(|conn| self.conn_living(conn))
    }

    /// What waited for a dead connection goes to `conn`, the one its queue
    /// went to, a lane it went through adopting that one, or, with none
    /// live, is held for the next; a pinned message is lost, and so is an
    /// ALL copy while another connection lives, else the message is held
    /// once, whole.
    fn replace_displaced(&mut self, conn: Option<ConnId>) {
        let shuts_down = self.client.shuts_down;
        for key in std::mem::take(&mut self.outbox.displaced) {
            let Some(waiting) = self.outbox.get(key) else { continue };
            if waiting.over {
                self.outbox.release(key);
                continue;
            }
            let frame = Arc::clone(waiting.frame.as_ref().expect("a waiting entry not over has its frame"));
            if frame.pinned || shuts_down {
                self.lose(key);
                self.outbox.release(key);
                continue;
            }
            if waiting.copy {
                if conn.is_some() {
                    self.lose(key);
                    self.outbox.release(key);
                } else if self.held_whole(&frame) {
                    let tracker = Arc::clone(&waiting.tracker);
                    let number = waiting.number;
                    if let Some(waiting) = self.outbox.get_mut(key) {
                        waiting.over = true;
                    }
                    self.outbox.waiting -= 1;
                    self.wait_flushes(number, -1);
                    self.supersede(Some(&tracker));
                    self.outbox.release(key);
                } else {
                    self.rehold(key, true);
                }
                continue;
            }
            let Some(conn) = conn else {
                self.rehold(key, false);
                continue;
            };
            let tracker = Arc::clone(&waiting.tracker);
            let (number, deadline, lane) = (waiting.number, waiting.deadline, waiting.lane.clone());
            if let Some(waiting) = self.outbox.get_mut(key) {
                waiting.over = true; // placed anew, as a new entry when it waits again
            }
            self.outbox.release(key);
            self.outbox.waiting -= 1;
            self.wait_flushes(number, -1);
            self.outbox.retries += 1;
            let followed_before = followed(&tracker);
            self.place_on(conn, frame, Some(Arc::clone(&tracker)), number, deadline, false, lane.clone());
            let wrapper = self.wrap(conn);
            if followed_before {
                self.record_moved(&tracker, wrapper.clone());
            }
            if let Some(lane) = lane {
                lane.adopt(&wrapper);
            }
        }
        self.check_flushes();
    }

    /// A dead connection's waiting message held for the next connection, its
    /// tracker, deadline and place in the counts kept.
    fn rehold(&mut self, key: WaitKey, all: bool) {
        if let Some(waiting) = self.outbox.get_mut(key) {
            waiting.on = None;
            waiting.copy = false;
            waiting.all = all;
        }
        self.outbox.held.push_back(key);
        self.outbox.held_live += 1;
    }
}
