//! The outbox: what a client's connections cannot take yet, and the one way
//! every client sends (`Core::send`), placing each message. Mirrors
//! multiplexer/outbox.cc, whose comment this follows, and its other parts
//! mirror the C++ outbox's files: `handover` outbox_handover.cc's, what a
//! connection that dies had not written; `expiry` outbox_expiry.cc's, a
//! waiting message's deadline; `flush` outbox_flush.cc's, the end of a send
//! its `done` hears, and flush_all(); `state` outbox.h's, the structures.
//!
//! A message goes into its connection's queue while the queue has room and
//! nothing waits for that connection before it; otherwise it waits in the
//! connection's backlog. When a full queue has room again the backlog moves
//! in, in order, as far as the room goes. A message given no connection
//! goes to one with room, round robin, or, with every one full, waits on
//! the one with the least waiting. With no connection live it is held, in
//! order, behind whatever was held before it, until one comes up. A send to
//! ALL gives every live connection its copy, and the connection of each
//! multiplexer whose first connection is still on its way one in its
//! backlog, which the welcome lets in; with none live it is held whole. A
//! message waits `timeout` at most (`expiry`). A message with no time to
//! wait goes only where a connection takes it now.

mod expiry;
mod flush;
mod handover;
mod state;

use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use state::{Backlog, WaitKey, Waiting};
pub(crate) use state::{FlushId, Outbox};

use super::batching::{BatchMark, Joining};
use super::client::{DropReason, Wrapper};
use super::lane::Lane;
use super::{deadline_after, ConnId, Core, OutFrame, SendState, Tracker};
use crate::frame::Head;

/// A frame as a caller hands it over, before its first placement: the
/// bytes, header and body, or a big body behind its header kept apart
/// (`head`, `Core::alone_frame`), the id and type a drop is reported by,
/// and what batching knows of it.
#[derive(Debug, Clone)]
pub(crate) struct Frame {
    pub(crate) bytes: Bytes,
    pub(crate) head: Option<Head>,
    pub(crate) id: u64,
    pub(crate) r#type: u32,
    pub(crate) batch: BatchMark,
}

/// Who hears a send's end, once: 1 once a copy is written, the first for
/// ALL, 0 once every copy was given up on or the client shut down first. A
/// callback of the program's, a send the threaded client follows for its
/// caller, settled by the core, the messages of a batch, each its own
/// `done` and tracker (`Core::end_batch`), or the frame of a confirmed
/// batch, by its envelope's id, whose messages' ends wait for their
/// confirmation (`Core::confirming_written`).
pub(crate) enum SendDone {
    Callback(Box<dyn FnOnce(u32) + Send>),
    Settle(u64),
    Batch(Vec<(Option<SendDone>, Option<Arc<Tracker>>)>),
    Confirmed(u64),
}

/// Who hears a flush's end: whether every message it waited for was
/// written. A callback, a flush_all() call the threaded client waits on,
/// or its shutdown's write-out.
pub(crate) enum FlushDone {
    #[cfg(test)]
    Callback(Box<dyn FnOnce(bool) + Send>),
    Wait(u64),
    Drain,
}

impl Core {
    /// The next number in the order sent.
    pub(crate) fn next_number(&mut self) -> u64 {
        self.outbox.last_number += 1;
        self.outbox.last_number
    }

    /// `frame` as it is first placed, with its marks; an event's, made
    /// unsealed for a batch it did not go in (`BatchMark::Event`), sealed.
    fn out_frame(frame: Frame, pinned: bool, for_all: bool, number: u64, deadline: Option<Instant>) -> Arc<OutFrame> {
        let (bytes, batch, confirmed) = match frame.batch {
            BatchMark::Event(_) => (crate::frame::seal(frame.bytes), None, false),
            BatchMark::Batch(ids) => (frame.bytes, Some(ids), false),
            BatchMark::Confirmed => (frame.bytes, None, true),
            BatchMark::None => (frame.bytes, None, false),
        };
        Arc::new(OutFrame {
            bytes,
            head: frame.head,
            id: frame.id,
            r#type: frame.r#type,
            pinned,
            for_all,
            own: false,
            number,
            deadline,
            batch,
            confirmed,
        })
    }

    /// Whether `deadline` has come: no time left to wait.
    fn past(&self, deadline: Option<Instant>) -> bool {
        deadline.is_some_and(|deadline| deadline <= self.now)
    }

    /// On one connection: the one a lane's dead connection handed its
    /// messages to, else round robin over those with room and nothing
    /// waiting, else the live one with the least waiting. None when no
    /// connection is live.
    pub(crate) fn schedule_one(
        &mut self,
        frame: Frame,
        pinned: bool,
        used: Option<&mut Option<Wrapper>>,
        timeout: Option<Duration>,
        number: u64,
        lane: Option<&Lane>,
    ) -> Option<Arc<Tracker>> {
        self.close_batches(); // nothing overtakes a message sent before it
        let mut conn = None;
        if let Some(lane) = lane {
            if !self.outbox.handovers.is_empty() && lane.holds_connection() {
                conn = lane.connection().and_then(|gone| self.handed_to(&gone));
            }
        }
        let conn = conn.or_else(|| self.choose_connection())?;
        if let Some(used) = used {
            *used = Some(self.wrap(conn));
        }
        let number = if number != 0 { number } else { self.next_number() };
        let deadline = deadline_after(self.now, timeout);
        let frame = Self::out_frame(frame, pinned, false, number, deadline);
        self.place_on(conn, frame, None, number, deadline, false, lane.cloned())
    }

    /// On `wrapper`'s connection and never another, waiting there when it
    /// is full: None when that connection is gone, or the client shut down.
    pub(crate) fn schedule_on(
        &mut self,
        frame: Frame,
        pinned: bool,
        wrapper: &Wrapper,
        timeout: Option<Duration>,
        number: u64,
        lane: Option<&Lane>,
    ) -> Option<Arc<Tracker>> {
        let conn = wrapper.conn?;
        if self.client.shuts_down || !self.conn_living(conn) {
            return None;
        }
        self.close_batches();
        let number = if number != 0 { number } else { self.next_number() };
        let deadline = deadline_after(self.now, timeout);
        let frame = Self::out_frame(frame, pinned, false, number, deadline);
        self.place_on(conn, frame, None, number, deadline, false, lane.cloned())
    }

    /// A copy on every live connection: how many, the connections in `used`
    /// and the copies' trackers in `trackers`.
    pub(crate) fn schedule_all(
        &mut self,
        frame: Frame,
        mut used: Option<&mut Vec<Wrapper>>,
        timeout: Option<Duration>,
        number: u64,
        mut trackers: Option<&mut Vec<Arc<Tracker>>>,
    ) -> usize {
        if self.client.shuts_down {
            return 0;
        }
        self.close_batches();
        let number = if number != 0 { number } else { self.next_number() };
        let deadline = deadline_after(self.now, timeout);
        let frame = Self::out_frame(frame, false, true, number, deadline);
        let mut copies = 0;
        let registered: Vec<ConnId> = self.by_peer_id.values().copied().collect();
        for conn in registered {
            if !self.conn_living(conn) {
                continue;
            }
            let Some(tracker) = self.place_on(conn, Arc::clone(&frame), None, number, deadline, true, None) else {
                continue;
            };
            copies += 1;
            if let Some(used) = used.as_deref_mut() {
                used.push(self.wrap(conn));
            }
            if let Some(trackers) = trackers.as_deref_mut() {
                trackers.push(tracker);
            }
        }
        copies
    }

    /// How every client sends a message: ONE way, round robin or through
    /// `lane`, or to ALL. Returns at once: the message is queued, waits in a
    /// backlog, or is held until a connection comes up, `timeout` at most.
    /// The trackers of what was placed or held go to `trackers`; `done`
    /// hears the message's end, once, posted. False, nothing placed, when
    /// nothing may take it: the client is shutting down, or a pinned lane's
    /// connection is gone. `may_batch`: a send nobody waits for, which goes
    /// by the delivery set for its type or key, through no lane and not to
    /// ALL (`batching`), `timeout` not used when one is: in a batch, or
    /// kept until confirmed, when the delivery batches or confirms, else as
    /// any send by the delivery's timeout. Any other lets every batch go
    /// first, so that nothing overtakes a message sent before it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send(
        &mut self,
        frame: Frame,
        all: bool,
        lane: Option<&Lane>,
        mut timeout: Option<Duration>,
        number: u64,
        trackers: Option<&mut Vec<Arc<Tracker>>>,
        used: Option<&mut Option<Wrapper>>,
        done: Option<SendDone>,
        may_batch: bool,
    ) -> bool {
        if self.client.shuts_down {
            return false;
        }
        if may_batch && !all && lane.is_none() {
            if let Some(delivery) = self.delivery_for(&frame) {
                if delivery.batch || delivery.confirm {
                    return self.batch(Joining::Frame(frame), delivery, trackers, done);
                }
                timeout = delivery.timeout; // a delivery that sends it as it is, by its own timeout
            }
        }
        self.close_batches();
        let deadline = deadline_after(self.now, timeout);
        self.send_at(frame, all, lane, deadline, number, trackers, used, done)
    }

    /// `send()` but for a delivery: the message waits until `deadline`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_at(
        &mut self,
        frame: Frame,
        all: bool,
        lane: Option<&Lane>,
        deadline: Option<Instant>,
        number: u64,
        trackers: Option<&mut Vec<Arc<Tracker>>>,
        used: Option<&mut Option<Wrapper>>,
        done: Option<SendDone>,
    ) -> bool {
        if self.client.shuts_down {
            return false;
        }
        // What `done` follows, when the caller keeps no trackers itself.
        let mut own = Vec::new();
        let mut trackers = match trackers {
            Some(trackers) => Some(trackers),
            None if done.is_some() => Some(&mut own),
            None => None,
        };
        let before = trackers.as_ref().map_or(0, |trackers| trackers.len());
        let pinned = !all && lane.is_some_and(Lane::pinned);
        let number = if number != 0 { number } else { self.next_number() };
        let frame = Self::out_frame(frame, pinned, all, number, deadline);
        let mut refused = false;
        let placed = self.outbox.held_live == 0
            && self.place_now(&frame, all, lane, number, deadline, None, trackers.as_deref_mut(), used, &mut refused);
        if !placed {
            // Something is held already, so no connection is live: a pinned
            // lane holding one has lost it.
            if refused || (!all && lane.is_some_and(|lane| lane.pinned() && lane.holds_connection())) {
                return false;
            }
            let tracker = if self.past(deadline) {
                // No time to wait for a connection: given up on at once.
                self.give_up_message(&frame, None, DropReason::NoConnection)
            } else {
                self.hold(&frame, None, number, deadline, all, lane.cloned())
            };
            if let Some(trackers) = trackers.as_deref_mut() {
                trackers.push(tracker);
            }
        }
        if let (Some(done), Some(trackers)) = (done, trackers) {
            let copies = trackers[before..].to_vec();
            self.follow(copies, done);
        }
        true
    }

    /// `frame` placed now, ONE way or to ALL, `tracker` the tracker of its
    /// first copy when given, as a held message keeps its own: false when no
    /// connection is live, or, `refused`, when a pinned lane's connection is
    /// gone.
    #[allow(clippy::too_many_arguments)]
    fn place_now(
        &mut self,
        frame: &Arc<OutFrame>,
        all: bool,
        lane: Option<&Lane>,
        number: u64,
        deadline: Option<Instant>,
        tracker: Option<Arc<Tracker>>,
        mut trackers: Option<&mut Vec<Arc<Tracker>>>,
        used: Option<&mut Option<Wrapper>>,
        refused: &mut bool,
    ) -> bool {
        if self.client.shuts_down {
            return false;
        }
        if all {
            let mut placed = false;
            let mut first_tracker = tracker;
            let mut used = used;
            // Every registered connection, walked by peer id rather than
            // collected first, which would allocate on every send.
            let mut cursor = self.by_peer_id.first_key_value().map(|(&peer_id, &conn)| (peer_id, conn));
            while let Some((peer_id, conn)) = cursor {
                cursor = self
                    .by_peer_id
                    .range((Bound::Excluded(peer_id), Bound::Unbounded))
                    .next()
                    .map(|(&peer_id, &conn)| (peer_id, conn));
                if !self.conn_living(conn) {
                    continue;
                }
                let copy = self.place_on(conn, Arc::clone(frame), first_tracker.take(), number, deadline, true, None);
                if !placed {
                    if let Some(used) = used.as_deref_mut() {
                        *used = Some(self.wrap(conn));
                    }
                }
                placed = true;
                if let (Some(trackers), Some(copy)) = (trackers.as_deref_mut(), copy) {
                    trackers.push(copy);
                }
            }
            // A multiplexer whose first connection is still on its way gets
            // its copy too, in that connection's backlog until the welcome
            // lets it in.
            if placed && !self.past(deadline) {
                let first: Vec<ConnId> = self
                    .client
                    .by_target
                    .iter()
                    .filter(|(target, conn)| {
                        self.conns.get(conn).is_some_and(|state| state.living() && !state.registered)
                            && !self.client.first_connection_over.contains(*target)
                    })
                    .map(|(_, conn)| *conn)
                    .collect();
                for conn in first {
                    let copy = self.place_on(conn, Arc::clone(frame), None, number, deadline, true, None);
                    if let (Some(trackers), Some(copy)) = (trackers.as_deref_mut(), copy) {
                        trackers.push(copy);
                    }
                }
            }
            return placed;
        }
        let mut conn = None;
        if let Some(lane) = lane {
            if lane.holds_connection() {
                let held = lane.connection();
                if let Some(own) = held.as_ref().and_then(|held| held.conn).filter(|own| self.conn_living(*own)) {
                    let placed =
                        self.place_on(own, Arc::clone(frame), tracker, number, deadline, false, Some(lane.clone()));
                    if let (Some(trackers), Some(placed)) = (trackers, placed) {
                        trackers.push(placed);
                    }
                    if let Some(used) = used {
                        *used = held;
                    }
                    return true;
                }
                if lane.pinned() {
                    *refused = true;
                    return false;
                }
                conn = held.and_then(|held| self.handed_to(&held));
            }
        }
        let Some(conn) = conn.or_else(|| self.choose_connection()) else {
            return false;
        };
        let placed = self.place_on(conn, Arc::clone(frame), tracker, number, deadline, false, lane.cloned());
        if let (Some(trackers), Some(placed)) = (trackers, placed) {
            trackers.push(placed);
        }
        if lane.is_some() || used.is_some() {
            // Built only for whoever takes it: a plain send wants none.
            let wrapper = self.wrap(conn);
            if let Some(lane) = lane {
                lane.adopt(&wrapper);
            }
            if let Some(used) = used {
                *used = Some(wrapper);
            }
        }
        true
    }

    /// `frame` held for a connection to come up, behind what is held
    /// already, with `tracker` as its tracker, made here when None.
    fn hold(
        &mut self,
        frame: &Arc<OutFrame>,
        tracker: Option<Arc<Tracker>>,
        number: u64,
        deadline: Option<Instant>,
        all: bool,
        lane: Option<Lane>,
    ) -> Arc<Tracker> {
        let tracker = tracker.unwrap_or_else(|| Tracker::new(SendState::Queued));
        let key = self.outbox.store(Waiting {
            frame: Some(Arc::clone(frame)),
            tracker: Arc::clone(&tracker),
            number,
            deadline,
            on: None,
            copy: false,
            all,
            lane,
            over: false,
        });
        self.outbox.held.push_back(key);
        self.outbox.held_live += 1;
        self.outbox.waiting += 1;
        self.wait_flushes(number, 1);
        self.expire_at(key);
        tracker
    }

    /// How many messages wait for a connection to come up: sent while none
    /// was live, or left by one that died with none to take them, each
    /// going out in order once one is registered, or given up on at its own
    /// deadline. What the server classes' drains wait for, a close dropping
    /// them; the last one to go ends a serve loop's wait as an
    /// acknowledgement does (`take_acknowledged_wake()`).
    pub(crate) fn held_for_a_connection(&self) -> usize {
        self.outbox.held_live
    }

    /// A message held for a connection went, placed on one that came up or
    /// given up on: the last to go ends a serve loop's wait, so that a
    /// drain waiting for what is held looks again at once, not at its next
    /// poll.
    pub(super) fn held_left(&mut self) {
        self.outbox.held_live -= 1;
        if self.outbox.held_live == 0 {
            self.client.acknowledged_wake = true;
        }
    }

    /// A connection came up: what was held for one is placed, in order,
    /// each with its own tracker and deadline, until nothing live takes it.
    pub(crate) fn place_held(&mut self) {
        while let Some(key) = self.outbox.held.front().copied() {
            let Some(waiting) = self.outbox.get(key) else {
                self.outbox.held.pop_front();
                continue;
            };
            if waiting.over {
                self.outbox.held.pop_front();
                self.outbox.release(key);
                continue;
            }
            let frame = Arc::clone(waiting.frame.as_ref().expect("a waiting entry not over has its frame"));
            let (all, lane, number, deadline) = (waiting.all, waiting.lane.clone(), waiting.number, waiting.deadline);
            let tracker = Arc::clone(&waiting.tracker);
            let mut refused = false;
            let mut placed_on = None;
            if !self.place_now(
                &frame,
                all,
                lane.as_ref(),
                number,
                deadline,
                Some(Arc::clone(&tracker)),
                None,
                Some(&mut placed_on),
                &mut refused,
            ) {
                if !refused {
                    break; // nothing live after all
                }
                self.outbox.held.pop_front();
                self.lose(key); // its pinned lane's connection is gone
                self.outbox.release(key);
                continue;
            }
            self.outbox.held.pop_front();
            // Somebody besides this entry follows it: where it went.
            if !all && Arc::strong_count(&tracker) > 2 {
                if let Some(placed_on) = placed_on {
                    self.record_moved(&tracker, placed_on);
                }
            }
            if let Some(waiting) = self.outbox.get_mut(key) {
                waiting.over = true;
            }
            self.outbox.release(key);
            self.held_left();
            self.outbox.waiting -= 1;
            self.wait_flushes(number, -1);
            self.outbox.retries += 1;
        }
        self.check_flushes();
    }

    /// A connection for a message given none: one with room and nothing
    /// waiting, round robin, the one chosen moved to the back; with none
    /// such, the live one with the least waiting. None when none is live or
    /// the client is shutting down.
    pub(crate) fn choose_connection(&mut self) -> Option<ConnId> {
        if self.client.shuts_down {
            return None;
        }
        let free = self.round_robin.iter().position(|conn| {
            self.conns.get(conn).is_some_and(|state| state.living() && !state.outgoing_queue_full())
                && !self.outbox.backlogs.iter().any(|backlog| backlog.conn == *conn && backlog.live > 0)
        });
        if let Some(position) = free {
            let conn = self.round_robin.remove(position).expect("found");
            self.round_robin.push_back(conn);
            return Some(conn);
        }
        let mut least: Option<(ConnId, usize)> = None;
        for conn in &self.round_robin {
            if !self.conn_living(*conn) {
                continue;
            }
            let waiting =
                self.outbox.backlogs.iter().find(|backlog| backlog.conn == *conn).map_or(0, |backlog| backlog.live);
            if least.is_none_or(|(_, fewest)| waiting < fewest) {
                least = Some((*conn, waiting));
            }
        }
        least.map(|(conn, _)| conn)
    }

    /// Into `conn`'s queue when it has room and nothing waits for it, else
    /// into its backlog; `tracker` is the message's tracker, made here when
    /// None.
    #[allow(clippy::too_many_arguments)]
    fn place_on(
        &mut self,
        conn: ConnId,
        frame: Arc<OutFrame>,
        tracker: Option<Arc<Tracker>>,
        number: u64,
        deadline: Option<Instant>,
        copy: bool,
        lane: Option<Lane>,
    ) -> Option<Arc<Tracker>> {
        if !self.conn_living(conn) {
            return None;
        }
        let tracker = tracker.unwrap_or_else(|| Tracker::new(SendState::Queued));
        let waiting = self.outbox.backlog(conn).is_some_and(|backlog| backlog.live > 0);
        if !waiting && self.take_over(conn, (Arc::downgrade(&tracker), Arc::clone(&frame))) {
            self.queued(number, &tracker, conn);
            return Some(tracker);
        }
        if self.past(deadline) {
            return Some(self.give_up_message(&frame, Some(tracker), DropReason::NoRoom));
        }
        let key = self.outbox.store(Waiting {
            frame: Some(frame),
            tracker: Arc::clone(&tracker),
            number,
            deadline,
            on: Some(conn),
            copy,
            all: false,
            lane,
            over: false,
        });
        match self.outbox.backlog(conn) {
            Some(backlog) => {
                backlog.waiting.push_back(key);
                backlog.live += 1;
            }
            None => self.outbox.backlogs.push(Backlog { conn, waiting: VecDeque::from([key]), live: 1 }),
        }
        self.outbox.waiting += 1;
        self.wait_flushes(number, 1);
        self.expire_at(key);
        Some(tracker)
    }

    /// A message went into `conn`'s queue: a flush that waits for it follows
    /// it there, as the last of its messages on that connection.
    fn queued(&mut self, number: u64, tracker: &Arc<Tracker>, conn: ConnId) {
        for id in &self.outbox.flushes {
            let Some(flush) = self.outbox.flush_states.get_mut(id) else { continue };
            if number > flush.last_number {
                continue;
            }
            match flush.marks.iter_mut().find(|mark| mark.0 == conn) {
                Some(mark) => mark.1 = Arc::clone(tracker),
                None => flush.marks.push((conn, Arc::clone(tracker))),
            }
        }
    }

    /// A connection whose queue was full has room again: what waits for it
    /// goes in, in order, as far as the room goes.
    pub(crate) fn outgoing_queue_has_room(&mut self, conn: ConnId) {
        let now = self.now;
        while let Some(backlog) = self.outbox.backlog(conn) {
            if backlog.live == 0 {
                break;
            }
            if self.conns.get(&conn).is_none_or(|state| state.outgoing_queue_full()) {
                break;
            }
            let Some(backlog) = self.outbox.backlog(conn) else { break };
            let Some(key) = backlog.waiting.pop_front() else { break };
            let Some(waiting) = self.outbox.get(key) else { continue };
            if waiting.over {
                self.outbox.release(key);
                continue; // dropped at its deadline meanwhile
            }
            if waiting.deadline.is_some_and(|deadline| deadline <= now) {
                // Its time is up and the timer has yet to run: dropped, not
                // written late.
                self.drop_expired(key);
                self.outbox.release(key);
                continue;
            }
            let frame = Arc::clone(waiting.frame.as_ref().expect("a waiting entry not over has its frame"));
            let tracker = Arc::clone(&waiting.tracker);
            let number = waiting.number;
            if let Some(waiting) = self.outbox.get_mut(key) {
                waiting.over = true;
            }
            self.outbox.release(key);
            if let Some(backlog) = self.outbox.backlog(conn) {
                backlog.live -= 1;
            }
            self.outbox.waiting -= 1;
            self.outbox.retries += 1;
            self.take_over(conn, (Arc::downgrade(&tracker), frame));
            self.wait_flushes(number, -1);
            self.queued(number, &tracker, conn);
        }
        if self.outbox.backlog(conn).is_some_and(|backlog| backlog.live == 0) {
            self.remove_backlog(conn);
        }
        self.check_flushes();
    }

    /// Takes `conn`'s backlog out, freeing the entries it holds that are
    /// over.
    fn remove_backlog(&mut self, conn: ConnId) {
        if let Some(position) = self.outbox.backlogs.iter().position(|backlog| backlog.conn == conn) {
            let backlog = self.outbox.backlogs.remove(position);
            for key in backlog.waiting {
                self.outbox.release(key);
            }
        }
    }

    /// The client shuts down: nothing waits any more, every tracker reads
    /// LOST, every message reported dropped.
    pub(crate) fn drop_outbox(&mut self) {
        let mut every: Vec<WaitKey> = Vec::new();
        for backlog in std::mem::take(&mut self.outbox.backlogs) {
            every.extend(backlog.waiting);
        }
        every.extend(std::mem::take(&mut self.outbox.displaced));
        every.extend(std::mem::take(&mut self.outbox.held));
        for key in every {
            let Some(waiting) = self.outbox.get_mut(key) else { continue };
            if waiting.over {
                self.outbox.release(key);
                continue;
            }
            waiting.over = true;
            let frame = waiting.frame.clone();
            let tracker = Arc::clone(&waiting.tracker);
            if let Some(frame) = frame {
                self.report_drop(&frame, DropReason::ShutDown);
            }
            if tracker.get() == SendState::Queued {
                tracker.set(SendState::Lost);
                if Arc::strong_count(&tracker) > 2 {
                    self.tracked_message_done(&tracker, false);
                }
            }
            self.outbox.release(key);
        }
        self.outbox.held_live = 0;
        self.outbox.waiting = 0;
        self.outbox.expiring.clear();
        self.timers.cancel(&mut self.outbox.timer);
        self.outbox.armed = None;
    }
}
