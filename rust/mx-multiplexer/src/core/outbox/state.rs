//! The outbox's structures, multiplexer/outbox.h's: the waiting entries,
//! the backlogs, the flushes and follows, and the outbox itself. Part of
//! the outbox; see `super`.
//!
//! What waits lives in a slab, `WaitKey`s in the queues and the deadline
//! heap, where C++ shares pointers: an entry is freed when it leaves its
//! queue, the heap skipping keys freed or ended. The fields are pub(super),
//! for the outbox's files.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Instant;

use super::FlushDone;
use super::SendDone;
use crate::core::client::Wrapper;
use crate::core::lane::Lane;
use crate::core::timers::TimerKey;
use crate::core::{ConnId, OutFrame, Target, Tracker};

/// The key of a waiting entry in the slab.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct WaitKey {
    index: usize,
    generation: u64,
}

/// A message waiting for room on one connection, or, held, for a
/// connection to come up.
#[derive(Debug)]
pub(super) struct Waiting {
    pub(super) frame: Option<Arc<OutFrame>>,
    pub(super) tracker: Arc<Tracker>,
    pub(super) number: u64,
    pub(super) deadline: Option<Instant>,
    /// The connection whose backlog holds it; None while held.
    pub(super) on: Option<ConnId>,
    /// An ALL send's copy, for this connection alone.
    pub(super) copy: bool,
    /// Held: an ALL send, every live connection's when one comes up.
    pub(super) all: bool,
    /// Adopts the connection it goes to if its own dies.
    pub(super) lane: Option<Lane>,
    /// Gone from where it waited: queued, dropped or lost.
    pub(super) over: bool,
}

/// What waits for one connection, in the order it came; `live` counts the
/// entries not over, and while it is not 0 the connection's queue is full.
#[derive(Debug)]
pub(super) struct Backlog {
    pub(super) conn: ConnId,
    pub(super) waiting: VecDeque<WaitKey>,
    pub(super) live: usize,
}

/// A flush_all() in progress, or ended and not forgotten yet.
pub(super) struct Flush {
    pub(super) last_number: u64,
    pub(super) waiting: usize,
    pub(super) marks: Vec<(ConnId, Arc<Tracker>)>,
    pub(super) done: Option<FlushDone>,
    pub(super) flushed: bool,
    pub(super) lost: bool,
}

/// A send followed to its end for its `done`.
pub(super) struct Follow {
    pub(super) done: Option<SendDone>,
    pub(super) copies: Vec<Arc<Tracker>>,
    pub(super) live: usize,
    pub(super) over: bool,
}

/// Where the last dead connection to each target handed what it had not
/// written.
#[derive(Debug)]
pub(super) struct Handover {
    pub(super) target: Target,
    pub(super) from: Option<ConnId>,
    pub(super) to: ConnId,
    pub(super) to_target: Target,
}

/// A flush's handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct FlushId(pub(super) u64);

/// The outbox's state; see the module comment.
#[derive(Default)]
pub(crate) struct Outbox {
    pub(super) slab: Vec<Option<Waiting>>,
    pub(super) generations: Vec<u64>,
    pub(super) free: Vec<usize>,
    pub(super) backlogs: Vec<Backlog>,
    pub(super) waiting: usize,
    pub(super) displaced: VecDeque<WaitKey>,
    pub(super) held: VecDeque<WaitKey>,
    pub(super) held_live: usize,
    pub(super) last_number: u64,
    pub(super) retries: u64,
    pub(super) expiring: BinaryHeap<Reverse<(Instant, WaitKey)>>,
    pub(super) compact_at: usize,
    pub(super) timer: Option<TimerKey>,
    pub(super) armed: Option<Instant>,
    pub(super) flushes: Vec<FlushId>,
    pub(super) flush_states: HashMap<FlushId, Flush>,
    pub(super) next_flush: u64,
    pub(super) moved: Vec<(Weak<Tracker>, Wrapper)>,
    pub(super) moved_compact_at: usize,
    pub(super) handovers: Vec<Handover>,
    pub(super) follows: HashMap<usize, u64>,
    pub(super) follow_states: HashMap<u64, Follow>,
    pub(super) next_follow: u64,
    pub(super) follow_events: Vec<(Arc<Tracker>, bool)>,
    pub(crate) follow_posted: bool,
}

impl Outbox {
    /// Stores `waiting`, returning its key.
    pub(super) fn store(&mut self, waiting: Waiting) -> WaitKey {
        if let Some(index) = self.free.pop() {
            self.slab[index] = Some(waiting);
            return WaitKey { index, generation: self.generations[index] };
        }
        self.slab.push(Some(waiting));
        self.generations.push(0);
        WaitKey { index: self.slab.len() - 1, generation: 0 }
    }

    /// The entry of `key`, if it is still stored.
    pub(super) fn get(&self, key: WaitKey) -> Option<&Waiting> {
        if self.generations.get(key.index) != Some(&key.generation) {
            return None;
        }
        self.slab[key.index].as_ref()
    }

    /// The same, to change.
    pub(super) fn get_mut(&mut self, key: WaitKey) -> Option<&mut Waiting> {
        if self.generations.get(key.index) != Some(&key.generation) {
            return None;
        }
        self.slab[key.index].as_mut()
    }

    /// Frees the entry of `key`, which has left its queue, when it is over.
    pub(super) fn release(&mut self, key: WaitKey) {
        if self.get(key).is_some_and(|waiting| waiting.over) {
            self.slab[key.index] = None;
            self.generations[key.index] += 1;
            self.free.push(key.index);
        }
    }

    /// The backlog of `conn`, if it has one.
    pub(super) fn backlog(&mut self, conn: ConnId) -> Option<&mut Backlog> {
        self.backlogs.iter_mut().find(|backlog| backlog.conn == conn)
    }

    /// How many entries the outbox keeps: those waiting and those that
    /// ended and are not cleared out yet; for tests.
    pub(crate) fn entries(&self) -> usize {
        self.held.len()
            + self.displaced.len()
            + self.backlogs.iter().map(|backlog| backlog.waiting.len()).sum::<usize>()
    }

    /// How many times a message was moved on after waiting; for tests.
    pub(crate) fn retries(&self) -> u64 {
        self.retries
    }

    /// The last number the order sent gave.
    pub(crate) fn last_number(&self) -> u64 {
        self.last_number
    }
}
