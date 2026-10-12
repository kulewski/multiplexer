//! The core's timers: one heap of deadlines whose earliest is what the
//! driver waits for, where C++ arms an asio::steady_timer each. A timer is
//! armed with what it is for, a `Due`, and cancelled by its key; arming a
//! timer of an owner again cancels the wait set before, as asio's
//! `expires_after` does. Cancelled entries stay in the heap until they come
//! to its top and are skipped, so arming and cancelling cost a heap push
//! and a map entry; once they outnumber the armed timers, the heap is
//! rebuilt from the armed ones alone, so that it holds no more than twice
//! what is armed, however fast timers are cancelled: a query answered in a
//! millisecond would otherwise keep its entry until its timeout's
//! deadline, and a client at a hundred thousand queries a second would
//! keep a million. Each rebuild is paid for by the cancellations before it.
//!
//! What counts as work for a synchronous wait (the driver's `run_one`) is
//! what C++ counts: a timer armed is work until it fires or is cancelled.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::Instant;

use super::Due;

/// The cancelled entries the heap may hold beyond as many as are armed
/// before it is rebuilt: what a client with few timers never reaches.
const CANCELLED_SLACK: usize = 64;

/// A timer's key, for cancelling it; never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct TimerKey(u64);

/// Every timer armed, and the heap of their deadlines.
#[derive(Debug, Default)]
pub(crate) struct Timers {
    heap: BinaryHeap<Reverse<(Instant, TimerKey)>>,
    armed: HashMap<TimerKey, Due>,
    next_key: u64,
}

impl Timers {
    /// Arms a timer for `due` at `deadline`; its key cancels it.
    pub(crate) fn arm(&mut self, deadline: Instant, due: Due) -> TimerKey {
        self.next_key += 1;
        let key = TimerKey(self.next_key);
        self.heap.push(Reverse((deadline, key)));
        self.armed.insert(key, due);
        key
    }

    /// Arms the timer kept in `slot` again for `due` at `deadline`,
    /// cancelling the wait it had.
    pub(crate) fn rearm(&mut self, slot: &mut Option<TimerKey>, deadline: Instant, due: Due) {
        self.cancel(slot);
        *slot = Some(self.arm(deadline, due));
    }

    /// Cancels the timer kept in `slot`, if any, and empties the slot.
    pub(crate) fn cancel(&mut self, slot: &mut Option<TimerKey>) {
        if let Some(key) = slot.take() {
            self.armed.remove(&key);
            if self.heap.len() > 2 * self.armed.len() + CANCELLED_SLACK {
                let armed = &self.armed;
                self.heap.retain(|Reverse((_, key))| armed.contains_key(key));
            }
        }
    }

    /// Whether any timer is armed: work, for a synchronous wait.
    pub(crate) fn any_armed(&self) -> bool {
        !self.armed.is_empty()
    }

    /// How many timers are armed; for tests.
    #[cfg(test)]
    pub(crate) fn armed(&self) -> usize {
        self.armed.len()
    }

    /// How many entries the heap holds, the cancelled ones too; for tests.
    #[cfg(test)]
    pub(crate) fn entries(&self) -> usize {
        self.heap.len()
    }

    /// The earliest deadline of a timer still armed, the cancelled ones on
    /// top of the heap dropped on the way.
    pub(crate) fn next_deadline(&mut self) -> Option<Instant> {
        while let Some(Reverse((deadline, key))) = self.heap.peek() {
            if self.armed.contains_key(key) {
                return Some(*deadline);
            }
            self.heap.pop();
        }
        None
    }

    /// The timer due at `now` with the earliest deadline, taken out: its
    /// key and what it is for. None when none is due.
    pub(crate) fn take_due(&mut self, now: Instant) -> Option<(TimerKey, Due)> {
        while let Some(Reverse((deadline, key))) = self.heap.peek().copied() {
            if !self.armed.contains_key(&key) {
                self.heap.pop();
                continue;
            }
            if deadline > now {
                return None;
            }
            self.heap.pop();
            return self.armed.remove(&key).map(|due| (key, due));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::super::Due;
    use super::Timers;
    use std::time::{Duration, Instant};

    #[test]
    fn fires_in_deadline_order_and_skips_what_was_cancelled() {
        let start = Instant::now();
        let mut timers = Timers::default();
        let mut second = Some(timers.arm(start + Duration::from_secs(2), Due::OutboxExpiry));
        timers.arm(start + Duration::from_secs(1), Due::LogSummary);
        timers.arm(start + Duration::from_secs(3), Due::ConnectDeadline(super::super::ConnId(7)));
        assert_eq!(Some(start + Duration::from_secs(1)), timers.next_deadline());
        assert!(timers.take_due(start).is_none());
        timers.cancel(&mut second);
        assert!(second.is_none());
        assert!(matches!(timers.take_due(start + Duration::from_secs(5)), Some((_, Due::LogSummary))));
        assert!(matches!(
            timers.take_due(start + Duration::from_secs(5)),
            Some((_, Due::ConnectDeadline(super::super::ConnId(7))))
        ));
        assert!(timers.take_due(start + Duration::from_secs(5)).is_none());
        assert!(!timers.any_armed());
        assert_eq!(None, timers.next_deadline());
    }

    #[test]
    fn arming_again_cancels_the_wait_before() {
        let start = Instant::now();
        let mut timers = Timers::default();
        let mut slot = None;
        timers.rearm(&mut slot, start + Duration::from_secs(1), Due::LogSummary);
        timers.rearm(&mut slot, start + Duration::from_secs(4), Due::LogSummary);
        assert_eq!(1, timers.armed());
        assert!(timers.take_due(start + Duration::from_secs(2)).is_none());
        assert!(timers.take_due(start + Duration::from_secs(4)).is_some());
    }

    #[test]
    fn timers_cancelled_long_before_their_deadline_do_not_pile_up() {
        let start = Instant::now();
        let mut timers = Timers::default();
        let mut kept = Some(timers.arm(start + Duration::from_secs(5), Due::LogSummary));
        for query in 0..10_000u64 {
            let mut slot = Some(timers.arm(start + Duration::from_secs(10), Due::OutboxExpiry));
            timers.cancel(&mut slot);
            assert!(
                timers.entries() <= 2 + super::CANCELLED_SLACK,
                "{} entries after {query} cancelled",
                timers.entries()
            );
        }
        assert_eq!(1, timers.armed());
        assert_eq!(Some(start + Duration::from_secs(5)), timers.next_deadline());
        timers.cancel(&mut kept);
        assert_eq!(None, timers.next_deadline());
    }
}
