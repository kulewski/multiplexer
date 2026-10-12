//! A bounded set of recently seen values: `insert` says whether a value is
//! new, and the oldest is forgotten once the set holds its span. Mirrors
//! lib/spanset.h, with a hash set where it has a tree: the client keeps the
//! ids of the last 2048 messages it received to drop the copies of a
//! message that arrived through several multiplexers.

use std::collections::{HashSet, VecDeque};

/// The last `span` values inserted; see the module comment.
#[derive(Debug)]
pub(crate) struct SpanSet {
    span: usize,
    set: HashSet<u64>,
    order: VecDeque<u64>,
}

impl SpanSet {
    /// An empty set remembering `span` values.
    pub(crate) fn new(span: usize) -> SpanSet {
        SpanSet { span, set: HashSet::with_capacity(span), order: VecDeque::with_capacity(span) }
    }

    /// Adds `value`, true when it was not there; the oldest goes when the
    /// set is full.
    pub(crate) fn insert(&mut self, value: u64) -> bool {
        if !self.set.insert(value) {
            return false;
        }
        if self.order.len() == self.span {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        self.order.push_back(value);
        true
    }

    /// Forgets the value the last `insert` that returned true added: one
    /// its caller did not keep after all, whose next copy is new again.
    /// Only right after that insert; the oldest value it may have pushed
    /// out is not brought back.
    pub(crate) fn forget_last(&mut self) {
        if let Some(last) = self.order.pop_back() {
            self.set.remove(&last);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SpanSet;

    #[test]
    fn says_new_once_and_forgets_the_oldest_past_its_span() {
        let mut set = SpanSet::new(3);
        assert!(set.insert(1) && set.insert(2) && set.insert(3));
        assert!(!set.insert(1) && !set.insert(3));
        assert!(set.insert(4)); // 1 goes
        assert!(set.insert(1)); // new again, 2 goes
        assert!(!set.insert(3) && !set.insert(4) && set.insert(2));
    }

    #[test]
    fn forgets_the_last_value_taken() {
        let mut set = SpanSet::new(2);
        assert!(set.insert(7));
        set.forget_last();
        assert!(set.insert(7));
        assert!(!set.insert(7));
    }
}
