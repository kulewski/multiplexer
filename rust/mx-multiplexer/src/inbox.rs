//! The inbox of a threaded class's io thread, as C++'s ThreadedClientInbox:
//! what other threads hand it, in one queue in the order they were made, a
//! command boxed, a plain send as data (`Posted::Plain`); and, the other
//! way, the bodies of plain sends a batch is done with, for the senders to
//! serialize their next ones into, which a sender takes under the lock it
//! pushes with. The io thread takes the whole queue at once, swapped out
//! under the lock, at each pass of its driver and after each step.
//!
//! A sender wakes the io thread, a system call, only when it sleeps, about
//! to wait for its sockets (`asleep`), and only the first sender after it
//! fell asleep: a running io thread takes what was queued at its next
//! look, so that a stream of sends costs no system call each, as asio's
//! post signals its reactor only when a thread waits in it. The io thread
//! says it is about to sleep before it looks at `pending` a last time, and
//! a sender sets `pending` before it looks at `asleep`, all four SeqCst:
//! one of the two sees the other's, so no send is left waiting for a
//! thread that sleeps on.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

use crate::core::bodies::Bodies;
use crate::core::plain::Plain;
use crate::core::Posted;

/// The inbox; see the module comment. On cache lines of its own: every
/// sender and the io thread touch it.
#[derive(Default)]
#[repr(align(64))]
pub(crate) struct Inbox {
    state: Mutex<InboxState>,
    /// Something is queued: one load when nothing is.
    pending: AtomicBool,
    /// The io thread is about to wait for its sockets, or waits.
    asleep: AtomicBool,
}

/// What the inbox's lock guards.
#[derive(Default)]
struct InboxState {
    posted: VecDeque<Posted>,
    returned: Bodies,
}

impl Inbox {
    /// Locks the inbox, which nothing panics under.
    fn lock(&self) -> MutexGuard<'_, InboxState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Queues `posted` for the io thread: true when the io thread sleeps,
    /// for the caller to wake it, which only the first caller after it fell
    /// asleep hears, since a wake is a system call; the others' commands
    /// ride that wake, the io thread taking them all at once.
    pub(crate) fn push(&self, posted: Posted) -> bool {
        {
            let mut state = self.lock();
            state.posted.push_back(posted);
            self.pending.store(true, Ordering::SeqCst);
        }
        self.wake_needed()
    }

    /// Queues a plain send, with its deadline, as `push` does, and takes,
    /// under the same lock, a body handed back for the caller's next one, or
    /// an empty one; and whether the caller wakes the io thread.
    pub(crate) fn push_plain(&self, plain: Plain, deadline: Option<Instant>) -> (Vec<u8>, bool) {
        let spare = {
            let mut state = self.lock();
            state.posted.push_back(Posted::Plain(plain, deadline));
            self.pending.store(true, Ordering::SeqCst);
            state.returned.take()
        };
        (spare, self.wake_needed())
    }

    /// Whether the io thread sleeps, which the caller then wakes: true for
    /// one caller. Read after `pending` was set, both SeqCst, as the io
    /// thread sets `asleep` before it reads `pending` (`fall_asleep`): one
    /// of the two sees the other's.
    fn wake_needed(&self) -> bool {
        self.asleep.load(Ordering::SeqCst) && self.asleep.swap(false, Ordering::SeqCst)
    }

    /// Swaps what was queued out into `taken`, empty, its order kept, and
    /// hands back `spent`'s bodies for the senders, as many as are kept
    /// (`Bodies`); nothing locked when nothing was queued and no body is
    /// spent.
    pub(crate) fn take(&self, taken: &mut VecDeque<Posted>, spent: &mut Bodies) {
        if !self.pending.load(Ordering::SeqCst) && spent.is_empty() {
            return;
        }
        let mut state = self.lock();
        self.pending.store(false, Ordering::SeqCst);
        std::mem::swap(&mut state.posted, taken);
        state.returned.take_all(spent);
    }

    /// The io thread is about to wait for its sockets: false, and it must
    /// not, when something was queued meanwhile.
    pub(crate) fn fall_asleep(&self) -> bool {
        self.asleep.store(true, Ordering::SeqCst);
        if self.pending.load(Ordering::SeqCst) {
            self.asleep.store(false, Ordering::SeqCst);
            return false;
        }
        true
    }

    /// The io thread's wait is over.
    pub(crate) fn wake_up(&self) {
        self.asleep.store(false, Ordering::SeqCst);
    }

    /// How many bodies wait for the senders; for tests.
    #[cfg(test)]
    pub(crate) fn returned(&self) -> usize {
        self.lock().returned.len()
    }
}

#[cfg(test)]
mod tests {
    //! The wakes a sender makes, the queue taken whole and in order, the
    //! bodies handed back, and a driver's step running the plain sends at
    //! the front of its queue, TURN at most.

    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::Inbox;
    use crate::core::batching::Batching;
    use crate::core::bodies::Bodies;
    use crate::core::client::IncomingMode;
    use crate::core::plain::Plain;
    use crate::core::{Core, Posted};
    use crate::driver::{Driver, Step, TURN};
    use crate::ids::Ids;

    /// A plain send of type 300 and id `id`, its body empty.
    fn plain(id: u64) -> Plain {
        Plain { body: Vec::new(), length: 0, keyed: false, id, r#type: 300 }
    }

    #[test]
    fn a_sender_wakes_the_io_thread_only_when_it_sleeps_and_only_once() {
        let inbox = Inbox::default();
        let (mut taken, mut spent) = (VecDeque::new(), Bodies::default());
        assert!(!inbox.push(Posted::Fill), "awake: it takes what was queued at its next look");
        assert!(!inbox.fall_asleep(), "something queued: it must not sleep");
        inbox.take(&mut taken, &mut spent);
        assert_eq!(1, taken.len());
        taken.clear();
        assert!(inbox.fall_asleep(), "nothing queued: it may");
        assert!(inbox.push(Posted::Fill), "asleep: the first sender wakes it");
        assert!(!inbox.push_plain(plain(1), None).1, "and only the first");
        inbox.wake_up();
        assert!(!inbox.push(Posted::Fill), "awake again");
    }

    #[test]
    fn the_queue_is_taken_whole_in_order_and_the_bodies_go_back_to_the_senders() {
        let inbox = Inbox::default();
        let mut spent = Bodies::default();
        let body = Vec::with_capacity(500);
        let handed_back = body.as_ptr();
        spent.give(body);
        inbox.push(Posted::Fill);
        let (spare, _) = inbox.push_plain(plain(1), None);
        assert_eq!(0, spare.capacity(), "none handed back yet: an empty one");
        inbox.push(Posted::ProcessCallbacks);
        let mut taken = VecDeque::new();
        inbox.take(&mut taken, &mut spent);
        assert_eq!(3, taken.len());
        assert!(matches!(taken[0], Posted::Fill));
        assert!(matches!(&taken[1], Posted::Plain(plain, None) if plain.id == 1));
        assert!(matches!(taken[2], Posted::ProcessCallbacks));
        assert!(spent.is_empty());
        assert_eq!(1, inbox.returned(), "handed back with the take");
        let (spare, _) = inbox.push_plain(plain(2), None);
        assert_eq!(handed_back, spare.as_ptr(), "the body handed back, for the sender's next send");
        assert_eq!(0, inbox.returned());
    }

    #[test]
    fn a_step_runs_the_plain_sends_at_the_front_of_the_queue_turn_at_most() {
        // One step, one clock read, for a run of plain sends; the rest at
        // the next step, after a pass, as after any TURN entries.
        let inbox = Arc::new(Inbox::default());
        let mut driver = Driver::new(Some(Arc::clone(&inbox))).expect("a driver");
        let mut core = Core::new(200, Arc::new(Ids::seeded(7)), IncomingMode::Dispatch, Instant::now());
        let batching = Batching { max_delay: Duration::from_secs(3600), ..Batching::default() };
        core.set_batching(300, None, batching).expect("set");
        let sends = TURN as u64 + 36;
        for id in 1..=sends {
            inbox.push_plain(plain(id), None);
        }
        assert_eq!(Step::Ran, driver.step(&mut core, None, false));
        assert_eq!(TURN, core.batched_messages());
        assert_eq!(Step::Ran, driver.step(&mut core, None, false));
        assert_eq!(sends as usize, core.batched_messages());
    }
}
