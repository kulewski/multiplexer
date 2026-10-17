//! Fork safety: how many times this process is a forked child of the one
//! that made its first client, and the client's socket descriptors, kept for
//! such a child to close. Mirrors lib/fork.cc and
//! multiplexer/descriptor_table.cc: a client records the generation it was
//! made in, and a call on it in a child, where its io thread does not exist
//! and its locks may be held by nobody, fails with `UsedAfterFork` before it
//! takes any lock; dropping it there closes the child's copies of its
//! sockets with close(2) alone, which leaves the parent's connections
//! open, and leaks the rest.
//!
//! The generation is bumped by a `pthread_atfork` child handler, registered
//! when the first client is made; a fork before that has no client to
//! protect. Registered with no lock any caller could wait on: a child
//! forked while another thread registered copies whatever the registering
//! thread had done, and a lock held there would be held for good. Two
//! first callers at once both register, and a fork then counts twice,
//! which a comparison of generations does not mind; a child forked before
//! the registration finished registers again itself, with no client of
//! the parent's to protect. The table is a fixed array of atomic slots, which a child reads
//! in its copy of memory, frozen at whatever instruction the owner was on:
//! every change is one atomic store, and neither side locks or allocates.
//! C++ grows its table; this one holds the 256 sockets no client comes near
//! (its targets and the connections closing) and says so once if it is
//! ever full, the child then leaving that socket open until it exits.
//!
//! The crate's `unsafe` code is here, the two libc calls, and in xxh3.rs:
//! its calls to the functions built for the processor's vector
//! instructions once the processor said it has them, and their loads and
//! stores of the arrays they are given.

#![allow(unsafe_code)]

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};

/// The count `generation` reads.
static GENERATION: AtomicU32 = AtomicU32::new(0);

/// Whether the child handler is registered; see the module comment.
static REGISTERED: AtomicBool = AtomicBool::new(false);

/// The child handler: nothing but the count, since a forked child has one
/// thread and every lock another thread held stays locked.
extern "C" fn in_child() {
    GENERATION.fetch_add(1, Ordering::Relaxed);
}

/// The fork generation now; registers the handler on the first call, which
/// every client makes as it is created.
pub(crate) fn generation() -> u32 {
    if !REGISTERED.load(Ordering::Acquire) {
        register();
    }
    GENERATION.load(Ordering::Relaxed)
}

/// Registers the child handler; see the module comment.
#[cold]
fn register() {
    // SAFETY: pthread_atfork only records the three handlers; the child
    // handler touches nothing but an atomic, which is async-signal-safe.
    unsafe {
        libc::pthread_atfork(None, None, Some(in_child));
    }
    REGISTERED.store(true, Ordering::Release);
}

/// A slot that holds no descriptor.
const FREE: i32 = -1;

/// How many sockets a table records at once.
pub(crate) const DESCRIPTOR_SLOTS: usize = 256;

/// A client's open sockets, for a forked child to close; see the module
/// comment. The owner adds and removes, a child reads.
#[derive(Debug)]
pub(crate) struct DescriptorTable {
    slots: Box<[AtomicI32]>,
    full_said: AtomicBool,
    /// Set by the first close_all(), in a child, which every later one
    /// finds: the numbers it closed may be the child's own files by then.
    closed: AtomicBool,
}

impl Default for DescriptorTable {
    fn default() -> Self {
        DescriptorTable {
            slots: (0..DESCRIPTOR_SLOTS).map(|_| AtomicI32::new(FREE)).collect(),
            full_said: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
    }
}

impl DescriptorTable {
    /// Records `fd`, a socket just opened, in the first free slot.
    pub(crate) fn add(&self, fd: i32) {
        for slot in self.slots.iter() {
            if slot.load(Ordering::Relaxed) == FREE {
                slot.store(fd, Ordering::Release);
                return;
            }
        }
        if !self.full_said.swap(true, Ordering::Relaxed) {
            log::warn!(
                "{DESCRIPTOR_SLOTS} sockets open at once: a process forked from this one keeps its copy of the \
                 newest open until it exits"
            );
        }
    }

    /// Forgets `fd` before it is closed, so that a child forked after this
    /// leaves the number alone, which the process may give to a file next.
    pub(crate) fn remove(&self, fd: i32) {
        for slot in self.slots.iter() {
            if slot.load(Ordering::Relaxed) == fd {
                slot.store(FREE, Ordering::Release);
            }
        }
    }

    /// In a forked child: closes its copy of every descriptor recorded,
    /// with close(2) only, once: every orphan teardown calls it, a
    /// shutdown() and then the drop, the last of several handles, and
    /// between two calls the child may have opened descriptors of its own
    /// under the same numbers, a fresh client's.
    pub(crate) fn close_all(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        for slot in self.slots.iter() {
            let fd = slot.load(Ordering::Acquire);
            if fd != FREE {
                // SAFETY: closing a descriptor number the table recorded as a
                // socket of this client; in the child nothing else uses it.
                unsafe {
                    libc::close(fd);
                }
            }
        }
    }

    /// How many descriptors are recorded; for tests.
    #[cfg(test)]
    pub(crate) fn recorded(&self) -> usize {
        self.slots.iter().filter(|slot| slot.load(Ordering::Relaxed) != FREE).count()
    }
}

#[cfg(test)]
mod tests {
    //! The table's bookkeeping; what a forked child does with it is tested
    //! with real forks in tests/fork.rs.

    use super::{generation, DescriptorTable, DESCRIPTOR_SLOTS};

    #[test]
    fn records_reuses_and_forgets_slots() {
        let table = DescriptorTable::default();
        table.add(10);
        table.add(11);
        assert_eq!(2, table.recorded());
        table.remove(10);
        assert_eq!(1, table.recorded());
        table.add(12); // into the slot 10 left
        assert_eq!(2, table.recorded());
        table.remove(11);
        table.remove(12);
        assert_eq!(0, table.recorded());
    }

    #[test]
    fn a_full_table_records_no_more() {
        let table = DescriptorTable::default();
        for fd in 0..DESCRIPTOR_SLOTS as i32 + 3 {
            table.add(1000 + fd);
        }
        assert_eq!(DESCRIPTOR_SLOTS, table.recorded());
    }

    #[test]
    fn the_generation_is_steady_without_a_fork() {
        assert_eq!(generation(), generation());
    }

    #[test]
    fn a_table_closes_its_descriptors_once() {
        // Two descriptors of this process's own, recorded as a client's:
        // the first close_all() closes them, and a second leaves alone the
        // numbers, which the process may have given to new files since.
        let table = DescriptorTable::default();
        let first = std::fs::File::open("/dev/null").unwrap();
        let second = std::fs::File::open("/dev/null").unwrap();
        use std::os::fd::IntoRawFd;
        let (first, second) = (first.into_raw_fd(), second.into_raw_fd());
        table.add(first);
        table.add(second);
        table.close_all();
        let again = std::fs::File::open("/dev/null").unwrap(); // takes one of the numbers
        use std::os::fd::AsRawFd;
        assert!([first, second].contains(&again.as_raw_fd()));
        table.close_all();
        assert!(again.metadata().is_ok(), "the new file is still open");
    }
}
